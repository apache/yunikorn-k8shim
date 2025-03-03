/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package external

import (
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storageV1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// SchedulerCache maintains some critical information about nodes and pods used for scheduling.
// Nodes are cached in the form of de-scheduler nodeInfo. Instead of re-creating all nodes info from scratch,
// we replicate nodes info from de-scheduler, in order to re-use predicates functions.
//
// When running YuniKorn as a scheduler plugin, we also track pod allocations that YuniKorn has decided upon, but which
// have not yet been fulfilled by the default scheduler. This tracking is needed to ensure that we pass along
// allocations to the default scheduler once (and only) once. Allocations can be in one of two states, either pending or
// in-progress. A pending allocation is one which has been decided upon by YuniKorn but has not yet been communicated
// to the default scheduler via PreFilter() / Filter(). Once PreFilter() / Filter() pass, the allocation transitions
// to in-progress to signify that the default scheduler is responsible for fulfilling the allocation. Once PostBind()
// is called in the plugin to signify completion of the allocation, it is removed.
type SchedulerCache struct {
	nodesMap              map[string]*framework.NodeInfo // node name to NodeInfo map
	podsMap               map[string]*v1.Pod
	pcMap                 map[string]*schedulingv1.PriorityClass
	assignedPods          map[string]string      // map of pods to the node they are currently assigned to
	assumedPods           map[string]bool        // map of assumed pods, value indicates if pod volumes are all bound
	orphanedPods          map[string]*v1.Pod     // map of orphaned pods, keyed by pod UID
	pendingAllocations    map[string]string      // map of pod to node ID, presence indicates a pending allocation for scheduler
	inProgressAllocations map[string]string      // map of pod to node ID, presence indicates an in-process allocation for scheduler
	schedulingTasks       map[string]interface{} // list of task IDs which are currently being processed by the scheduler
	pvcRefCounts          map[string]map[string]int
	lock                  locking.RWMutex
	clients               *client.Clients // client APIs
	klogger               klog.Logger

	// cached data, re-calculated on demand from nodesMap
	nodesInfo                        []*framework.NodeInfo
	nodesInfoPodsWithAffinity        []*framework.NodeInfo
	nodesInfoPodsWithReqAntiAffinity []*framework.NodeInfo

	// task bloom filter, recomputed whenever task scheduling state changes
	taskBloomFilterRef atomic.Pointer[taskBloomFilter]
}

type taskBloomFilter struct {
	data [4][256]bool
}

func NewSchedulerCache(clients *client.Clients) *SchedulerCache {
	cache := &SchedulerCache{
		nodesMap:              make(map[string]*framework.NodeInfo),
		podsMap:               make(map[string]*v1.Pod),
		pcMap:                 make(map[string]*schedulingv1.PriorityClass),
		assignedPods:          make(map[string]string),
		assumedPods:           make(map[string]bool),
		orphanedPods:          make(map[string]*v1.Pod),
		pendingAllocations:    make(map[string]string),
		inProgressAllocations: make(map[string]string),
		schedulingTasks:       make(map[string]interface{}),
		pvcRefCounts:          make(map[string]map[string]int),
		clients:               clients,
		klogger:               klog.NewKlogr(),
	}
	cache.taskBloomFilterRef.Store(&taskBloomFilter{})
	return cache
}

// GetNodesInfoMap returns a reference to the internal node map. This is explicitly for the use of the predicate
// shared lister and requires that the scheduler cache lock be held while accessing.
func (cache *SchedulerCache) GetNodesInfoMap() map[string]*framework.NodeInfo {
	return cache.nodesMap
}

// GetNodesInfo returns a (possibly cached) list of nodes. This is explicitly for the use of the predicate
// shared lister and requires that the scheduler cache lock be held while accessing.
func (cache *SchedulerCache) GetNodesInfo() []*framework.NodeInfo {
	if cache.nodesInfo == nil {
		nodeList := make([]*framework.NodeInfo, 0, len(cache.nodesMap))
		for _, node := range cache.nodesMap {
			nodeList = append(nodeList, node)
		}
		cache.nodesInfo = nodeList
	}

	return cache.nodesInfo
}

// GetNodesInfoPodsWithAffinity returns a (possibly cached) list of nodes which contain pods with affinity.
// This is explicitly for the use of the predicate shared lister and requires that the scheduler cache lock
// be held while accessing.
func (cache *SchedulerCache) GetNodesInfoPodsWithAffinity() []*framework.NodeInfo {
	if cache.nodesInfoPodsWithAffinity == nil {
		nodeList := make([]*framework.NodeInfo, 0, len(cache.nodesMap))
		for _, node := range cache.nodesMap {
			if len(node.PodsWithAffinity) > 0 {
				nodeList = append(nodeList, node)
			}
		}
		cache.nodesInfoPodsWithAffinity = nodeList
	}

	return cache.nodesInfoPodsWithAffinity
}

// GetNodesInfoPodsWithReqAntiAffinity returns a (possibly cached) list of nodes which contain pods with required anti-affinity.
// This is explicitly for the use of the predicate shared lister and requires that the scheduler cache lock
// be held while accessing.
func (cache *SchedulerCache) GetNodesInfoPodsWithReqAntiAffinity() []*framework.NodeInfo {
	if cache.nodesInfoPodsWithReqAntiAffinity == nil {
		nodeList := make([]*framework.NodeInfo, 0, len(cache.nodesMap))
		for _, node := range cache.nodesMap {
			if len(node.PodsWithRequiredAntiAffinity) > 0 {
				nodeList = append(nodeList, node)
			}
		}
		cache.nodesInfoPodsWithReqAntiAffinity = nodeList
	}

	return cache.nodesInfoPodsWithReqAntiAffinity
}

func (cache *SchedulerCache) LockForReads() {
	cache.lock.RLock()
}

func (cache *SchedulerCache) UnlockForReads() {
	cache.lock.RUnlock()
}

func (cache *SchedulerCache) GetNode(name string) *framework.NodeInfo {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if n, ok := cache.nodesMap[name]; ok {
		return n
	}
	return nil
}

// UpdateNode updates the given node in the cache and returns the previous node if it exists
func (cache *SchedulerCache) UpdateNode(node *v1.Node) (*v1.Node, []*v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("UpdateNode.Pre")
	defer cache.dumpState("UpdateNode.Post")
	return cache.updateNode(node)
}

func (cache *SchedulerCache) updateNode(node *v1.Node) (*v1.Node, []*v1.Pod) {
	var prevNode *v1.Node
	adopted := make([]*v1.Pod, 0)

	nodeInfo, ok := cache.nodesMap[node.Name]
	if !ok {
		log.Log(log.ShimCacheExternal).Debug("Adding node to cache", zap.String("nodeName", node.Name))
		nodeInfo = framework.NewNodeInfo()
		cache.nodesMap[node.Name] = nodeInfo
		cache.nodesInfo = nil
		nodeInfo.SetNode(node)

		// look for orphaned pods to adopt
		for _, pod := range cache.orphanedPods {
			if pod.Spec.NodeName == node.Name {
				if cache.updatePod(pod) {
					adopted = append(adopted, pod)
				}
			}
		}
	} else {
		log.Log(log.ShimCacheExternal).Debug("Updating node in cache", zap.String("nodeName", node.Name))
		prevNode = nodeInfo.Node()
		nodeInfo.SetNode(node)
	}

	cache.nodesInfoPodsWithAffinity = nil
	cache.nodesInfoPodsWithReqAntiAffinity = nil
	cache.updatePVCRefCounts(nodeInfo, false)

	return prevNode, adopted
}

func (cache *SchedulerCache) RemoveNode(node *v1.Node) (*v1.Node, []*v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("RemoveNode.Pre")
	defer cache.dumpState("RemoveNode.Post")

	return cache.removeNode(node)
}

func (cache *SchedulerCache) removeNode(node *v1.Node) (*v1.Node, []*v1.Pod) {
	orphans := make([]*v1.Pod, 0)
	nodeInfo, ok := cache.nodesMap[node.Name]
	if !ok {
		log.Log(log.ShimCacheExternal).Debug("Attempted to remove non-existent node", zap.String("nodeName", node.Name))
		return nil, nil
	}
	result := nodeInfo.Node()

	for _, pod := range nodeInfo.Pods {
		key := string(pod.Pod.UID)
		delete(cache.assignedPods, key)
		delete(cache.assumedPods, key)
		delete(cache.pendingAllocations, key)
		delete(cache.inProgressAllocations, key)
		cache.orphanedPods[key] = pod.Pod
		orphans = append(orphans, pod.Pod)
	}

	log.Log(log.ShimCacheExternal).Debug("Removing node from cache", zap.String("nodeName", node.Name))
	delete(cache.nodesMap, node.Name)
	cache.nodesInfo = nil
	cache.nodesInfoPodsWithAffinity = nil
	cache.nodesInfoPodsWithReqAntiAffinity = nil
	cache.updatePVCRefCounts(nodeInfo, true)

	return result, orphans
}

func (cache *SchedulerCache) GetPriorityClass(name string) *schedulingv1.PriorityClass {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	if n, ok := cache.pcMap[name]; ok {
		return n
	}
	return nil
}

func (cache *SchedulerCache) UpdatePriorityClass(priorityClass *schedulingv1.PriorityClass) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("UpdatePriorityClass.Pre")
	defer cache.dumpState("UpdatePriorityClass.Post")

	cache.updatePriorityClass(priorityClass)
}

func (cache *SchedulerCache) updatePriorityClass(priorityClass *schedulingv1.PriorityClass) {
	_, ok := cache.pcMap[priorityClass.Name]
	if !ok {
		log.Log(log.ShimCacheExternal).Debug("Adding priorityClass to cache", zap.String("name", priorityClass.Name))
	} else {
		log.Log(log.ShimCacheExternal).Debug("Updating priorityClass in cache", zap.String("name", priorityClass.Name))
	}
	cache.pcMap[priorityClass.Name] = priorityClass
}

func (cache *SchedulerCache) RemovePriorityClass(priorityClass *schedulingv1.PriorityClass) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("RemovePriorityClass.Pre")
	defer cache.dumpState("RemovePriorityClass.Post")

	cache.removePriorityClass(priorityClass)
}

func (cache *SchedulerCache) removePriorityClass(priorityClass *schedulingv1.PriorityClass) {
	log.Log(log.ShimCacheExternal).Debug("Removing priorityClass from cache", zap.String("name", priorityClass.Name))
	delete(cache.pcMap, priorityClass.Name)
}

// NotifyTaskSchedulerAction registers the fact that a task has been evaluated for scheduling, and consequently the
// scheduler plugin should move it to the activeQ if requested to do so.
func (cache *SchedulerCache) NotifyTaskSchedulerAction(taskID string) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	// verify that the pod exists in the cache, otherwise ignore
	if pod := cache.GetPodNoLock(taskID); pod == nil {
		return
	}
	cache.addSchedulingTask(taskID)
}

// IsTaskMaybeSchedulable returns true if a task might be currently able to be scheduled. This uses a bloom filter
// cached from a set of taskIDs to perform efficient negative lookups.
func (cache *SchedulerCache) IsTaskMaybeSchedulable(taskID string) bool {
	return cache.taskBloomFilterRef.Load().isTaskMaybePresent(taskID)
}

// AddPendingPodAllocation is used to add a new pod -> node mapping to the cache when running in scheduler plugin mode.
// This function is called (in plugin mode) after a task is allocated by the YuniKorn scheduler.
func (cache *SchedulerCache) AddPendingPodAllocation(podKey string, nodeID string) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("AddPendingPodAllocation.Pre")
	defer cache.dumpState("AddPendingPodAllocation.Post")
	delete(cache.inProgressAllocations, podKey)
	cache.pendingAllocations[podKey] = nodeID
}

// RemovePodAllocation is used to remove a pod -> node mapping from the cache when running in scheduler plugin
// mode. It removes both pending and in-progress allocations. This function is called (via cache) from the scheduler
// plugin in PreFilter() if a previous allocation was found, and in PostBind() to cleanup the allocation since it is no
// longer relevant.
func (cache *SchedulerCache) RemovePodAllocation(podKey string) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("RemovePendingPodAllocation.Pre")
	defer cache.dumpState("RemovePendingPodAllocation.Post")
	delete(cache.pendingAllocations, podKey)
	delete(cache.inProgressAllocations, podKey)
}

// GetPendingPodAllocation is used in scheduler plugin mode to retrieve a pending pod allocation. A pending
// allocation is one which has been decided upon by YuniKorn but has not yet been communicated to the default scheduler.
func (cache *SchedulerCache) GetPendingPodAllocation(podKey string) (nodeID string, ok bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	res, ok := cache.pendingAllocations[podKey]
	return res, ok
}

// GetInProgressPodAllocation is used in scheduler plugin mode to retrieve an in-progress pod allocation. An in-progress
// allocation is one which has been communicated to the default scheduler, but has not yet been bound.
func (cache *SchedulerCache) GetInProgressPodAllocation(podKey string) (nodeID string, ok bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	res, ok := cache.inProgressAllocations[podKey]
	return res, ok
}

// StartPodAllocation is used in scheduler plugin mode to transition a pod allocation from pending to in-progress. If
// the given pod has a pending allocation on the given node, the allocation is marked as in-progress and this function
// returns true. If the pod is not pending or is pending on another node, this function does nothing and returns false.
func (cache *SchedulerCache) StartPodAllocation(podKey string, nodeID string) bool {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("StartPendingPodAllocation.Pre")
	defer cache.dumpState("StartPendingPodAllocation.Post")
	expectedNodeID, ok := cache.pendingAllocations[podKey]
	if ok && expectedNodeID == nodeID {
		delete(cache.pendingAllocations, podKey)
		cache.inProgressAllocations[podKey] = nodeID
		return true
	}
	return false
}

// IsAssumedPod returns if pod is assumed in cache, avoid nil
func (cache *SchedulerCache) IsAssumedPod(podKey string) bool {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.isAssumedPod(podKey)
}

func (cache *SchedulerCache) isAssumedPod(podKey string) bool {
	_, ok := cache.assumedPods[podKey]
	return ok
}

func (cache *SchedulerCache) ArePodVolumesAllBound(podKey string) bool {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.assumedPods[podKey]
}

// UpdatePod updates a pod in the cache
func (cache *SchedulerCache) UpdatePod(newPod *v1.Pod) bool {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("UpdatePod.Pre")
	defer cache.dumpState("UpdatePod.Post")
	return cache.updatePod(newPod)
}

func (cache *SchedulerCache) updatePod(pod *v1.Pod) bool {
	key := string(pod.UID)
	result := true

	currState, ok := cache.podsMap[key]
	if ok {
		// remove current version of pod
		delete(cache.podsMap, key)
		delete(cache.orphanedPods, key)
		nodeName, ok := cache.assignedPods[key]
		if ok {
			nodeInfo, ok := cache.nodesMap[nodeName]
			if ok {
				if err := nodeInfo.RemovePod(cache.klogger, currState); err != nil {
					log.Log(log.ShimCacheExternal).Warn("BUG: Failed to remove pod from node",
						zap.String("podName", currState.Name),
						zap.String("nodeName", nodeName),
						zap.Error(err))
				}
				cache.updatePVCRefCounts(nodeInfo, false)
				if podWithAffinity(pod) {
					cache.nodesInfoPodsWithAffinity = nil
				}
				if podWithRequiredAntiAffinity(pod) {
					cache.nodesInfoPodsWithReqAntiAffinity = nil
				}
			}
			if pod.Spec.NodeName == "" {
				// new pod wasn't assigned to a node, so use existing assignment
				pod.Spec.NodeName = nodeName
			}
		}
		delete(cache.assignedPods, key)
	}

	if utils.IsPodRunning(pod) || utils.IsPodTerminated(pod) {
		// delete all assumed state from cache, as pod has now been bound
		delete(cache.assumedPods, key)
		delete(cache.pendingAllocations, key)
		delete(cache.inProgressAllocations, key)
		cache.removeSchedulingTask(key)
	}

	if utils.IsAssignedPod(pod) && !utils.IsPodTerminated(pod) {
		// assign to node
		nodeInfo, ok := cache.nodesMap[pod.Spec.NodeName]
		if !ok {
			// node doesn't exist, so this is an orphaned pod
			log.Log(log.ShimCacheExternal).Info("Marking pod as orphan (required node not present)",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("nodeName", pod.Spec.NodeName))
			cache.orphanedPods[key] = pod
			result = false
		} else {
			nodeInfo.AddPod(pod)
			cache.assignedPods[key] = pod.Spec.NodeName
			if podWithAffinity(pod) {
				cache.nodesInfoPodsWithAffinity = nil
			}
			if podWithRequiredAntiAffinity(pod) {
				cache.nodesInfoPodsWithReqAntiAffinity = nil
			}
			cache.updatePVCRefCounts(nodeInfo, false)
		}
	}

	// if pod is not in a terminal state, add it back into cache
	if !utils.IsPodTerminated(pod) {
		log.Log(log.ShimCacheExternal).Debug("Putting pod in cache", zap.String("podName", pod.Name), zap.String("podKey", key))
		cache.podsMap[key] = pod
	} else {
		log.Log(log.ShimCacheExternal).Debug("Removing terminated pod from cache", zap.String("podName", pod.Name), zap.String("podKey", key))
		delete(cache.podsMap, key)
		delete(cache.assignedPods, key)
		delete(cache.assumedPods, key)
		delete(cache.orphanedPods, key)
		delete(cache.pendingAllocations, key)
		delete(cache.inProgressAllocations, key)
		cache.removeSchedulingTask(key)
	}

	return result
}

// RemovePod removes a pod from the cache
func (cache *SchedulerCache) RemovePod(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("RemovePod.Pre")
	defer cache.dumpState("RemovePod.Post")
	cache.removePod(pod)
}

func (cache *SchedulerCache) removePod(pod *v1.Pod) {
	key := string(pod.UID)
	log.Log(log.ShimCacheExternal).Debug("Removing deleted pod from cache", zap.String("podName", pod.Name), zap.String("podKey", key))
	nodeName, ok := cache.assignedPods[key]
	if ok {
		nodeInfo, ok := cache.nodesMap[nodeName]
		if ok {
			if err := nodeInfo.RemovePod(cache.klogger, pod); err != nil {
				log.Log(log.ShimCacheExternal).Warn("BUG: Failed to remove pod from node",
					zap.String("podName", pod.Name),
					zap.String("nodeName", nodeName),
					zap.Error(err))
			}
		}
		cache.updatePVCRefCounts(nodeInfo, false)
	}
	delete(cache.podsMap, key)
	delete(cache.assignedPods, key)
	delete(cache.assumedPods, key)
	delete(cache.orphanedPods, key)
	delete(cache.pendingAllocations, key)
	delete(cache.inProgressAllocations, key)
	cache.removeSchedulingTask(key)
	cache.nodesInfoPodsWithAffinity = nil
	cache.nodesInfoPodsWithReqAntiAffinity = nil
}

func (cache *SchedulerCache) removeSchedulingTask(taskID string) {
	delete(cache.schedulingTasks, taskID)
	filter := &taskBloomFilter{}
	for taskID := range cache.schedulingTasks {
		filter.addTask(taskID)
	}
	cache.taskBloomFilterRef.Store(filter)
}

func (cache *SchedulerCache) addSchedulingTask(taskID string) {
	cache.schedulingTasks[taskID] = nil
	filter := &taskBloomFilter{
		data: cache.taskBloomFilterRef.Load().data,
	}
	filter.addTask(taskID)
	cache.taskBloomFilterRef.Store(filter)
}

func (filter *taskBloomFilter) addTask(taskID string) {
	limit := min(4, len(taskID))
	for i := 0; i < limit; i++ {
		filter.data[i][taskID[i]] = true
	}
}

func (filter *taskBloomFilter) isTaskMaybePresent(taskID string) bool {
	limit := len(taskID)
	if limit > 4 {
		limit = 4
	}
	for i := 0; i < limit; i++ {
		if !filter.data[i][taskID[i]] {
			return false
		}
	}
	return true
}

func (cache *SchedulerCache) GetPod(uid string) *v1.Pod {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.GetPodNoLock(uid)
}

func (cache *SchedulerCache) IsPodOrphaned(uid string) bool {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	_, ok := cache.orphanedPods[uid]
	return ok
}

func (cache *SchedulerCache) GetPodNoLock(uid string) *v1.Pod {
	if pod, ok := cache.podsMap[uid]; ok {
		return pod
	}
	return nil
}

func (cache *SchedulerCache) AssumePod(pod *v1.Pod, allBound bool) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("AssumePod.Pre")
	defer cache.dumpState("AssumePod.Post")
	cache.assumePod(pod, allBound)
}

func (cache *SchedulerCache) assumePod(pod *v1.Pod, allBound bool) {
	key := string(pod.UID)

	log.Log(log.ShimCacheExternal).Debug("Adding assumed pod to cache",
		zap.String("podName", pod.Name),
		zap.String("podKey", key),
		zap.String("node", pod.Spec.NodeName),
		zap.Bool("allBound", allBound))
	cache.updatePod(pod)
	cache.assumedPods[key] = allBound
}

func (cache *SchedulerCache) ForgetPod(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("ForgetPod.Pre")
	defer cache.dumpState("ForgetPod.Post")

	cache.forgetPod(pod)
}

func (cache *SchedulerCache) forgetPod(pod *v1.Pod) {
	key := string(pod.UID)

	// update the pod in cache
	cache.updatePod(pod)

	// remove assigned allocation
	log.Log(log.ShimCacheExternal).Debug("Removing assumed pod from cache",
		zap.String("podName", pod.Name),
		zap.String("podKey", key))

	delete(cache.assumedPods, key)
	delete(cache.pendingAllocations, key)
	delete(cache.inProgressAllocations, key)
	cache.removeSchedulingTask(key)
}

// Implement k8s.io/client-go/listers/core/v1#PodLister interface
func (cache *SchedulerCache) List(selector labels.Selector) ([]*v1.Pod, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	maxSize := 0
	for _, nodeInfo := range cache.nodesMap {
		maxSize += len(nodeInfo.Pods)
	}
	pods := make([]*v1.Pod, 0, maxSize)
	for _, nodeInfo := range cache.nodesMap {
		for _, pod := range nodeInfo.Pods {
			if selector.Matches(labels.Set(pod.Pod.Labels)) {
				pods = append(pods, pod.Pod)
			}
		}
	}
	return pods, nil
}

// Implement scheduler/algorithm/predicates/predicates.go#NodeInfo interface
func (cache *SchedulerCache) GetNodeInfo(nodeName string) (*v1.Node, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	if nodeInfo, ok := cache.nodesMap[nodeName]; ok {
		return nodeInfo.Node(), nil
	}
	return nil, fmt.Errorf("node %s is not found", nodeName)
}

// Implement scheduler/algorithm/predicates/predicates.go#StorageClassInfo interface
func (cache *SchedulerCache) GetStorageClassInfo(className string) (*storageV1.StorageClass, error) {
	return cache.clients.StorageClassInformer.Lister().Get(className)
}

// Implement scheduler/algorithm/predicates/predicates.go#PersistentVolumeClaimInfo interface
func (cache *SchedulerCache) GetPersistentVolumeClaimInfo(nameSpace, name string) (*v1.PersistentVolumeClaim, error) {
	return cache.clients.PVCInformer.Lister().PersistentVolumeClaims(nameSpace).Get(name)
}

// Implement scheduler/algorithm/predicates/predicates.go#PersistentVolumeClaimInfo interface
func (cache *SchedulerCache) GetPersistentVolumeInfo(name string) (*v1.PersistentVolume, error) {
	return cache.clients.PVInformer.Lister().Get(name)
}

// dumpState dumps summary statistics for the cache. Must be called with lock already acquired
func (cache *SchedulerCache) dumpState(context string) {
	if log.Log(log.ShimCacheExternal).Core().Enabled(zapcore.DebugLevel) {
		log.Log(log.ShimCacheExternal).Debug("Scheduler cache state ("+context+")",
			zap.Int("nodes", len(cache.nodesMap)),
			zap.Int("pods", len(cache.podsMap)),
			zap.Int("assumed", len(cache.assumedPods)),
			zap.Int("pendingAllocs", len(cache.pendingAllocations)),
			zap.Int("inProgressAllocs", len(cache.inProgressAllocations)),
			zap.Int("podsAssigned", cache.nodePodCount()),
			zap.Int("schedulingTasks", len(cache.schedulingTasks)),
			zap.Any("phases", cache.podPhases()))
	}
}

func (cache *SchedulerCache) podPhases() map[string]int {
	result := make(map[string]int)

	for _, pod := range cache.podsMap {
		key := string(pod.Status.Phase)
		count, ok := result[key]
		if !ok {
			count = 0
		}
		count++
		result[key] = count
	}
	return result
}

func (cache *SchedulerCache) nodePodCount() int {
	result := 0
	for _, node := range cache.nodesMap {
		result += len(node.Pods)
	}
	return result
}

// IsPVCUsedByPods determines if a given volume claim is in use by any current pods. This is explicitly for the use
// of the predicate shared lister and requires that the scheduler cache lock be held while accessing.
func (cache *SchedulerCache) IsPVCUsedByPods(key string) bool {
	_, ok := cache.pvcRefCounts[key]
	return ok
}

func (cache *SchedulerCache) updatePVCRefCounts(node *framework.NodeInfo, removeNode bool) {
	nodeName := node.Node().Name
	for k, v := range cache.pvcRefCounts {
		delete(v, nodeName)
		if len(v) == 0 {
			delete(cache.pvcRefCounts, k)
		}
	}

	if !removeNode {
		for k, count := range node.PVCRefCounts {
			entry, ok := cache.pvcRefCounts[k]
			if !ok {
				entry = make(map[string]int)
				cache.pvcRefCounts[k] = entry
			}
			entry[nodeName] = count
		}
	}
}

func (cache *SchedulerCache) GetSchedulerCacheDao() SchedulerCacheDao {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	nodes := make(map[string]NodeDao)
	pods := make(map[string]PodDao)
	priorityClasses := make(map[string]PriorityClassDao)
	podSchedulingInfoByUID := make(map[string]*PodSchedulingInfoDao)

	for nodeName, nodeInfo := range cache.nodesMap {
		node := nodeInfo.Node().DeepCopy()
		nodes[nodeName] = NodeDao{
			Name:              node.Name,
			UID:               node.UID,
			NodeInfo:          node.Status.NodeInfo,
			CreationTimestamp: node.CreationTimestamp.Time,
			Annotations:       node.Annotations,
			Labels:            node.Labels,
			PodCIDRs:          node.Spec.PodCIDRs,
			Taints:            node.Spec.Taints,
			Addresses:         node.Status.Addresses,
			Allocatable:       node.Status.Allocatable,
			Capacity:          node.Status.Capacity,
			Conditions:        node.Status.Conditions,
		}
	}

	for podUID, pod := range cache.podsMap {
		podCopy := pod.DeepCopy()
		podSchedulingInfoByUID[podUID] = &PodSchedulingInfoDao{
			Namespace: podCopy.Namespace,
			Name:      podCopy.Name,
			UID:       podCopy.UID,
		}
		containers := make([]ContainerDao, 0)
		for _, container := range podCopy.Spec.Containers {
			containers = append(containers, ContainerDao{
				Name:      container.Name,
				Resources: container.Resources,
			})
		}
		pods[fmt.Sprintf("%s/%s", podCopy.Namespace, podCopy.Name)] = PodDao{
			Namespace:         podCopy.Namespace,
			Name:              podCopy.Name,
			GenerateName:      podCopy.GenerateName,
			UID:               podCopy.UID,
			CreationTimestamp: podCopy.CreationTimestamp.Time,
			Annotations:       podCopy.Annotations,
			Labels:            podCopy.Labels,
			Affinity:          podCopy.Spec.Affinity,
			NodeName:          podCopy.Spec.NodeName,
			NodeSelector:      podCopy.Spec.NodeSelector,
			PriorityClassName: podCopy.Spec.PriorityClassName,
			Priority:          podCopy.Spec.Priority,
			PreemptionPolicy:  podCopy.Spec.PreemptionPolicy,
			SchedulerName:     podCopy.Spec.SchedulerName,
			Containers:        containers,
			Status:            podCopy.Status,
		}
	}

	for pcName, pc := range cache.pcMap {
		priorityClasses[pcName] = PriorityClassDao{
			Name:             pc.Name,
			Annotations:      pc.Annotations,
			Labels:           pc.Labels,
			Value:            pc.Value,
			GlobalDefault:    pc.GlobalDefault,
			PreemptionPolicy: pc.PreemptionPolicy,
		}
	}

	for podUID, nodeName := range cache.assignedPods {
		if info, ok := podSchedulingInfoByUID[podUID]; ok {
			info.AssignedNode = nodeName
		}
	}
	for podUID, allBound := range cache.assumedPods {
		if info, ok := podSchedulingInfoByUID[podUID]; ok {
			info.Assumed = true
			info.AllVolumesBound = allBound
		}
	}
	for podUID, nodeName := range cache.pendingAllocations {
		if info, ok := podSchedulingInfoByUID[podUID]; ok {
			info.PendingNode = nodeName
		}
	}
	for podUID, nodeName := range cache.inProgressAllocations {
		if info, ok := podSchedulingInfoByUID[podUID]; ok {
			info.InProgressNode = nodeName
		}
	}

	podSchedulingInfoByName := make(map[string]PodSchedulingInfoDao)
	for _, info := range podSchedulingInfoByUID {
		podSchedulingInfoByName[fmt.Sprintf("%s/%s", info.Namespace, info.Name)] = *info
	}

	return SchedulerCacheDao{
		Statistics: SchedulerCacheStatisticsDao{
			Nodes:                 len(cache.nodesMap),
			Pods:                  len(cache.podsMap),
			PriorityClasses:       len(cache.pcMap),
			Assumed:               len(cache.assumedPods),
			PendingAllocations:    len(cache.pendingAllocations),
			InProgressAllocations: len(cache.inProgressAllocations),
			PodsAssigned:          cache.nodePodCount(),
			Phases:                cache.podPhases(),
		},
		Nodes:           nodes,
		Pods:            pods,
		PriorityClasses: priorityClasses,
		SchedulingPods:  podSchedulingInfoByName,
	}
}

func podWithAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && (affinity.PodAffinity != nil || affinity.PodAntiAffinity != nil)
}

func podWithRequiredAntiAffinity(p *v1.Pod) bool {
	affinity := p.Spec.Affinity
	return affinity != nil && affinity.PodAntiAffinity != nil &&
		len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0
}
