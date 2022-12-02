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
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	storageV1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
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
	assignedPods          map[string]string // map of pods to the node they are currently assigned to
	assumedPods           map[string]bool   // map of assumed pods, value indicates if pod volumes are all bound
	pendingAllocations    map[string]string // map of pod to node ID, presence indicates a pending allocation for scheduler
	inProgressAllocations map[string]string // map of pod to node ID, presence indicates an in-process allocation for scheduler
	lock                  sync.RWMutex
	clients               *client.Clients // client APIs
}

func NewSchedulerCache(clients *client.Clients) *SchedulerCache {
	cache := &SchedulerCache{
		nodesMap:              make(map[string]*framework.NodeInfo),
		podsMap:               make(map[string]*v1.Pod),
		assignedPods:          make(map[string]string),
		assumedPods:           make(map[string]bool),
		pendingAllocations:    make(map[string]string),
		inProgressAllocations: make(map[string]string),
		clients:               clients,
	}
	return cache
}

// GetNodesInfoMap returns a reference to the internal node map. This is explicitly for the use of the predicate
// shared lister and requires that the scheduler cache lock be held while accessing.
func (cache *SchedulerCache) GetNodesInfoMap() map[string]*framework.NodeInfo {
	return cache.nodesMap
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

func (cache *SchedulerCache) AddNode(node *v1.Node) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("AddNode.Pre")
	defer cache.dumpState("AddNode.Post")

	cache.updateNode(node)
}

func (cache *SchedulerCache) UpdateNode(newNode *v1.Node) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("UpdateNode.Pre")
	defer cache.dumpState("UpdateNode.Post")

	cache.updateNode(newNode)
}

func (cache *SchedulerCache) updateNode(node *v1.Node) {
	nodeInfo, ok := cache.nodesMap[node.Name]
	if !ok {
		log.Logger().Debug("Adding node to cache", zap.String("nodeName", node.Name))
		nodeInfo = framework.NewNodeInfo()
		cache.nodesMap[node.Name] = nodeInfo
	} else {
		log.Logger().Debug("Updating node in cache", zap.String("nodeName", node.Name))
	}
	nodeInfo.SetNode(node)
}

func (cache *SchedulerCache) RemoveNode(node *v1.Node) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("RemoveNode.Pre")
	defer cache.dumpState("RemoveNode.Post")

	cache.removeNode(node)
}

func (cache *SchedulerCache) removeNode(node *v1.Node) {
	nodeInfo, ok := cache.nodesMap[node.Name]
	if !ok {
		log.Logger().Debug("Attempted to remove non-existent node", zap.String("nodeName", node.Name))
		return
	}

	for _, pod := range nodeInfo.Pods {
		key := string(pod.Pod.UID)
		delete(cache.assignedPods, key)
		delete(cache.assumedPods, key)
		delete(cache.pendingAllocations, key)
		delete(cache.inProgressAllocations, key)
	}

	log.Logger().Debug("Removing node from cache", zap.String("nodeName", node.Name))
	delete(cache.nodesMap, node.Name)
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

// return if pod is assumed in cache, avoid nil
func (cache *SchedulerCache) isAssumedPod(podKey string) bool {
	_, ok := cache.assumedPods[podKey]
	return ok
}

func (cache *SchedulerCache) ArePodVolumesAllBound(podKey string) bool {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	return cache.assumedPods[podKey]
}

// AddPod adds a pod to the scheduler cache
func (cache *SchedulerCache) AddPod(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("AddPod.Pre")
	defer cache.dumpState("AddPod.Post")
	cache.updatePod(pod)
}

// UpdatePod updates a pod in the cache
func (cache *SchedulerCache) UpdatePod(newPod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.dumpState("UpdatePod.Pre")
	defer cache.dumpState("UpdatePod.Post")
	cache.updatePod(newPod)
}

func (cache *SchedulerCache) updatePod(pod *v1.Pod) {
	key := string(pod.UID)

	currState, ok := cache.podsMap[key]
	if ok {
		// remove current version of pod
		delete(cache.podsMap, key)
		nodeName, ok := cache.assignedPods[key]
		if ok {
			nodeInfo, ok := cache.nodesMap[nodeName]
			if ok {
				if err := nodeInfo.RemovePod(currState); err != nil {
					log.Logger().Warn("BUG: Failed to remove pod from node",
						zap.String("podName", currState.Name),
						zap.String("nodeName", nodeName),
						zap.Error(err))
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
	}

	if utils.IsAssignedPod(pod) && !utils.IsPodTerminated(pod) {
		// assign to node
		nodeInfo, ok := cache.nodesMap[pod.Spec.NodeName]
		if !ok {
			// node doesn't exist, create a synthetic one for now
			nodeInfo = framework.NewNodeInfo()
			cache.nodesMap[pod.Spec.NodeName] = nodeInfo
			// work around a crash bug in NodeInfo.RemoveNode() when Node is unset
			nodeInfo.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: pod.Spec.NodeName}})
		}
		nodeInfo.AddPod(pod)
		cache.assignedPods[key] = pod.Spec.NodeName
	}

	// if pod is not in a terminal state, add it back into cache
	if !utils.IsPodTerminated(pod) {
		log.Logger().Debug("Putting pod in cache", zap.String("podName", pod.Name), zap.String("podKey", key))
		cache.podsMap[key] = pod
	} else {
		log.Logger().Debug("Removing terminated pod from cache", zap.String("podName", pod.Name), zap.String("podKey", key))
		delete(cache.podsMap, key)
		delete(cache.assignedPods, key)
		delete(cache.assumedPods, key)
		delete(cache.pendingAllocations, key)
		delete(cache.inProgressAllocations, key)
	}
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
	log.Logger().Debug("Removing deleted pod from cache", zap.String("podName", pod.Name), zap.String("podKey", key))
	nodeName, ok := cache.assignedPods[key]
	if ok {
		nodeInfo, ok := cache.nodesMap[nodeName]
		if ok {
			if err := nodeInfo.RemovePod(pod); err != nil {
				log.Logger().Warn("BUG: Failed to remove pod from node",
					zap.String("podName", pod.Name),
					zap.String("nodeName", nodeName),
					zap.Error(err))
			}
		}
	}
	delete(cache.podsMap, key)
	delete(cache.assignedPods, key)
	delete(cache.assumedPods, key)
	delete(cache.pendingAllocations, key)
	delete(cache.inProgressAllocations, key)
}

func (cache *SchedulerCache) GetPod(uid string) (*v1.Pod, bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if pod, ok := cache.podsMap[uid]; ok {
		return pod, true
	}
	return nil, false
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

	log.Logger().Debug("Adding assumed pod to cache",
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
	log.Logger().Debug("Removing assumed pod from cache",
		zap.String("podName", pod.Name),
		zap.String("podKey", key))

	delete(cache.assumedPods, key)
	delete(cache.pendingAllocations, key)
	delete(cache.inProgressAllocations, key)
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
	return cache.clients.StorageInformer.Lister().Get(className)
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
	if log.Logger().Core().Enabled(zapcore.DebugLevel) {
		log.Logger().Debug("Scheduler cache state ("+context+")",
			zap.Int("nodes", len(cache.nodesMap)),
			zap.Int("pods", len(cache.podsMap)),
			zap.Int("assumed", len(cache.assumedPods)),
			zap.Int("pendingAllocs", len(cache.pendingAllocations)),
			zap.Int("inProgressAllocs", len(cache.inProgressAllocations)),
			zap.Int("podsAssigned", cache.nodePodCount()),
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

func (cache *SchedulerCache) GetSchedulerCacheDao() SchedulerCacheDao {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	nodes := make(map[string]NodeDao)
	pods := make(map[string]PodDao)
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
			Assumed:               len(cache.assumedPods),
			PendingAllocations:    len(cache.pendingAllocations),
			InProgressAllocations: len(cache.inProgressAllocations),
			PodsAssigned:          cache.nodePodCount(),
			Phases:                cache.podPhases(),
		},
		Nodes:          nodes,
		Pods:           pods,
		SchedulingPods: podSchedulingInfoByName,
	}
}
