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
	v1 "k8s.io/api/core/v1"
	storageV1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
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
		assumedPods:           make(map[string]bool),
		pendingAllocations:    make(map[string]string),
		inProgressAllocations: make(map[string]string),
		clients:               clients,
	}
	return cache
}

func (cache *SchedulerCache) GetNodesInfoMap() map[string]*framework.NodeInfo {
	return cache.nodesMap
}

func (cache *SchedulerCache) GetNodesInfoMapCopy() map[string]*framework.NodeInfo {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	copyOfMap := make(map[string]*framework.NodeInfo, len(cache.nodesMap))
	for k, v := range cache.nodesMap {
		copyOfMap[k] = v.Clone()
	}
	return copyOfMap
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

	_, ok := cache.nodesMap[node.Name]
	if !ok {
		n := framework.NewNodeInfo()
		cache.nodesMap[node.Name] = n
	}

	// make sure the node is always linked to the cached node object
	// Currently, SetNode API call always returns nil, never an error
	if err := cache.nodesMap[node.Name].SetNode(node); err != nil {
		// currently, this may never reached because SetNode always return nil
		// keep the check around to prevent the API changes to provide an error in some cases
		log.Logger().Error("failed to store v1.Node in cache", zap.Error(err))
	}
}

func (cache *SchedulerCache) UpdateNode(oldNode, newNode *v1.Node) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	n, ok := cache.nodesMap[newNode.Name]
	if !ok {
		log.Logger().Warn("updated node info not found, adding it to the cache",
			zap.String("nodeName", newNode.Name))
		n = framework.NewNodeInfo()
		cache.nodesMap[newNode.Name] = n
	}

	return n.SetNode(newNode)
}

func (cache *SchedulerCache) RemoveNode(node *v1.Node) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	return cache.removeNode(node)
}

func (cache *SchedulerCache) removeNode(node *v1.Node) error {
	_, ok := cache.nodesMap[node.Name]
	if !ok {
		return fmt.Errorf("node %v is not found", node.Name)
	}

	delete(cache.nodesMap, node.Name)
	return nil
}

// AddPendingPodAllocation is used to add a new pod -> node mapping to the cache when running in scheduler plugin mode.
// This function is called (in plugin mode) after a task is allocated by the YuniKorn scheduler.
func (cache *SchedulerCache) AddPendingPodAllocation(podKey string, nodeID string) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
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
	expectedNodeID, ok := cache.pendingAllocations[podKey]
	if ok && expectedNodeID == nodeID {
		delete(cache.pendingAllocations, podKey)
		cache.inProgressAllocations[podKey] = nodeID
	}
	return ok
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

// cache pod in the scheduler internal map, so it can be fast retrieved by UID,
// if pod is assigned to a node, update the cached nodes map too so that scheduler
// knows which pod is running before pod is bound to that node.
func (cache *SchedulerCache) AddPod(pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	currState, ok := cache.podsMap[key]
	if ok {
		// pod exists
		if cache.isAssumedPod(key) {
			if currState.Spec.NodeName != pod.Spec.NodeName {
				// pod was added to a different node than it was assumed to
				log.Logger().Warn("added pod has inconsistent pod location",
					zap.String("assumedLocation", pod.Spec.NodeName),
					zap.String("actualLocation", currState.Spec.NodeName))
			}
		}
		// update pod
		cache.updatePod(currState, pod, key)
	} else {
		// add pod
		cache.addPod(pod, key)
	}
	return nil
}

func (cache *SchedulerCache) UpdatePod(oldPod, newPod *v1.Pod) error {
	key, err := framework.GetPodKey(oldPod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	currState, ok := cache.podsMap[key]
	if ok {
		// pod exists and is assumed
		if !cache.isAssumedPod(key) && currState.Spec.NodeName != newPod.Spec.NodeName {
			// pod was added to a different node than it was assumed to
			log.Logger().Warn("updated pod found on different node than assigned to",
				zap.String("assumedLocation", newPod.Spec.NodeName),
				zap.String("actualLocation", currState.Spec.NodeName))
		}
		// update it
		cache.updatePod(oldPod, newPod, key)
	} else {
		// pod does not exist yet, add it
		cache.addPod(newPod, key)
	}
	return nil
}

// Assumes that lock is already acquired.
func (cache *SchedulerCache) addPod(pod *v1.Pod, key string) {
	if pod.Spec.NodeName != "" {
		n, ok := cache.nodesMap[pod.Spec.NodeName]
		if !ok {
			n = framework.NewNodeInfo()
			cache.nodesMap[pod.Spec.NodeName] = n
		}
		n.AddPod(pod)
	}
	cache.podsMap[key] = pod
}

func (cache *SchedulerCache) updatePod(oldPod, newPod *v1.Pod, key string) {
	// remove old version from cache
	cache.removePod(oldPod, false)

	// add new version to cache
	cache.addPod(newPod, key)
}

func (cache *SchedulerCache) RemovePod(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.removePod(pod, true)
}

func (cache *SchedulerCache) removePod(pod *v1.Pod, removeAlloc bool) {
	if removeAlloc {
		// remove pod from any pending or in-progress allocations
		delete(cache.pendingAllocations, string(pod.UID))
		delete(cache.inProgressAllocations, string(pod.UID))
	}

	// remove pod from cache
	delete(cache.podsMap, string(pod.UID))

	// remove pod from node
	if pod.Spec.NodeName != "" {
		n, ok := cache.nodesMap[pod.Spec.NodeName]
		if ok {
			if err := n.RemovePod(pod); err != nil {
				log.Logger().Warn("unable to remove pod from node",
					zap.String("pod", pod.Name),
					zap.String("node", pod.Spec.NodeName),
					zap.Error(err))
			}
		} else {
			log.Logger().Warn("unable to find node for pod removal",
				zap.String("pod", pod.Name),
				zap.String("node", pod.Spec.NodeName))
		}
	}
}

func (cache *SchedulerCache) GetPod(uid string) (*v1.Pod, bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if pod, ok := cache.podsMap[uid]; ok {
		return pod, true
	}
	return nil, false
}

func (cache *SchedulerCache) AssumePod(pod *v1.Pod, allBound bool) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	cache.addPod(pod, key)
	cache.assumedPods[key] = allBound

	return nil
}

func (cache *SchedulerCache) ForgetPod(pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	currState, ok := cache.podsMap[key]
	if ok && currState.Spec.NodeName != pod.Spec.NodeName {
		log.Logger().Warn("pod was assumed on one node but found on another",
			zap.String("pod", key),
			zap.String("expectedNode", currState.Spec.NodeName),
			zap.String("actualNode", pod.Spec.NodeName))
	}

	delete(cache.assumedPods, key)
	delete(cache.pendingAllocations, key)
	delete(cache.inProgressAllocations, key)
	return nil
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
