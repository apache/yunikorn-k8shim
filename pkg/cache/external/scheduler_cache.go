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
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	"k8s.io/kubernetes/pkg/scheduler/factory"
	schedulernode "k8s.io/kubernetes/pkg/scheduler/nodeinfo"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

// scheduler cache maintains some critical information about nodes and pods used for scheduling
// nodes are cached in the form of de-scheduler nodeInfo, instead of re-creating all nodes info from scratch,
// we replicate nodes info from de-scheduler, in order to re-use predicates functions.
type SchedulerCache struct {
	// node name to NodeInfo map
	nodesMap map[string]*schedulernode.NodeInfo
	podsMap  map[string]*v1.Pod
	// this is a map of assumed pods,
	// the value indicates if a pod volumes are all bound
	assumedPods map[string]bool
	lock        sync.RWMutex
	// client APIs
	clients *client.Clients
}

func NewSchedulerCache(clients *client.Clients) *SchedulerCache {
	cache := &SchedulerCache{
		nodesMap:    make(map[string]*schedulernode.NodeInfo),
		podsMap:     make(map[string]*v1.Pod),
		assumedPods: make(map[string]bool),
		clients:     clients,
	}
	cache.assignArgs(GetPluginArgs())
	return cache
}

func (cache *SchedulerCache) GetNodesInfoMap() map[string]*schedulernode.NodeInfo {
	return cache.nodesMap
}

func (cache *SchedulerCache) assignArgs(args *factory.PluginFactoryArgs) {
	// nodes cache implemented PodLister and NodeInfo interface
	log.Logger.Debug("Initialising PluginFactoryArgs using SchedulerCache")
	args.PodLister = cache
	args.NodeInfo = cache
	args.VolumeBinder = cache.clients.VolumeBinder
	args.PVInfo = &predicates.CachedPersistentVolumeInfo{PersistentVolumeLister: cache.clients.PVInformer.Lister()}
	args.PVCInfo = &predicates.CachedPersistentVolumeClaimInfo{PersistentVolumeClaimLister: cache.clients.PVCInformer.Lister()}
	args.StorageClassInfo = &predicates.CachedStorageClassInfo{StorageClassLister: cache.clients.StorageInformer.Lister()}
}

func (cache *SchedulerCache) GetNode(name string) *schedulernode.NodeInfo {
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
		n := schedulernode.NewNodeInfo()
		cache.nodesMap[node.Name] = n
	}

	// make sure the node is always linked to the cached node object
	// Currently, SetNode API call always returns nil, never an error
	if err := cache.nodesMap[node.Name].SetNode(node); err != nil {
		// currently, this may never reached because SetNode always return nil
		// keep the check around to prevent the API changes to provide an error in some cases
		log.Logger.Error("failed to store v1.Node in cache", zap.Error(err))
	}
}

func (cache *SchedulerCache) UpdateNode(oldNode, newNode *v1.Node) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	n, ok := cache.nodesMap[newNode.Name]
	if !ok {
		log.Logger.Warn("updated node info not found, adding it to the cache",
			zap.String("nodeName", newNode.Name))
		n = schedulernode.NewNodeInfo()
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
	key, err := schedulernode.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	currState, ok := cache.podsMap[key]
	switch {
	case ok && cache.isAssumedPod(key):
		if currState.Spec.NodeName != pod.Spec.NodeName {
			// The pod was added to a different node than it was assumed to.
			log.Logger.Warn("inconsistent pod location",
				zap.String("assumedLocation", pod.Spec.NodeName),
				zap.String("actualLocation", currState.Spec.NodeName))

			// Clean this up.
			err = cache.removePod(currState)
			if err != nil {
				log.Logger.Debug("node not in cache",
					zap.Error(err))
			}
			cache.addPod(pod)
		}
		delete(cache.assumedPods, key)
		cache.podsMap[key] = pod
	case !ok:
		// Pod was expired. We should add it back.
		cache.addPod(pod)
		cache.podsMap[key] = pod
	default:
		log.Logger.Debug("pod was already in added state", zap.String("pod", key))
	}
	return nil
}

func (cache *SchedulerCache) UpdatePod(oldPod, newPod *v1.Pod) error {
	key, err := schedulernode.GetPodKey(oldPod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	currState, ok := cache.podsMap[key]
	switch {
	// An assumed pod won't have Update/Remove event. It needs to have Add event
	// before Update event, in which case the state would change from Assumed to Added.
	case ok && !cache.isAssumedPod(key):
		if currState.Spec.NodeName != newPod.Spec.NodeName {
			log.Logger.Error("pod updated on a different node than previously added to", zap.String("pod", key))
			log.Logger.Error("scheduler cache is corrupted and can badly affect scheduling decisions")
		}
		if err = cache.updatePod(oldPod, newPod); err != nil {
			return err
		}
		cache.podsMap[key] = newPod
	default:
		return fmt.Errorf("pod %v is not added to scheduler cache, so cannot be updated", key)
	}
	return nil
}

// Assumes that lock is already acquired.
func (cache *SchedulerCache) addPod(pod *v1.Pod) {
	if pod.Spec.NodeName != "" {
		n, ok := cache.nodesMap[pod.Spec.NodeName]
		if !ok {
			n = schedulernode.NewNodeInfo()
			cache.nodesMap[pod.Spec.NodeName] = n
		}
		n.AddPod(pod)
	}
}

func (cache *SchedulerCache) updatePod(oldPod, newPod *v1.Pod) error {
	if err := cache.removePod(oldPod); err != nil {
		return err
	}
	cache.addPod(newPod)
	return nil
}

func (cache *SchedulerCache) RemovePod(pod *v1.Pod) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.removePod(pod)
}

func (cache *SchedulerCache) removePod(pod *v1.Pod) error {
	n, ok := cache.nodesMap[pod.Spec.NodeName]
	if !ok {
		return fmt.Errorf("node %v is not found", pod.Spec.NodeName)
	}
	if err := n.RemovePod(pod); err != nil {
		return err
	}
	return nil
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
	key, err := schedulernode.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()
	//if _, ok := cache.podsMap[key]; ok {
	//	return fmt.Errorf("pod %v is in the cache, so can't be assumed", key)
	//}

	cache.addPod(pod)
	cache.podsMap[key] = pod
	cache.assumedPods[key] = allBound

	return nil
}

func (cache *SchedulerCache) ForgetPod(pod *v1.Pod) error {
	key, err := schedulernode.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	currState, ok := cache.podsMap[key]
	if ok && currState.Spec.NodeName != pod.Spec.NodeName {
		return fmt.Errorf("pod %v was assumed on %v but assigned to %v",
			key, pod.Spec.NodeName, currState.Spec.NodeName)
	}

	switch {
	// Only assumed pod can be forgotten.
	case ok && cache.isAssumedPod(key):
		err = cache.removePod(pod)
		if err != nil {
			return err
		}
		delete(cache.assumedPods, key)
		delete(cache.podsMap, key)
	default:
		return fmt.Errorf("pod %v wasn't assumed so cannot be forgotten", key)
	}
	return nil
}

// Implement scheduler/algorithm/types.go#PodLister interface
func (cache *SchedulerCache) List(selector labels.Selector) ([]*v1.Pod, error) {
	alwaysTrue := func(p *v1.Pod) bool { return true }
	return cache.FilteredList(alwaysTrue, selector)
}

// Implement scheduler/algorithm/types.go#PodLister interface
func (cache *SchedulerCache) FilteredList(podFilter algorithm.PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	// podFilter is expected to return true for most or all of the pods. We
	// can avoid expensive array growth without wasting too much memory by
	// pre-allocating capacity.
	maxSize := 0
	for _, nodeInfo := range cache.nodesMap {
		maxSize += len(nodeInfo.Pods())
	}
	pods := make([]*v1.Pod, 0, maxSize)
	for _, nodeInfo := range cache.nodesMap {
		for _, pod := range nodeInfo.Pods() {
			if podFilter(pod) && selector.Matches(labels.Set(pod.Labels)) {
				pods = append(pods, pod)
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
