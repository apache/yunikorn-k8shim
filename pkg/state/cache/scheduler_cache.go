/*
Copyright 2019 The Unity Scheduler Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"fmt"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/factory"
	deschedulernode "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"sync"
)

// scheduler cache maintains some critical information about nodes and pods used for scheduling
// nodes are cached in the form of de-scheduler nodeInfo, instead of re-creating all nodes info from scratch,
// we replicate nodes info from de-scheduler, in order to re-use predicates functions.
type SchedulerCache struct {
	// node name to NodeInfo map
	nodesMap map[string]*deschedulernode.NodeInfo
	podsMap  map[string]*v1.Pod
	lock     sync.RWMutex
}

func NewSchedulerCache() *SchedulerCache {
	cache := &SchedulerCache {
		nodesMap: make(map[string]*deschedulernode.NodeInfo),
		podsMap:  make(map[string]*v1.Pod),
	}
	cache.assignArgs(GetPluginArgs())
	return cache
}

func (cache *SchedulerCache) GetNodesInfoMap() map[string]*deschedulernode.NodeInfo {
	return cache.nodesMap
}

func (cache *SchedulerCache) assignArgs(args *factory.PluginFactoryArgs) {
	// nodes cache implemented PodLister and NodeInfo interface
	glog.V(5).Infof("PluginFactoryArgs#PodLister -> cachedNodes")
	glog.V(5).Infof("PluginFactoryArgs#NodeInfo -> cachedNodes")
	args.PodLister = cache
	args.NodeInfo = cache
}

func (cache *SchedulerCache) GetNode(name string) *deschedulernode.NodeInfo {
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

	n, ok := cache.nodesMap[node.Name]
	if !ok {
		n = deschedulernode.NewNodeInfo()
		n.SetNode(node)
		cache.nodesMap[node.Name] = n
	}
}

func (cache *SchedulerCache) UpdateNode(oldNode, newNode *v1.Node) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	n, ok := cache.nodesMap[oldNode.Name]
	if ok {
		n.RemoveNode(oldNode)
		n.SetNode(newNode)
	}
	return nil
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

func (cache *SchedulerCache) AddPod(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.addPod(pod)
}

func (cache *SchedulerCache) addPod(pod *v1.Pod) {
	n, ok := cache.nodesMap[pod.Spec.NodeName]
	if !ok {
		n = deschedulernode.NewNodeInfo()
		cache.nodesMap[pod.Spec.NodeName] = n
	}
	n.AddPod(pod)
}

func (cache *SchedulerCache) AddPodToCache(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	cache.podsMap[string(pod.UID)] = pod
}

func (cache *SchedulerCache) InvalidatePodFromCache(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	delete(cache.podsMap, string(pod.UID))
}

func (cache *SchedulerCache) GetPod(uid string) (*v1.Pod, bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if pod, ok := cache.podsMap[uid]; ok {
		return pod, true
	} else {
		return nil, false
	}
}

func (cache *SchedulerCache) UpdatePod(oldPod, newPod *v1.Pod) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	return cache.updatePod(oldPod, newPod)
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
