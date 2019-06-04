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

package external

import (
	"fmt"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/factory"
	deschedulernode "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"sync"
)

// this is a cache of nodes in the form of de-scheduler nodeInfo,
// instead of re-creating all nodes info from scratch, we replicate
// nodes info from de-scheduler, in order to re-use predicates functions.
type CachedNodes struct {
	// node name to NodeInfo map
	nodesMap map[string]*deschedulernode.NodeInfo
	lock sync.RWMutex
}

func NewCachedNodes() *CachedNodes {
	return &CachedNodes {
		nodesMap: make(map[string]*deschedulernode.NodeInfo),
	}
}

func (cache *CachedNodes) assignArgs(args factory.PluginFactoryArgs) {
	// nodes cache implemented PodLister and NodeInfo interface
	args.PodLister = cache
	args.NodeInfo = cache
}

func (cache *CachedNodes) GetNode(name string) *deschedulernode.NodeInfo {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	if n, ok := cache.nodesMap[name]; ok {
		return n
	}
	return nil
}

func (cache *CachedNodes) AddNode(node *v1.Node) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	n, ok := cache.nodesMap[node.Name]
	if !ok {
		n = deschedulernode.NewNodeInfo()
		n.SetNode(node)
		cache.nodesMap[node.Name] = n
	}
}

func (cache *CachedNodes) UpdateNode(oldNode, newNode *v1.Node) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	n, ok := cache.nodesMap[oldNode.Name]
	if ok {
		n.RemoveNode(oldNode)
		n.SetNode(newNode)
	}
	return nil
}

func (cache *CachedNodes) RemoveNode(node *v1.Node) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	_, ok := cache.nodesMap[node.Name]
	if !ok {
		return fmt.Errorf("node %v is not found", node.Name)
	}

	delete(cache.nodesMap, node.Name)
	return nil
}


func (cache *CachedNodes) AddPod(pod *v1.Pod) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	n, ok := cache.nodesMap[pod.Spec.NodeName]
	if !ok {
		n = deschedulernode.NewNodeInfo()
		cache.nodesMap[pod.Spec.NodeName] = n
	}
	n.AddPod(pod)
}

func (cache *CachedNodes) UpdatePod(oldPod, newPod *v1.Pod) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	if err := cache.RemovePod(oldPod); err != nil {
		return err
	}
	cache.AddPod(newPod)
	return nil
}

func (cache *CachedNodes) RemovePod(pod *v1.Pod) error {
	cache.lock.Lock()
	defer cache.lock.Unlock()

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
func (cache *CachedNodes) List(selector labels.Selector) ([]*v1.Pod, error) {
	alwaysTrue := func(p *v1.Pod) bool { return true }
	return cache.FilteredList(alwaysTrue, selector)
}

// Implement scheduler/algorithm/types.go#PodLister interface
func (cache *CachedNodes) FilteredList(podFilter algorithm.PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
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
func (cache *CachedNodes) GetNodeInfo(nodeName string) (*v1.Node, error) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	if nodeInfo, ok := cache.nodesMap[nodeName]; ok {
		return nodeInfo.Node(), nil
	}
	return nil, fmt.Errorf("node %s is not found", nodeName)
}
