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
	"testing"

	"gotest.tools/v3/assert"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/client"
)

const (
	host1    = "host0001"
	host2    = "host0002"
	nodeUID1 = "Node-UID-00001"
	nodeUID2 = "Node-UID-00002"
	podName1 = "pod0001"
	podName2 = "pod0002"
	podUID1  = "Pod-UID-00001"
	podUID2  = "Pod-UID-00002"
	pvcName1 = "pvc0001"
	pvcName2 = "pvc0002"
)

// this test verifies that no matter which comes first, pod or node,
// the cache should be updated correctly to contain the correct references
// for assigned pod, it is stored in the cached node too.
func TestAssignedPod(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName1,
			UID:  podUID1,
		},
		Spec: v1.PodSpec{
			NodeName: host1,
		},
	}

	testCases := []struct {
		name string
		arg0 interface{}
		arg1 interface{}
	}{
		{"pod first node second", pod, node},
		{"node first pod second", node, pod},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := add2Cache(cache, tc.arg0, tc.arg1)
			assert.NilError(t, err, "add object to scheduler cache failed")

			// verify the node is added to cache
			// the cached node has reference to the v1.Node object
			// the cached node has one pod assigned
			cachedNode := cache.GetNode(node.Name)
			assert.Check(t, cachedNode != nil, "host0001 is not found in cache")
			assert.Check(t, cachedNode.Node() != nil, "host0001 exists in cache but the ref to v1.Node doesn't exist")
			assert.Equal(t, cachedNode.Node().Name, node.Name)
			assert.Equal(t, cachedNode.Node().UID, node.UID)
			// nolint:staticcheck
			assert.Equal(t, len(cachedNode.Pods), 1)

			// verify the pod is added to cache
			// the pod should be added to the node as well
			cachedPod := cache.GetPod(podUID1)
			assert.Check(t, cachedPod != nil)
			assert.Equal(t, cachedPod.Name, pod.Name)
			assert.Equal(t, cachedPod.UID, pod.UID)
		})
	}
}

func TestRemovePodWithoutNodeName(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	cache.removePod(&v1.Pod{Spec: v1.PodSpec{}})
}

// this test verifies that no matter which comes first, pod or node,
// the cache should be updated correctly to contain the correct references
// for unassigned pod, it will not be stored in the cached node.
func TestAddUnassignedPod(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName1,
			UID:  podUID1,
		},
		Spec: v1.PodSpec{},
	}

	testCases := []struct {
		name string
		arg0 interface{}
		arg1 interface{}
	}{
		{"pod first node second", pod, node},
		{"node first pod second", node, pod},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := add2Cache(cache, tc.arg0, tc.arg1)
			assert.NilError(t, err, "add object to scheduler cache failed")

			// verify the node is added to cache
			// the cached node has reference to the v1.Node object
			// the cached node has no pods assigned
			cachedNode := cache.GetNode(node.Name)
			assert.Check(t, cachedNode != nil, "host0001 is not found in cache")
			assert.Check(t, cachedNode.Node() != nil, "host0001 exists in cache but the ref to v1.Node doesn't exist")
			assert.Equal(t, cachedNode.Node().Name, node.Name)
			assert.Equal(t, cachedNode.Node().UID, node.UID)
			// nolint:staticcheck
			assert.Equal(t, len(cachedNode.Pods), 0)

			// verify the pod is added to cache
			// the pod should be added to the node as well
			cachedPod := cache.GetPod(podUID1)
			assert.Check(t, cachedPod != nil)
			assert.Equal(t, cachedPod.Name, pod.Name)
			assert.Equal(t, cachedPod.UID, pod.UID)
		})
	}
}

func TestUpdateNode(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	// old node, state: unschedulable
	oldNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}

	// old node, state: schedulable
	newNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	// first add the old node
	cache.UpdateNode(oldNode)

	// make sure the node is added to the cache
	nodeInCache := cache.GetNode(host1)
	assert.Assert(t, nodeInCache.Node() != nil)
	assert.Equal(t, nodeInCache.Node().Name, host1)
	assert.Equal(t, nodeInCache.Node().Spec.Unschedulable, true)

	// then update the node
	cache.UpdateNode(newNode)

	// make sure the node in cache also gets updated
	// unschedulable -> schedulable
	assert.Assert(t, nodeInCache.Node() != nil)
	assert.Equal(t, nodeInCache.Node().Name, host1)
	assert.Equal(t, nodeInCache.Node().Spec.Unschedulable, false)

	cache.RemoveNode(newNode)
	assert.Equal(t, 0, len(cache.nodesInfo), "nodesInfo list size")
}

func TestGetNodesInfo(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	assert.Assert(t, cache.nodesInfo == nil)
	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}
	cache.UpdateNode(node)
	assert.Assert(t, cache.nodesInfo == nil)
	nodesInfo := cache.GetNodesInfo()
	expectHost(t, host1, nodesInfo)

	// update
	updatedNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	cache.UpdateNode(updatedNode)
	expectHost(t, host1, nodesInfo)

	// add new
	newNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host2,
			Namespace: "default",
			UID:       nodeUID2,
		},
	}
	cache.UpdateNode(newNode)
	assert.Assert(t, cache.nodesInfo == nil, "nodesInfo list was not invalidated")
	nodesInfo = cache.GetNodesInfo()
	expectHost1AndHost2(t, nodesInfo)

	// remove
	cache.RemoveNode(node)
	assert.Assert(t, cache.nodesInfo == nil, "nodesInfo list was not invalidated")
	nodesInfo = cache.GetNodesInfo()
	expectHost(t, host2, nodesInfo)
}

//nolint:funlen
func TestGetNodesInfoPodsWithAffinity(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil)
	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}
	cache.UpdateNode(node)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil)
	cache.AssumePod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName1,
			UID:  podUID1,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAffinity: &v1.PodAffinity{},
			},
			NodeName: host1,
		},
	}, true)
	nodesInfo := cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)

	// add node + pod
	newNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host2,
			Namespace: "default",
			UID:       nodeUID2,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	cache.UpdateNode(newNode)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "nodesInfo list was not invalidated")
	cache.AssumePod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAffinity: &v1.PodAffinity{},
			},
			NodeName: host2,
		},
	}, true)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "nodesInfo list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost1AndHost2(t, nodesInfo)

	// remove node
	cache.RemoveNode(newNode)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "nodesInfo list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)

	// update node
	updatedNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	cache.UpdateNode(updatedNode)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "node list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)

	// add pod
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAffinity: &v1.PodAffinity{},
			},
			NodeName: host1,
		},
	}
	cache.AssumePod(pod2, true)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "node list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)

	// remove pod
	cache.RemovePod(pod2)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "node list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)

	// add & update pod w/o affinity
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
	}
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)
	cache.assumePod(pod3, true)
	cache.updatePod(pod3)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity != nil, "node list was invalidated")

	// add & update pod w/ affinity
	pod4 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAffinity: &v1.PodAffinity{},
			},
			NodeName: host1,
		},
	}
	nodesInfo = cache.GetNodesInfoPodsWithAffinity()
	expectHost(t, host1, nodesInfo)
	cache.assumePod(pod4, true)
	cache.updatePod(pod4)
	assert.Assert(t, cache.nodesInfoPodsWithAffinity == nil, "node list was not invalidated")
}

//nolint:funlen
func TestGetNodesInfoPodsWithReqAntiAffinity(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil)
	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}
	cache.UpdateNode(node)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil)
	cache.AssumePod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName1,
			UID:  podUID1,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{}},
				},
			},
			NodeName: host1,
		},
	}, true)
	nodesInfo := cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)

	// add node + pod
	newNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host2,
			Namespace: "default",
			UID:       nodeUID2,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	cache.UpdateNode(newNode)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "nodesInfo list was not invalidated")
	cache.AssumePod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{}},
				},
			},
			NodeName: host2,
		},
	}, true)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "nodesInfo list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost1AndHost2(t, nodesInfo)

	// remove node
	cache.RemoveNode(newNode)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "nodesInfo list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)

	// update node
	updatedNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	cache.UpdateNode(updatedNode)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "node list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)

	// add pod
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{}},
				},
			},
			NodeName: host1,
		},
	}
	cache.AssumePod(pod2, true)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "node list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)

	// remove pod
	cache.RemovePod(pod2)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "node list was not invalidated")
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)

	// add & update pod w/o anti-affinity
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
	}
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)
	cache.assumePod(pod3, true)
	cache.updatePod(pod3)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity != nil, "node list was invalidated")

	// add & update pod w/ anti-affinity
	pod4 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName2,
			UID:  podUID2,
		},
		Spec: v1.PodSpec{
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{}},
				},
			},
			NodeName: host1,
		},
	}
	nodesInfo = cache.GetNodesInfoPodsWithReqAntiAffinity()
	expectHost(t, host1, nodesInfo)
	cache.assumePod(pod4, true)
	cache.updatePod(pod4)
	assert.Assert(t, cache.nodesInfoPodsWithReqAntiAffinity == nil, "node list was not invalidated")
}

func TestUpdateNonExistNode(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	// old node, state: schedulable
	newNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	cache.UpdateNode(newNode)

	nodeInCache := cache.GetNode(host1)
	assert.Assert(t, nodeInCache.Node() != nil)
	assert.Equal(t, nodeInCache.Node().Name, host1)
	assert.Equal(t, nodeInCache.Node().Spec.Unschedulable, false)
}

func add2Cache(cache *SchedulerCache, objects ...interface{}) error {
	for _, obj := range objects {
		switch podOrNode := obj.(type) {
		case *v1.Node:
			cache.UpdateNode(podOrNode)
		case *v1.Pod:
			cache.UpdatePod(podOrNode)
		default:
			return fmt.Errorf("unknown object type")
		}
	}
	return nil
}

func TestGetNodesInfoMap(t *testing.T) {
	// empty map
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	ref := cache.GetNodesInfoMap()
	assert.Equal(t, len(ref), 0)

	for i := 0; i < 10; i++ {
		cache.UpdateNode(&v1.Node{
			ObjectMeta: apis.ObjectMeta{
				Name: fmt.Sprintf("node-%d", i),
				Labels: map[string]string{
					"a": "a1",
					"b": "b1",
				},
				Annotations: map[string]string{
					"a": "a1",
					"b": "b1",
					"c": "c1",
				},
			},
		})
	}

	ref = cache.GetNodesInfoMap()
	assert.Equal(t, len(ref), 10)
	for k, v := range ref {
		assert.Assert(t, v.Node() != nil, "node %s should not be nil", k)
		assert.Equal(t, len(v.Node().Labels), 2)
		assert.Equal(t, len(v.Node().Annotations), 3)
	}
}

func TestUpdatePod(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	node1 := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	node2 := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host2,
			Namespace: "default",
			UID:       nodeUID2,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	cache.UpdateNode(node1)
	cache.UpdateNode(node2)

	podTemplate := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Annotations: map[string]string{"state": "new"},
		},
		Spec: v1.PodSpec{},
	}

	pod1 := podTemplate.DeepCopy()
	pod1.ObjectMeta.Name = podName1
	pod1.ObjectMeta.UID = podUID1
	cache.UpdatePod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1")
	pod := cache.GetPod(podUID1)
	assert.Check(t, pod != nil, "pod1 not found")

	// update of non-existent pod should be equivalent to an add
	pod2 := podTemplate.DeepCopy()
	pod2.ObjectMeta.Name = podName2
	pod2.ObjectMeta.UID = podUID2
	cache.UpdatePod(pod2)
	assert.Equal(t, len(cache.podsMap), 2, "wrong pod count after add of pod2")
	pod = cache.GetPod(podUID2)
	assert.Check(t, pod != nil, "pod2 not found")

	// normal pod update should succeed
	pod1Copy := pod1.DeepCopy()
	pod1Copy.ObjectMeta.Annotations["state"] = "updated"
	cache.UpdatePod(pod1Copy)
	found := cache.GetPod(podUID1)
	assert.Check(t, found != nil, "pod1 not found")
	assert.Equal(t, found.GetAnnotations()["state"], "updated", "wrong state after update")
	cache.RemovePod(pod1Copy)

	// assumed pod should still be assumed if node changes
	pod1.Spec.NodeName = node1.Name
	cache.UpdatePod(pod1)
	cache.AssumePod(pod1, true)
	assert.Check(t, cache.isAssumedPod(podUID1), "pod is not assumed")
	pod1Copy = pod1.DeepCopy()
	pod1Copy.Spec.NodeName = node2.Name
	cache.UpdatePod(pod1Copy)
	assert.Check(t, cache.isAssumedPod(podUID1), "pod is not assumed after re-add")

	// unassumed pod should survive node changing without crashing
	pod3 := podTemplate.DeepCopy()
	pod3.ObjectMeta.Name = "pod00003"
	pod3.ObjectMeta.UID = "Pod-UID-00003"
	pod3.Spec.NodeName = "orig-node"
	cache.UpdatePod(pod3)
	pod3Copy := pod3.DeepCopy()
	pod3Copy.Spec.NodeName = "new-node"
	cache.UpdatePod(pod3Copy)
	pod3Result := cache.GetPod("Pod-UID-00003")
	assert.Check(t, pod3Result != nil, "unable to get pod3")
	assert.Equal(t, pod3Result.Spec.NodeName, "new-node", "node name not updated")
}

func TestRemovePod(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        podName1,
			UID:         podUID1,
			Annotations: map[string]string{"state": "new"},
		},
		Spec: v1.PodSpec{},
	}

	// add pod1
	cache.UpdatePod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1")
	pod := cache.GetPod(podUID1)
	assert.Check(t, pod != nil, "pod1 not found")

	// remove pod1
	cache.RemovePod(pod1)
	pod = cache.GetPod(podUID1)
	assert.Check(t, pod == nil, "pod1 still found")
	assert.Equal(t, len(cache.podsMap), 0, "wrong pod count after remove of pod1")

	// again, with assigned node
	pod1.Spec.NodeName = "test-node-remove"
	cache.UpdatePod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1 with node")
	pod = cache.GetPod(podUID1)
	assert.Check(t, pod != nil, "pod1 not found")

	// remove pod1
	cache.RemovePod(pod1)
	pod = cache.GetPod(podUID1)
	assert.Check(t, pod == nil, "pod1 still found")
	assert.Equal(t, len(cache.podsMap), 0, "wrong pod count after remove of pod1 with node")

	// removal of pod added to synthetic node should succeed
	pod1.Spec.NodeName = "unknown-node"
	cache.UpdatePod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1 with synthetic node")
	cache.RemovePod(pod1)
	assert.Equal(t, len(cache.podsMap), 0, "wrong pod count after remove of pod1 with synthetic node")

	// verify removal again doesn't crash
	cache.RemovePod(pod1)
	assert.Equal(t, len(cache.podsMap), 0, "wrong pod count after remove of pod1 with synthetic node")

	// verify removal of pod with unknown node doesn't crash
	pod1.Spec.NodeName = "missing-node"
	cache.RemovePod(pod1)
}

func TestUpdatePriorityClass(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "class001",
			UID:  "Class-UID-00001",
		},
		Value: 10,
	}
	pc2 := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "class001",
			UID:  "Class-UID-00001",
		},
		Value: 20,
	}

	cache.UpdatePriorityClass(pc)
	cache.UpdatePriorityClass(pc2)

	result := cache.GetPriorityClass("class001")
	assert.Assert(t, result != nil)
	assert.Equal(t, result.Value, int32(20))
}

func TestRemovePriorityClass(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "class001",
			UID:  "Class-UID-00001",
		},
		Value: 10,
	}

	cache.UpdatePriorityClass(pc)
	result := cache.GetPriorityClass("class001")
	assert.Assert(t, result != nil)
	assert.Equal(t, result.Value, int32(10))

	cache.RemovePriorityClass(pc)
	result = cache.GetPriorityClass("class001")
	assert.Assert(t, result == nil)
}

func TestGetSchedulerCacheDao(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	// test empty
	dao := cache.GetSchedulerCacheDao()
	assert.Equal(t, len(dao.Nodes), 0)
	assert.Equal(t, len(dao.Pods), 0)
	assert.Equal(t, len(dao.PriorityClasses), 0)
	assert.Equal(t, len(dao.SchedulingPods), 0)
	assert.Equal(t, dao.Statistics.Nodes, 0)
	assert.Equal(t, dao.Statistics.Pods, 0)
	assert.Equal(t, dao.Statistics.PriorityClasses, 0)
	assert.Equal(t, dao.Statistics.Assumed, 0)
	assert.Equal(t, dao.Statistics.PodsAssigned, 0)
	assert.Equal(t, dao.Statistics.InProgressAllocations, 0)
	assert.Equal(t, dao.Statistics.PendingAllocations, 0)

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Namespace: "test",
			Name:      podName1,
			UID:       podUID1,
		},
		Spec: v1.PodSpec{},
	}
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "class001",
			UID:  "Class-UID-00001",
		},
		Value: 10,
	}

	cache.UpdateNode(node)
	cache.UpdatePod(pod)
	cache.UpdatePriorityClass(pc)

	// test with data
	dao = cache.GetSchedulerCacheDao()
	assert.Equal(t, len(dao.Nodes), 1)
	nodeDao, ok := dao.Nodes[host1]
	assert.Assert(t, ok)
	assert.DeepEqual(t, *nodeDao.Allocatable.Memory(), resourceList["memory"])
	assert.DeepEqual(t, *nodeDao.Allocatable.Cpu(), resourceList["cpu"])
	assert.Equal(t, len(dao.Pods), 1)
	podDao, ok := dao.Pods["test/pod0001"]
	assert.Assert(t, ok)
	assert.Equal(t, string(podDao.UID), podUID1)
	assert.Equal(t, len(dao.PriorityClasses), 1)
	pcDao, ok := dao.PriorityClasses["class001"]
	assert.Assert(t, ok)
	assert.Equal(t, pcDao.Value, int32(10))
	assert.Equal(t, len(dao.SchedulingPods), 1)
	psDao, ok := dao.SchedulingPods["test/pod0001"]
	assert.Assert(t, ok)
	assert.Equal(t, string(psDao.UID), podUID1)
	assert.Equal(t, dao.Statistics.Nodes, 1)
	assert.Equal(t, dao.Statistics.Pods, 1)
	assert.Equal(t, dao.Statistics.PriorityClasses, 1)
	assert.Equal(t, dao.Statistics.Assumed, 0)
	assert.Equal(t, dao.Statistics.PodsAssigned, 0)
	assert.Equal(t, dao.Statistics.InProgressAllocations, 0)
	assert.Equal(t, dao.Statistics.PendingAllocations, 0)
}

func expectHost1AndHost2(t *testing.T, nodesInfo []*framework.NodeInfo) {
	assert.Assert(t, nodesInfo != nil, "nodesInfo list was not created")
	assert.Equal(t, 2, len(nodesInfo), "nodesInfo list size")
	m := make(map[string]bool)
	for _, info := range nodesInfo {
		m[info.Node().Name] = true
	}
	assert.Equal(t, true, m[host1], "node not found")
	assert.Equal(t, true, m[host2], "node not found")
}

func expectHost(t *testing.T, host string, nodesInfo []*framework.NodeInfo) {
	assert.Assert(t, nodesInfo != nil, "nodes list was not created or got deleted")
	assert.Equal(t, 1, len(nodesInfo), "nodes list size")
	assert.Equal(t, host, nodesInfo[0].Node().Name)
}

func TestUpdatePVCRefCounts(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	node1 := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}
	node2 := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host2,
			Namespace: "default",
			UID:       nodeUID2,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	cache.UpdateNode(node1)
	cache.UpdateNode(node2)

	podTemplate := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Namespace:   "default",
			Annotations: map[string]string{"state": "new"},
		},
		Spec: v1.PodSpec{},
	}

	pod1 := podTemplate.DeepCopy()
	pod1.ObjectMeta.Name = podName1
	pod1.ObjectMeta.UID = podUID1
	pod1.Spec.NodeName = node1.Name
	pod1.Spec.Volumes = []v1.Volume{
		{
			Name:         pvcName1,
			VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName1}},
		},
	}
	cache.UpdatePod(pod1)
	assert.Check(t, cache.IsPVCUsedByPods(framework.GetNamespacedName(pod1.Namespace, pvcName1)), "pvc1 is not in pvcRefCounts")

	// add a pod without assigned node can't update pvcRefCounts
	pod2 := podTemplate.DeepCopy()
	pod2.ObjectMeta.Name = podName2
	pod2.ObjectMeta.UID = podUID2
	pod2.Spec.Volumes = []v1.Volume{
		{
			Name:         pvcName2,
			VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName2}},
		},
	}
	cache.UpdatePod(pod2)
	assert.Check(t, !cache.IsPVCUsedByPods(framework.GetNamespacedName(pod2.Namespace, pvcName2)), "pvc2 is in pvcRefCounts")

	// assign a node to pod2
	pod2Copy := pod2.DeepCopy()
	pod2Copy.Spec.NodeName = node2.Name
	cache.UpdatePod(pod2Copy)
	assert.Check(t, cache.IsPVCUsedByPods(framework.GetNamespacedName(pod2.Namespace, pvcName2)), "pvc2 is not in pvcRefCounts")

	// remove pod1
	cache.RemovePod(pod1)
	assert.Check(t, !cache.IsPVCUsedByPods(framework.GetNamespacedName(pod1.Namespace, pvcName1)), "pvc1 is in pvcRefCounts")

	// remove node2
	cache.RemoveNode(node2)
	assert.Check(t, !cache.IsPVCUsedByPods(framework.GetNamespacedName(pod2.Namespace, pvcName2)), "pvc2 is in pvcRefCounts")
}

func TestOrphanPods(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList["memory"] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList["cpu"] = *resource.NewQuantity(10, resource.DecimalSI)
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Namespace: "test",
			Name:      podName1,
			UID:       podUID1,
		},
		Spec: v1.PodSpec{
			NodeName: host1,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      host1,
			Namespace: "default",
			UID:       nodeUID1,
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	// missing pod should not be orphaned
	assert.Check(t, !cache.IsPodOrphaned(podUID1), "missing pod showing as orphaned")

	// pod referencing non-existent node should be orphaned
	cache.UpdatePod(pod)
	assert.Check(t, cache.IsPodOrphaned(podUID1), "pod on missing node not marked as orphaned")

	// once node is added, pod should no longer be orphaned
	cache.UpdateNode(node)
	assert.Check(t, !cache.IsPodOrphaned(podUID1), "pod on added node still marked as orphaned")
}
