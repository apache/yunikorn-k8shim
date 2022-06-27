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

	"gotest.tools/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
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
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
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
			Name: "pod0001",
			UID:  "Pod-UID-00001",
		},
		Spec: v1.PodSpec{
			NodeName: "host0001",
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
			// the cached node has no pods assigned
			cachedNode := cache.GetNode(node.Name)
			assert.Check(t, cachedNode != nil, "host0001 is not found in cache")
			assert.Check(t, cachedNode.Node() != nil, "host0001 exists in cache but the ref to v1.Node doesn't exist")
			assert.Equal(t, cachedNode.Node().Name, node.Name)
			assert.Equal(t, cachedNode.Node().UID, node.UID)
			// nolint:staticcheck
			assert.Equal(t, len(cachedNode.Pods), 1)

			// verify the pod is added to cache
			// the pod should be added to the node as well
			cachedPod, exist := cache.GetPod("Pod-UID-00001")
			assert.Equal(t, exist, true)
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
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
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
			Name: "pod0001",
			UID:  "Pod-UID-00001",
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
			cachedPod, exist := cache.GetPod("Pod-UID-00001")
			assert.Equal(t, exist, true)
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
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
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
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	// first add the old node
	cache.AddNode(oldNode)

	// make sure the node is added to the cache
	nodeInCache := cache.GetNode("host0001")
	assert.Assert(t, nodeInCache.Node() != nil)
	assert.Equal(t, nodeInCache.Node().Name, "host0001")
	assert.Equal(t, nodeInCache.Node().Spec.Unschedulable, true)

	// then update the node
	cache.UpdateNode(newNode)

	// make sure the node in cache also gets updated
	// unschedulable -> schedulable
	assert.Assert(t, nodeInCache.Node() != nil)
	assert.Equal(t, nodeInCache.Node().Name, "host0001")
	assert.Equal(t, nodeInCache.Node().Spec.Unschedulable, false)
}

func TestUpdateNonExistNode(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	// old node, state: schedulable
	newNode := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	cache.UpdateNode(newNode)

	nodeInCache := cache.GetNode("host0001")
	assert.Assert(t, nodeInCache.Node() != nil)
	assert.Equal(t, nodeInCache.Node().Name, "host0001")
	assert.Equal(t, nodeInCache.Node().Spec.Unschedulable, false)
}

func add2Cache(cache *SchedulerCache, objects ...interface{}) error {
	for _, obj := range objects {
		switch podOrNode := obj.(type) {
		case *v1.Node:
			cache.AddNode(podOrNode)
		case *v1.Pod:
			cache.AddPod(podOrNode)
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
		cache.AddNode(&v1.Node{
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

func TestAddPod(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod0001",
			UID:  "Pod-UID-00001",
		},
		Spec: v1.PodSpec{},
	}

	// add
	cache.AddPod(pod1)

	_, ok := cache.GetPod("Pod-UID-00001")
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1")
	assert.Check(t, ok, "pod1 not found")

	// re-add
	cache.AddPod(pod1)
	_, ok = cache.GetPod("Pod-UID-00001")
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after re-add of pod1")
	assert.Check(t, ok, "pod1 not found")

	// assumed pod should no longer be assumed if node changes
	pod1Copy := pod1.DeepCopy()
	pod1Copy.Spec.NodeName = "test-node-add"
	cache.AssumePod(pod1Copy, true)

	assert.Check(t, cache.isAssumedPod("Pod-UID-00001"), "pod is not assumed")
	cache.AddPod(pod1)
	assert.Check(t, cache.isAssumedPod("Pod-UID-00001"), "pod is not assumed after re-add")
}

func TestUpdatePod(t *testing.T) {
	cache := NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	node1 := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
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
			Name:      "host0002",
			Namespace: "default",
			UID:       "Node-UID-00002",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	cache.AddNode(node1)
	cache.AddNode(node2)

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
	pod1.ObjectMeta.Name = "pod0001"
	pod1.ObjectMeta.UID = "Pod-UID-00001"
	cache.AddPod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1")
	_, ok := cache.GetPod("Pod-UID-00001")
	assert.Check(t, ok, "pod1 not found")

	// update of non-existent pod should be equivalent to an add
	pod2 := podTemplate.DeepCopy()
	pod2.ObjectMeta.Name = "pod0002"
	pod2.ObjectMeta.UID = "Pod-UID-00002"
	cache.UpdatePod(pod2)
	assert.Equal(t, len(cache.podsMap), 2, "wrong pod count after add of pod2")
	_, ok = cache.GetPod("Pod-UID-00002")
	assert.Check(t, ok, "pod2 not found")

	// normal pod update should succeed
	pod1Copy := pod1.DeepCopy()
	pod1Copy.ObjectMeta.Annotations["state"] = "updated"
	cache.UpdatePod(pod1Copy)
	found, ok := cache.GetPod("Pod-UID-00001")
	assert.Check(t, ok, "pod1 not found")
	assert.Equal(t, found.GetAnnotations()["state"], "updated", "wrong state after update")
	cache.RemovePod(pod1Copy)

	// assumed pod should still be assumed if node changes
	pod1.Spec.NodeName = node1.Name
	cache.AddPod(pod1)
	cache.AssumePod(pod1, true)
	assert.Check(t, cache.isAssumedPod("Pod-UID-00001"), "pod is not assumed")
	pod1Copy = pod1.DeepCopy()
	pod1Copy.Spec.NodeName = node2.Name
	cache.UpdatePod(pod1Copy)
	assert.Check(t, cache.isAssumedPod("Pod-UID-00001"), "pod is not assumed after re-add")

	// unassumed pod should survive node changing without crashing
	pod3 := podTemplate.DeepCopy()
	pod3.ObjectMeta.Name = "pod00003"
	pod3.ObjectMeta.UID = "Pod-UID-00003"
	pod3.Spec.NodeName = "orig-node"
	cache.AddPod(pod3)
	pod3Copy := pod3.DeepCopy()
	pod3Copy.Spec.NodeName = "new-node"
	cache.UpdatePod(pod3Copy)
	pod3Result, ok := cache.GetPod("Pod-UID-00003")
	assert.Check(t, ok, "unable to get pod3")
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
			Name:        "pod0001",
			UID:         "Pod-UID-00001",
			Annotations: map[string]string{"state": "new"},
		},
		Spec: v1.PodSpec{},
	}

	// add pod1
	cache.AddPod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1")
	_, ok := cache.GetPod("Pod-UID-00001")
	assert.Check(t, ok, "pod1 not found")

	// remove pod1
	cache.RemovePod(pod1)
	_, ok = cache.GetPod("Pod-UID-00001")
	assert.Check(t, !ok, "pod1 still found")
	assert.Equal(t, len(cache.podsMap), 0, "wrong pod count after remove of pod1")

	// again, with assigned node
	pod1.Spec.NodeName = "test-node-remove"
	cache.AddPod(pod1)
	assert.Equal(t, len(cache.podsMap), 1, "wrong pod count after add of pod1 with node")
	_, ok = cache.GetPod("Pod-UID-00001")
	assert.Check(t, ok, "pod1 not found")

	// remove pod1
	cache.RemovePod(pod1)
	_, ok = cache.GetPod("Pod-UID-00001")
	assert.Check(t, !ok, "pod1 still found")
	assert.Equal(t, len(cache.podsMap), 0, "wrong pod count after remove of pod1 with node")

	// removal of pod added to synthetic node should succeed
	pod1.Spec.NodeName = "unknown-node"
	cache.AddPod(pod1)
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
