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

package cache

import (
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	Host1     = "HOST1"
	Host2     = "HOST2"
	HostEmpty = ""
)

func TestNodeCoordinatorLifecycle(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	nodes := newSchedulerNodes(mockedSchedulerApi, NewTestSchedulerCache())
	host1 := utils.NodeForTest(Host1, "10G", "10")
	host2 := utils.NodeForTest(Host2, "10G", "10")
	nodes.addNode(host1)
	nodes.addNode(host2)
	coordinator := newNodeResourceCoordinator(nodes)
	pod1 := utils.PodForTest("pod1", "1G", "500m")
	pod2 := utils.PodForTest("pod1", "1G", "500m")

	// add the first pod with no host
	pod1.Spec.NodeName = HostEmpty
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		t.Fatalf("update should not run because host is empty")
		return nil
	}
	coordinator.addPod(pod1)

	// pod is not assigned to any node
	// this won't trigger an update
	pod2.Spec.NodeName = HostEmpty
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		t.Fatalf("update should not run because host is empty")
		return nil
	}
	coordinator.updatePod(pod1, pod2)

	// update host
	pod1.Spec.NodeName = HostEmpty
	pod2.Spec.NodeName = Host1
	executed := false
	mockedSchedulerApi.UpdateNodeFn = mockAddPodToNode(t, &executed, Host1)
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// verify that updating when host has not changed shouldn't trigger an update
	pod2.Spec.NodeName = Host1
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		t.Fatalf("update should not run because host is not changed")
		return nil
	}
	coordinator.updatePod(pod1, pod2)

	// pod terminated
	pod2.Status.Phase = v1.PodFailed
	pod1.Spec.NodeName = Host1
	pod2.Spec.NodeName = Host1
	executed = false
	mockedSchedulerApi.UpdateNodeFn = mockRemovePodFromNode(t, &executed, Host1)
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// re-add pod
	pod2.Status.Phase = v1.PodRunning
	pod1.Spec.NodeName = Host1
	pod2.Spec.NodeName = Host1
	executed = false
	mockedSchedulerApi.UpdateNodeFn = mockAddPodToNode(t, &executed, Host1)
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// pod moved to another host (should not happen, but best to cover all cases as Pod Spec is mutable)
	pod1.Spec.NodeName = Host1
	pod2.Spec.NodeName = Host2
	executed = false
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		firstExecuted := false
		err := mockRemovePodFromNode(t, &firstExecuted, Host1)(request)
		mockedSchedulerApi.UpdateNodeFn = mockAddPodToNode(t, &executed, Host2)
		return err
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// delete running pod
	executed = false
	mockedSchedulerApi.UpdateNodeFn = mockRemovePodFromNode(t, &executed, Host2)
	coordinator.deletePod(pod2)
	assert.Assert(t, executed)
}

func mockAddPodToNode(t *testing.T, executed *bool, nodeID string) func(request *si.NodeRequest) error {
	return func(request *si.NodeRequest) error {
		*executed = true
		assert.Equal(t, len(request.Nodes), 1)
		updatedNode := request.Nodes[0]
		assert.Equal(t, updatedNode.NodeID, nodeID)
		assert.Equal(t, updatedNode.Action, si.NodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.Memory].Value, int64(10000*1000*1000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.Memory].Value, int64(1000*1000*1000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.CPU].Value, int64(500))
		return nil
	}
}

func mockRemovePodFromNode(t *testing.T, executed *bool, nodeID string) func(request *si.NodeRequest) error {
	return func(request *si.NodeRequest) error {
		*executed = true
		assert.Equal(t, len(request.Nodes), 1)
		updatedNode := request.Nodes[0]
		assert.Equal(t, updatedNode.NodeID, nodeID)
		assert.Equal(t, updatedNode.Action, si.NodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.Memory].Value, int64(10000*1000*1000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.Memory].Value, int64(0))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.CPU].Value, int64(0))
		return nil
	}
}

func TestNodeCoordinatorDeleteTerminatedPod(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	nodes := newSchedulerNodes(mockedSchedulerApi, NewTestSchedulerCache())
	host1 := utils.NodeForTest(Host1, "10G", "10")
	nodes.addNode(host1)
	coordinator := newNodeResourceCoordinator(nodes)

	// pod assigned
	// occupied resources should be added to the node
	pod1 := utils.PodForTest("pod1", "1G", "500m")
	pod2 := utils.PodForTest("pod1", "1G", "500m")
	pod1.Spec.NodeName = HostEmpty
	pod2.Spec.NodeName = Host1
	executed := false
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		executed = true
		assert.Equal(t, len(request.Nodes), 1)
		updatedNode := request.Nodes[0]
		assert.Equal(t, updatedNode.NodeID, Host1)
		assert.Equal(t, updatedNode.Action, si.NodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.Memory].Value, int64(10000*1000*1000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.Memory].Value, int64(1000*1000*1000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.CPU].Value, int64(500))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// pod from running to succeed
	// occupied resources should be added to the node
	pod2.Status.Phase = v1.PodSucceeded
	pod1.Spec.NodeName = Host1
	pod2.Spec.NodeName = Host1
	executed = false
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		executed = true
		assert.Equal(t, len(request.Nodes), 1)
		updatedNode := request.Nodes[0]
		assert.Equal(t, updatedNode.NodeID, Host1)
		assert.Equal(t, updatedNode.Action, si.NodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.Memory].Value, int64(10000*1000*1000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[siCommon.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.Memory].Value, int64(0))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[siCommon.CPU].Value, int64(0))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// delete pod from the succeed state
	executed = false
	mockedSchedulerApi.UpdateNodeFn = func(request *si.NodeRequest) error {
		executed = true
		t.Fatalf("update should not be triggered as it should have already done")
		return nil
	}
	coordinator.deletePod(pod2)
	assert.Equal(t, executed, false)
}

func TestNodeCoordinatorFilterPods(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	nodes := newSchedulerNodes(mockedSchedulerAPI, NewTestSchedulerCache())
	host1 := utils.NodeForTest(Host1, "10G", "10")
	nodes.addNode(host1)
	coordinator := newNodeResourceCoordinator(nodes)

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{SchedulerName: "default-scheduler"},
	}
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:   "yunikorn-test-00003",
			UID:    "UID-00003",
			Labels: map[string]string{"applicationId": "test-00003"},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	assert.Check(t, !coordinator.filterPods(nil), "nil object was allowed")
	assert.Check(t, coordinator.filterPods(pod1), "yunikorn-managed pod with no app id was filtered")
	assert.Check(t, coordinator.filterPods(pod2), "non-yunikorn-managed pod was filtered")
	assert.Check(t, !coordinator.filterPods(pod3), "yunikorn-managed pod was allowed")
}
