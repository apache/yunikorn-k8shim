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

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestUpdatePod(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	nodes := newSchedulerNodes(mockedSchedulerApi, NewTestSchedulerCache())
	host1 := utils.NodeForTest("HOST1", "10G", "10")
	host2 := utils.NodeForTest("HOST2", "10G", "10")
	nodes.addNode(host1)
	nodes.addNode(host2)
	coordinator := newNodeResourceCoordinator(nodes)

	// pod state not changed, verify this won't trigger the update
	pod1 := utils.PodForTest("pod1", "1G", "500m")
	pod2 := utils.PodForTest("pod1", "1G", "500m")
	pod1.Status.Phase = v1.PodRunning
	pod1.Status.Phase = v1.PodRunning
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		t.Fatalf("update should not run because state is not changed")
		return nil
	}
	coordinator.updatePod(pod1, pod2)

	// pod state changed from pending to running,
	// trigger an update
	pod1.Status.Phase = v1.PodPending
	pod2.Status.Phase = v1.PodRunning
	pod1.Spec.NodeName = "HOST1"
	pod2.Spec.NodeName = "HOST1"
	executed := false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST1")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(1000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(500))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// pod state changed from pending to failed
	// trigger another update
	pod1.Status.Phase = v1.PodRunning
	pod2.Status.Phase = v1.PodFailed
	pod1.Spec.NodeName = "HOST1"
	pod2.Spec.NodeName = "HOST1"
	executed = false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST1")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(0))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(0))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// pod state changed from pending to succeed
	// trigger another update
	// pending to running to succeed
	pod1.Status.Phase = v1.PodPending
	pod2.Status.Phase = v1.PodRunning
	pod1.Spec.NodeName = "HOST2"
	pod2.Spec.NodeName = "HOST2"
	executed = false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST2")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(1000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(500))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	pod1.Status.Phase = v1.PodRunning
	pod2.Status.Phase = v1.PodSucceeded
	pod1.Spec.NodeName = "HOST2"
	pod2.Spec.NodeName = "HOST2"
	executed = false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST2")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(0))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(0))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)
}

func TestDeletePod(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	nodes := newSchedulerNodes(mockedSchedulerApi, NewTestSchedulerCache())
	host1 := utils.NodeForTest("HOST1", "10G", "10")
	nodes.addNode(host1)
	coordinator := newNodeResourceCoordinator(nodes)

	// pod from pending to running
	// occupied resources should be added to the node
	pod1 := utils.PodForTest("pod1", "1G", "500m")
	pod2 := utils.PodForTest("pod1", "1G", "500m")
	pod1.Status.Phase = v1.PodPending
	pod2.Status.Phase = v1.PodRunning
	pod1.Spec.NodeName = "HOST1"
	pod2.Spec.NodeName = "HOST1"
	executed := false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST1")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(1000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(500))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// delete pod from the running state
	executed = false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST1")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(0))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(0))
		return nil
	}
	coordinator.deletePod(pod1)
}

func TestDeleteTerminatedPod(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	nodes := newSchedulerNodes(mockedSchedulerApi, NewTestSchedulerCache())
	host1 := utils.NodeForTest("HOST1", "10G", "10")
	nodes.addNode(host1)
	coordinator := newNodeResourceCoordinator(nodes)

	// pod from pending to running
	// occupied resources should be added to the node
	pod1 := utils.PodForTest("pod1", "1G", "500m")
	pod2 := utils.PodForTest("pod1", "1G", "500m")
	pod1.Status.Phase = v1.PodPending
	pod2.Status.Phase = v1.PodRunning
	pod1.Spec.NodeName = "HOST1"
	pod2.Spec.NodeName = "HOST1"
	executed := false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST1")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(1000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(500))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// pod from pending to running
	// occupied resources should be added to the node
	pod1.Status.Phase = v1.PodRunning
	pod2.Status.Phase = v1.PodSucceeded
	pod1.Spec.NodeName = "HOST1"
	pod2.Spec.NodeName = "HOST1"
	executed = false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		assert.Equal(t, len(request.UpdatedNodes), 1)
		updatedNode := request.UpdatedNodes[0]
		assert.Equal(t, updatedNode.NodeID, "HOST1")
		assert.Equal(t, updatedNode.Action, si.UpdateNodeInfo_UPDATE)
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.Memory].Value, int64(10000))
		assert.Equal(t, updatedNode.SchedulableResource.Resources[constants.CPU].Value, int64(10000))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.Memory].Value, int64(0))
		assert.Equal(t, updatedNode.OccupiedResource.Resources[constants.CPU].Value, int64(0))
		return nil
	}
	coordinator.updatePod(pod1, pod2)
	assert.Assert(t, executed)

	// delete pod from the succeed state
	executed = false
	mockedSchedulerApi.updateFn = func(request *si.UpdateRequest) error {
		executed = true
		t.Fatalf("update should not be triggered as it should have already done")
		return nil
	}
	coordinator.deletePod(pod2)
	assert.Equal(t, executed, false)
}
