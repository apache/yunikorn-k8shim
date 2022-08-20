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
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAddNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	// register fn doesn't nothing than checking input
	api.UpdateNodeFunction(getUpdateNodeFunction(t, "host0001", 1024*1000*1000, 10000, false))

	nodes := newSchedulerNodes(api, NewTestSchedulerCache())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	nodes.addNode(&newNode)

	// values are verified in injected fn
	// verify register is not called, update is called and just called once
	err := utils.WaitForCondition(func() bool {
		return api.GetRegisterCount() == 0
	}, time.Second, 5*time.Second)
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 1
	}, time.Second, 5*time.Second)
	assert.NilError(t, err)
}

func TestUpdateNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	nodes := newSchedulerNodes(api, NewTestSchedulerCache())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var oldNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	// this function validates the new node can be added
	// this verifies the shim sends the si.UpdateRequest to core with the new node info
	api.UpdateNodeFunction(getUpdateNodeFunction(t, "host0001", 1024*1000*1000, 10000, false))

	// add the node first
	nodes.addNode(&oldNode)

	// wait for node being added
	assert.NilError(t, utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 1
	}, time.Second, 5*time.Second))
	assert.Assert(t, nodes.getNode("host0001") != nil)
	assert.Equal(t, nodes.getNode("host0001").name, "host0001")

	// reset all counters to make the verification easier
	api.ResetAllCounters()

	// if node resource stays same, update update should be ignored
	ignoreNodeUpdateFn := func(request *si.NodeRequest) error {
		if request.Nodes != nil && len(request.Nodes) > 0 {
			t.Fatalf("expecting no update nodes sent to scheduler as node resource has no change")
		}

		return nil
	}
	api.UpdateNodeFunction(ignoreNodeUpdateFn)
	nodes.updateNode(&oldNode, &newNode)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateNodeCount(), int32(0))

	// change new node's resource, afterwards the update request should be sent to the scheduler
	newResourceList := make(map[v1.ResourceName]resource.Quantity)
	newResourceList[v1.ResourceName("memory")] = *resource.NewQuantity(2048*1000*1000, resource.DecimalSI)
	newResourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: newResourceList,
		},
	}

	api.UpdateNodeFunction(getUpdateNodeFunction(t, "host0001", 2048*1000*1000, 10000, false))

	nodes.updateNode(&oldNode, &newNode)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateNodeCount(), int32(1))

	condition := v1.NodeCondition{Type: v1.NodeReady, Status: v1.ConditionTrue}
	var conditions []v1.NodeCondition
	conditions = append(conditions, condition)

	newNode1 := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: newResourceList,
			Conditions:  conditions,
		},
	}

	api.UpdateNodeFunction(getUpdateNodeFunction(t, "host0001", 2048*1000*1000, 10000, true))

	nodes.updateNode(&oldNode, &newNode1)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateNodeCount(), int32(2))
}

func TestUpdateWithoutNodeAdded(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	nodes := newSchedulerNodes(api, NewTestSchedulerCache())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var oldNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	api.UpdateNodeFunction(getUpdateNodeFunction(t, "host0001", 1024*1000*1000, 10000, false))

	// directly trigger an update
	// if the node was not seeing in the cache, we should see the node be added
	nodes.updateNode(&oldNode, &newNode)

	// wait for node being added
	assert.NilError(t, utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 1
	}, time.Second, 5*time.Second))
	assert.Assert(t, nodes.getNode("host0001") != nil)
	assert.Equal(t, nodes.getNode("host0001").name, "host0001")
	assert.Equal(t, api.GetUpdateNodeCount(), int32(1))

	// change new node's resource, afterwards the update request should be sent to the scheduler
	newResourceList := make(map[v1.ResourceName]resource.Quantity)
	newResourceList[v1.ResourceName("memory")] = *resource.NewQuantity(2048*1000*1000, resource.DecimalSI)
	newResourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: newResourceList,
		},
	}

	api.UpdateNodeFunction(getUpdateNodeFunction(t, "host0001", 2048*1000*1000, 10000, false))

	nodes.updateNode(&oldNode, &newNode)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateNodeCount(), int32(2))
}

func TestDeleteNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()
	nodes := newSchedulerNodes(api, NewTestSchedulerCache())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var node = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	ignoreNodeUpdateFn := func(request *si.NodeRequest) error {
		// fake update
		return nil
	}
	api.UpdateNodeFunction(ignoreNodeUpdateFn)

	// add node to the cache
	nodes.addNode(&node)
	err := utils.WaitForCondition(func() bool {
		return api.GetRegisterCount() == 0
	}, 1*time.Second, 5*time.Second)
	assert.NilError(t, err)
	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 1
	}, 100*time.Millisecond, 1000*time.Millisecond)
	assert.NilError(t, err)

	// delete node should trigger another update
	nodes.deleteNode(&node)
	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 2
	}, 100*time.Millisecond, 1000*time.Millisecond)
	assert.NilError(t, err)

	// ensure the node is removed from cache
	assert.Assert(t, nodes.getNode("host0001") == nil)

	// add the node back, hostName is same but UID is different
	var nodeNew = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_002",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}
	nodes.addNode(&nodeNew)
	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 3
	}, 100*time.Millisecond, 1000*time.Millisecond)
	assert.NilError(t, err)

	assert.Assert(t, nodes.getNode("host0001") != nil)
	assert.Equal(t, nodes.getNode("host0001").name, "host0001")
	assert.Equal(t, nodes.getNode("host0001").uid, "uid_002")

	// remove the node again, and then try update
	nodes.deleteNode(&nodeNew)
	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 4
	}, 100*time.Millisecond, 1000*time.Millisecond)
	assert.NilError(t, err)

	// instead of a add, do a update
	// this could happen when a node is removed and added back,
	// or a new node is created with the same hostname
	var nodeNew2 = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_003",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}

	// update the node, this will trigger a update to add the node
	nodes.updateNode(&nodeNew, &nodeNew2)
	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateNodeCount() == 5
	}, 100*time.Millisecond, 1000*time.Millisecond)
	assert.NilError(t, err)

	assert.Assert(t, nodes.getNode("host0001") != nil)
	assert.Equal(t, nodes.getNode("host0001").name, "host0001")
	assert.Equal(t, nodes.getNode("host0001").uid, "uid_003")
}

// A wrapper around the scheduler cache which does not initialise the lister and volumebinder
func NewTestSchedulerCache() *external.SchedulerCache {
	return external.NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
}

func TestCordonNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	// register fn doesn't nothing than checking input
	inputCheckerUpdateFn := func(request *si.NodeRequest) error {
		if request.Nodes == nil {
			t.Fatalf("updated nodes should not be nil")
		}

		if len(request.Nodes) != 1 {
			t.Fatalf("expecting 1 updated node")
		}

		if request.Nodes[0].Action != si.NodeInfo_DRAIN_NODE {
			t.Fatalf("expecting NodeInfo_DRAIN_NODE but get %s",
				request.Nodes[0].Action.String())
		}
		return nil
	}

	nodes := newSchedulerNodes(api, NewTestSchedulerCache())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var oldNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: true,
		},
	}

	api.UpdateNodeFunction(inputCheckerUpdateFn)
	nodes.addAndReportNode(&oldNode, false)
	nodes.getNode("host0001").fsm.SetState(SchedulerNodeStates().Healthy)
	api.UpdateNodeFunction(inputCheckerUpdateFn)
	nodes.updateNode(&oldNode, &newNode)

	// wait until node reaches Draining state
	err := utils.WaitForCondition(func() bool {
		return nodes.getNode("host0001").getNodeState() == SchedulerNodeStates().Draining
	}, 1*time.Second, 5*time.Second)
	assert.NilError(t, err)

	// restore the node
	var newNode2 = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
	}

	// register fn doesn't nothing than checking input
	inputCheckerUpdateFn2 := func(request *si.NodeRequest) error {
		if request.Nodes == nil {
			t.Fatalf("updated nodes should not be nil")
		}

		if len(request.Nodes) != 1 {
			t.Fatalf("expecting 1 updated node")
		}

		if request.Nodes[0].Action != si.NodeInfo_DRAIN_TO_SCHEDULABLE {
			t.Fatalf("expecting UpdateNodeInfo_DRAIN_NODE but get %s",
				request.Nodes[0].Action.String())
		}
		return nil
	}

	api.UpdateNodeFunction(inputCheckerUpdateFn2)
	nodes.updateNode(&newNode, &newNode2)

	// wait until node reaches Draining state
	err = utils.WaitForCondition(func() bool {
		return nodes.getNode("host0001").getNodeState() == SchedulerNodeStates().Healthy
	}, 1*time.Second, 5*time.Second)
	assert.NilError(t, err)
}

func getUpdateNodeFunction(t *testing.T, expectedNodeID string, expectedMem int32,
	expectedCores int32, expectedReady bool) func(request *si.NodeRequest) error {
	updateFn := func(request *si.NodeRequest) error {
		if request.Nodes == nil || len(request.Nodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.Nodes[0]
		if info.NodeID != expectedNodeID {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[siCommon.Memory].Value; memory != int64(expectedMem) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[siCommon.CPU].Value; cpu != int64(expectedCores) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		if ready := info.Attributes[siCommon.NodeReadyAttribute]; ready != strconv.FormatBool(expectedReady) {
			t.Fatalf("unexpected node ready flag %s", ready)
		}
		return nil
	}
	return updateFn
}
