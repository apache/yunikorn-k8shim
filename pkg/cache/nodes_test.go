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
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestAddNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	// register fn doesn't nothing than checking input
	inputCheckerUpdateFn := func(request *si.UpdateRequest) error {
		if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.NewSchedulableNodes[0]
		if info.NodeID != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(1024) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	}

	api.UpdateFunction(inputCheckerUpdateFn)

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
		return api.GetUpdateCount() == 1
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
	api.UpdateFunction(func(request *si.UpdateRequest) error {
		if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.NewSchedulableNodes[0]
		if info.NodeID != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(1024) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	})

	// add the node first
	nodes.addNode(&oldNode)

	// wait for node being added
	assert.NilError(t, utils.WaitForCondition(func() bool {
		return api.GetUpdateCount() == 1
	}, time.Second, 5*time.Second))
	assert.Assert(t, nodes.getNode("host0001") != nil)
	assert.Equal(t, nodes.getNode("host0001").name, "host0001")

	// reset all counters to make the verification easier
	api.ResetAllCounters()

	// if node resource stays same, update update should be ignored
	ignoreNodeUpdateFn := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes != nil && len(request.UpdatedNodes) > 0 {
			t.Fatalf("expecting no update nodes sent to scheduler as node resource has no change")
		}

		return nil
	}
	api.UpdateFunction(ignoreNodeUpdateFn)
	nodes.updateNode(&oldNode, &newNode)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateCount(), int32(0))

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

	checkFn := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes == nil || len(request.UpdatedNodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.UpdatedNodes[0]
		if info.NodeID != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(2048) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	}

	api.UpdateFunction(checkFn)

	nodes.updateNode(&oldNode, &newNode)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateCount(), int32(1))
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

	//
	api.UpdateFunction(func(request *si.UpdateRequest) error {
		if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.NewSchedulableNodes[0]
		if info.NodeID != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(1024) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	})

	// directly trigger an update
	// if the node was not seeing in the cache, we should see the node be added
	nodes.updateNode(&oldNode, &newNode)

	// wait for node being added
	assert.NilError(t, utils.WaitForCondition(func() bool {
		return api.GetUpdateCount() == 1
	}, time.Second, 5*time.Second))
	assert.Assert(t, nodes.getNode("host0001") != nil)
	assert.Equal(t, nodes.getNode("host0001").name, "host0001")
	assert.Equal(t, api.GetUpdateCount(), int32(1))

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

	checkFn := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes == nil || len(request.UpdatedNodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.UpdatedNodes[0]
		if info.NodeID != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(2048) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	}

	api.UpdateFunction(checkFn)

	nodes.updateNode(&oldNode, &newNode)
	assert.Equal(t, api.GetRegisterCount(), int32(0))
	assert.Equal(t, api.GetUpdateCount(), int32(2))
}

func TestDeleteNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	// register fn doesn't nothing than checking input
	inputCheckerFn := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes == nil || len(request.UpdatedNodes) != 1 {
			t.Fatalf("unexpected updated nodes info from the request")
		}

		info := request.UpdatedNodes[0]
		if info.NodeID != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeID)
		}

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(1024) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	}

	api.UpdateFunction(inputCheckerFn)

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

	ignoreNodeUpdateFn := func(request *si.UpdateRequest) error {
		// fake update
		return nil
	}
	api.UpdateFunction(ignoreNodeUpdateFn)
	nodes.addNode(&node)
	nodes.deleteNode(&node)

	err := utils.WaitForCondition(func() bool {
		return api.GetRegisterCount() == 0
	}, 1*time.Second, 5*time.Second)
	assert.NilError(t, err)

	// update should be called twice
	// one for add, the other one for delete
	err = utils.WaitForCondition(func() bool {
		return api.GetUpdateCount() == 2
	}, 1*time.Second, 5*time.Second)
	assert.NilError(t, err)
}

// A wrapper around the scheduler cache which does not initialise the lister and volumebinder
func NewTestSchedulerCache() *external.SchedulerCache {
	return external.NewSchedulerCache(client.NewMockedAPIProvider().GetAPIs())
}

func TestCordonNode(t *testing.T) {
	api := test.NewSchedulerAPIMock()

	// register fn doesn't nothing than checking input
	inputCheckerUpdateFn := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes == nil {
			t.Fatalf("updated nodes should not be nil")
		}

		if len(request.UpdatedNodes) != 1 {
			t.Fatalf("expecting 1 updated node")
		}

		if request.UpdatedNodes[0].Action != si.UpdateNodeInfo_DRAIN_NODE {
			t.Fatalf("expecting UpdateNodeInfo_DRAIN_NODE but get %s",
				request.UpdatedNodes[0].Action.String())
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

	api.UpdateFunction(inputCheckerUpdateFn)
	nodes.addAndReportNode(&oldNode, false)
	nodes.getNode("host0001").fsm.SetState(events.States().Node.Healthy)
	api.UpdateFunction(inputCheckerUpdateFn)
	nodes.updateNode(&oldNode, &newNode)

	// wait until node reaches Draining state
	err := utils.WaitForCondition(func() bool {
		return nodes.getNode("host0001").getNodeState() == events.States().Node.Draining
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
	inputCheckerUpdateFn2 := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes == nil {
			t.Fatalf("updated nodes should not be nil")
		}

		if len(request.UpdatedNodes) != 1 {
			t.Fatalf("expecting 1 updated node")
		}

		if request.UpdatedNodes[0].Action != si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE {
			t.Fatalf("expecting UpdateNodeInfo_DRAIN_NODE but get %s",
				request.UpdatedNodes[0].Action.String())
		}
		return nil
	}

	api.UpdateFunction(inputCheckerUpdateFn2)
	nodes.updateNode(&newNode, &newNode2)

	// wait until node reaches Draining state
	err = utils.WaitForCondition(func() bool {
		return nodes.getNode("host0001").getNodeState() == events.States().Node.Healthy
	}, 1*time.Second, 5*time.Second)
	assert.NilError(t, err)
}
