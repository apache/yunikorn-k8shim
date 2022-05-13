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
	"reflect"
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
)

func TestNodeRecoveringState(t *testing.T) {
	apiProvider4test := client.NewMockedAPIProvider(false)
	context := NewContext(apiProvider4test)
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, context.nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	var node1 = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: utils.NewK8sResourceList(
				utils.K8sResource{
					ResourceName: v1.ResourceMemory,
					Value:        1024,
				}, utils.K8sResource{
					ResourceName: v1.ResourceCPU,
					Value:        10,
				}),
		},
	}

	var node2 = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0002",
			Namespace: "default",
			UID:       "uid_0002",
		},
		Status: v1.NodeStatus{
			Capacity: utils.NewK8sResourceList(
				utils.K8sResource{
					ResourceName: v1.ResourceMemory,
					Value:        1024,
				}, utils.K8sResource{
					ResourceName: v1.ResourceCPU,
					Value:        10,
				}),
		},
	}

	nodeLister := test.NewNodeListerMock()
	nodeLister.AddNode(&node1)
	nodeLister.AddNode(&node2)
	apiProvider4test.SetNodeLister(nodeLister)

	mockedAppMgr := test.NewMockedRecoverableAppManager()
	if err := context.recover([]interfaces.Recoverable{mockedAppMgr}, 3*time.Second); err == nil {
		t.Fatalf("expecting timeout here!")
	} else {
		t.Logf("context stays waiting for recovery, error: %v", err)
	}

	sn1 := context.nodes.getNode("host0001")
	sn2 := context.nodes.getNode("host0002")

	assert.Assert(t, sn1 != nil)
	assert.Assert(t, sn2 != nil)

	assert.Equal(t, sn1.getNodeState(), SchedulerNodeStates().Recovering)
	assert.Equal(t, sn2.getNodeState(), SchedulerNodeStates().Recovering)
}

func TestNodesRecovery(t *testing.T) {
	apiProvide4test := client.NewMockedAPIProvider(false)
	context := NewContext(apiProvide4test)
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, context.nodes.schedulerNodeEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	numNodes := 3
	nodes := make([]*v1.Node, numNodes)
	expectedStates := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = &v1.Node{
			ObjectMeta: apis.ObjectMeta{
				Name:      "host000%d" + strconv.Itoa(i),
				Namespace: "default",
				UID:       types.UID("uid_000" + strconv.Itoa(i)),
			},
			Status: v1.NodeStatus{
				Capacity: utils.NewK8sResourceList(
					utils.K8sResource{
						ResourceName: v1.ResourceMemory,
						Value:        1024,
					}, utils.K8sResource{
						ResourceName: v1.ResourceCPU,
						Value:        10,
					}),
			},
		}
		expectedStates[i] = SchedulerNodeStates().Recovering
	}

	nodeLister := test.NewNodeListerMock()
	for _, node := range nodes {
		nodeLister.AddNode(node)
	}
	apiProvide4test.SetNodeLister(nodeLister)

	mockedAppRecover := test.NewMockedRecoverableAppManager()
	if err := context.recover([]interfaces.Recoverable{mockedAppRecover}, 1*time.Second); err == nil {
		t.Fatalf("expecting timeout here!")
	}

	// verify all nodes were added into context
	schedulerNodes := make([]*SchedulerNode, len(nodes))
	for i, node := range nodes {
		schedulerNodes[i] = context.nodes.getNode(node.Name)
		assert.Assert(t, schedulerNodes[i] != nil)
	}

	// dispatch NodeAccepted event for the first node, its expected state should be Healthy
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeID: schedulerNodes[0].name,
		Event:  NodeAccepted,
	})
	expectedStates[0] = SchedulerNodeStates().Healthy
	err := utils.WaitForCondition(func() bool {
		return reflect.DeepEqual(getNodeStates(schedulerNodes), expectedStates)
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "unexpected node states, actual: %v, expected: %v", getNodeStates(schedulerNodes), expectedStates)

	// dispatch NodeRejected event for the second node, its expected state should be Rejected
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeID: schedulerNodes[1].name,
		Event:  NodeRejected,
	})
	expectedStates[1] = SchedulerNodeStates().Rejected
	err = utils.WaitForCondition(func() bool {
		return reflect.DeepEqual(getNodeStates(schedulerNodes), expectedStates)
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "unexpected node states, actual: %v, expected: %v", getNodeStates(schedulerNodes), expectedStates)

	// dispatch DrainNode event for the third node, its expected state should be Draining
	schedulerNodes[2].schedulable = false
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeID: schedulerNodes[2].name,
		Event:  NodeAccepted,
	})
	expectedStates[2] = SchedulerNodeStates().Draining
	err = context.recover([]interfaces.Recoverable{mockedAppRecover}, 3*time.Second)
	assert.NilError(t, err, "recovery should be successful, however got error")
	assert.DeepEqual(t, getNodeStates(schedulerNodes), expectedStates)
}

func getNodeStates(schedulerNodes []*SchedulerNode) []string {
	nodeStates := make([]string, len(schedulerNodes))
	for i, sn := range schedulerNodes {
		nodeStates[i] = sn.getNodeState()
	}
	return nodeStates
}
