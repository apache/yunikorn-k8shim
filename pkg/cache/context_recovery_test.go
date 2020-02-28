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
	"k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
)

func TestNodeRecoveringState(t *testing.T) {
	apiProvider4test := client.NewMockedAPIProvider()
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

	assert.Equal(t, sn1.getNodeState(), string(events.States().Node.Recovering))
	assert.Equal(t, sn2.getNodeState(), string(events.States().Node.Recovering))
}

func TestNodesRecovery(t *testing.T) {
	apiProvide4test := client.NewMockedAPIProvider()
	context := NewContext(apiProvide4test)
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
	apiProvide4test.SetNodeLister(nodeLister)

	mockedAppRecover := test.NewMockedRecoverableAppManager()
	if err := context.recover([]interfaces.Recoverable{mockedAppRecover}, 3*time.Second); err == nil {
		t.Fatalf("expecting timeout here!")
	} else {
		t.Logf("context stays waiting for recovery, error: %v", err)
	}

	sn1 := context.nodes.getNode("host0001")
	sn2 := context.nodes.getNode("host0002")

	assert.Assert(t, sn1 != nil)
	assert.Assert(t, sn2 != nil)

	// node1 recovery is done
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeID: "host0001",
		Event:  events.NodeAccepted,
	})

	if err := utils.WaitForCondition(func() bool {
		return sn1.getNodeState() == string(events.States().Node.Healthy) &&
			sn2.getNodeState() == string(events.States().Node.Recovering)
	}, time.Second, 5*time.Second); err != nil {
		t.Fatal("unexpected node states")
	}

	// node2 recovery is done
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeID: "host0002",
		Event:  events.NodeAccepted,
	})

	if err := context.recover([]interfaces.Recoverable{mockedAppRecover}, 3*time.Second); err != nil {
		t.Fatalf("recovery should be successful, however got error %v", err)
	}

	assert.Equal(t, sn1.getNodeState(), string(events.States().Node.Healthy))
	assert.Equal(t, sn2.getNodeState(), string(events.States().Node.Healthy))
}
