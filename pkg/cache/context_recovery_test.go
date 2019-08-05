/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/utils"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestRecoveringState(t *testing.T) {
	mockApi := test.NewSchedulerApiMock()
	mockClient := test.NewKubeClientMock()

	context := NewContextInternal(mockApi, &conf.SchedulerConf{}, mockClient, true)
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
					Value: 1024,
				}, utils.K8sResource{
					ResourceName: v1.ResourceCPU,
					Value: 10,
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
					Value: 1024,
				}, utils.K8sResource{
					ResourceName: v1.ResourceCPU,
					Value: 10,
				}),
		},
	}

	context.nodes.addNode(&node1)
	context.nodes.addNode(&node2)

	sn1 := context.nodes.getNode("host0001")
	sn2 := context.nodes.getNode("host0002")

	assert.Assert(t, sn1 != nil)
	assert.Assert(t, sn2 != nil)

	if err := utils.WaitForCondition(func() bool {
		return sn1.getNodeState() == string(events.States().Node.Recovering) &&
			sn2.getNodeState() == string(events.States().Node.Recovering)
	}, time.Duration(1*time.Second), time.Duration(5*time.Second)); err != nil {
		t.Fatalf("all nodes should under recoverying state")
	}

	nodeLister := test.NewNodeListerMock()
	nodeLister.AddNode(&node1)
	nodeLister.AddNode(&node2)
	if err := context.waitForRecovery(nodeLister, time.Duration(3 * time.Second)); err == nil {
		t.Fatalf("expecting timeout here!")
	} else {
		t.Logf("context stays waiting for recovery, error: %v", err)
	}
}

func TestNodesRecovery(t *testing.T) {
	mockApi := test.NewSchedulerApiMock()
	mockClient := test.NewKubeClientMock()

	context := NewContextInternal(mockApi, &conf.SchedulerConf{}, mockClient, true)
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
					Value: 1024,
				}, utils.K8sResource{
					ResourceName: v1.ResourceCPU,
					Value: 10,
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
					Value: 1024,
				}, utils.K8sResource{
					ResourceName: v1.ResourceCPU,
					Value: 10,
				}),
		},
	}

	context.nodes.addNode(&node1)
	context.nodes.addNode(&node2)

	sn1 := context.nodes.getNode("host0001")
	sn2 := context.nodes.getNode("host0002")

	assert.Assert(t, sn1 != nil)
	assert.Assert(t, sn2 != nil)

	if err := utils.WaitForCondition(func() bool {
		return sn1.getNodeState() == string(events.States().Node.Recovering) &&
			sn2.getNodeState() == string(events.States().Node.Recovering)
	}, time.Duration(1*time.Second), time.Duration(5*time.Second)); err != nil {
		t.Fatalf("all nodes should under recoverying state")
	}

	// node1 recovery is done
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeId:    "host0001",
		Event:     events.NodeAccepted,
	})

	nodeLister := test.NewNodeListerMock()
	nodeLister.AddNode(&node1)
	nodeLister.AddNode(&node2)
	if err := context.waitForRecovery(nodeLister, time.Duration(3 * time.Second)); err == nil {
		t.Fatalf("expecting timeout here!")
	} else {
		t.Logf("context stays waiting for recovery, error: %v", err)
	}

	// node2 recovery is done
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeId:    "host0002",
		Event:     events.NodeAccepted,
	})

	if err := context.waitForRecovery(nodeLister, time.Duration(3 * time.Second)); err != nil {
		t.Fatalf("recovery should be successful, however got error %v", err)
	}
}