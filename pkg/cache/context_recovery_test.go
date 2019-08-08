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

func TestNodeRecoveringState(t *testing.T) {
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

	nodeLister := test.NewNodeListerMock()
	nodeLister.AddNode(&node1)
	nodeLister.AddNode(&node2)
	if err := context.waitForNodeRecovery(nodeLister, 3*time.Second); err == nil {
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

	nodeLister := test.NewNodeListerMock()
	nodeLister.AddNode(&node1)
	nodeLister.AddNode(&node2)
	if err := context.waitForNodeRecovery(nodeLister, 3*time.Second); err == nil {
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
		NodeId:    "host0001",
		Event:     events.NodeAccepted,
	})

	if err := utils.WaitForCondition(func() bool {
		return sn1.getNodeState() == string(events.States().Node.Healthy) &&
			sn2.getNodeState() == string(events.States().Node.Recovering)
	}, time.Second, 5*time.Second); err != nil {
		t.Fatal("unexpected node states")
	}

	// node2 recovery is done
	dispatcher.Dispatch(CachedSchedulerNodeEvent{
		NodeId:    "host0002",
		Event:     events.NodeAccepted,
	})

	if err := context.waitForNodeRecovery(nodeLister, 3*time.Second); err != nil {
		t.Fatalf("recovery should be successful, however got error %v", err)
	}

	assert.Equal(t, sn1.getNodeState(), string(events.States().Node.Healthy))
	assert.Equal(t, sn2.getNodeState(), string(events.States().Node.Healthy))
}

func TestAppRecovery(t *testing.T) {
	mockApi := test.NewSchedulerApiMock()
	mockClient := test.NewKubeClientMock()
	conf.GetSchedulerConf().SchedulerName = fakeClusterSchedulerName

	context := NewContextInternal(mockApi, &conf.SchedulerConf{}, mockClient, true)
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// app1 -> pod1
	// assigned on node1
	pod1 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": "app1",
				"queue":         "root.a",
			},
		},
		Spec: v1.PodSpec{
			NodeName:      "node01",
			SchedulerName: fakeClusterSchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	// app2 -> pod2
	// assigned on node2
	pod2 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod2",
			Namespace: "default",
			UID:       "UID-POD-00002",
			Labels: map[string]string{
				"applicationId": "app2",
				"queue":         "root.a",
			},
		},
		Spec: v1.PodSpec{
			NodeName:      "node2",
			SchedulerName: fakeClusterSchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	// app3 -> pod3
	// pending for scheduling
	pod3 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod3",
			Namespace: "default",
			UID:       "UID-POD-00003",
			Labels: map[string]string{
				"applicationId": "app3",
				"queue":         "root.a",
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: fakeClusterSchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	podLister := test.NewPodListerMock()
	podLister.AddPod(&pod1)
	podLister.AddPod(&pod2)
	podLister.AddPod(&pod3)

	if err := context.waitForAppRecovery(podLister, 3*time.Second); err == nil {
		t.Fatalf("expecting timeout here!")
	} else {
		t.Logf("context stays waiting for recovery, error: %v", err)
	}

	// simulate app is accepted by scheduler
	dispatcher.Dispatch(NewSimpleApplicationEvent("app1", events.AcceptApplication))
	dispatcher.Dispatch(NewSimpleApplicationEvent("app2", events.AcceptApplication))

	// apps are accepted, recovery of apps are done
	if err := context.waitForAppRecovery(podLister, 3*time.Second); err == nil {
		t.Logf("recovery exits once all apps are recovered")
	} else {
		t.Fatalf("unexpected failure, error: %v", err)
	}
}