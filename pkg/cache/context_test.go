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
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	k8sEvents "k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	Host1 = "host0001"
	Host2 = "host0002"

	appID1             = "app00001"
	appID2             = "app00002"
	appID3             = "app00003"
	appID4             = "app00004"
	appID5             = "app00005"
	non_existing_appID = "app-none-exist"

	uid1 = "uid_0001"
	uid2 = "uid_0002"
	uid3 = "uid_0003"
	uid4 = "uid_0004"
	uid5 = "uid_0005"
	uid0 = "uid_0000"

	pod1UID     = "task00001"
	pod2UID     = "task00002"
	taskUID1    = "task00001"
	taskUID2    = "task00002"
	taskUID3    = "task00003"
	taskUID4    = "task00004"
	taskUnknown = "non_existing_taskID"

	podName1       = "pod1"
	podName2       = "pod2"
	podName3       = "pod3"
	podName4       = "pod4"
	podForeignName = "foreign-1"
	podForeignUID  = "UUID-foreign-1"
	podNamespace   = "yk"

	nodeName1    = "node1"
	nodeName2    = "node2"
	fakeNodeName = "fake-node"

	queueNameA       = "root.a"
	queueNameB       = "root.b"
	queueNameC       = "root.c"
	queueNameUnknown = "root.other"

	testUser = "test-user"
)

var (
	testGroups = []string{"dev", "yunikorn"}
)

func initContextForTest() *Context {
	ctx, _ := initContextAndAPIProviderForTest()
	return ctx
}

func initContextAndAPIProviderForTest() (*Context, *client.MockedAPIProvider) {
	apis := client.NewMockedAPIProvider(false)
	context := NewContext(apis)
	return context, apis
}

func setVolumeBinder(ctx *Context, binder volumebinding.SchedulerVolumeBinder) {
	mockedAPI := ctx.apiProvider.(*client.MockedAPIProvider) //nolint:errcheck
	mockedAPI.SetVolumeBinder(binder)
}

func newPodHelper(name, namespace, podUID, nodeName string, appID string, podPhase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(podUID),
			Labels:    map[string]string{constants.LabelApplicationID: appID},
		},
		Spec: v1.PodSpec{
			NodeName:      nodeName,
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: podPhase,
		},
	}
}

func TestAddNodes(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
	}

	ctx.addNode(&node)

	assert.Equal(t, true, ctx.schedulerCache.GetNode(Host1) != nil)
}

func TestUpdateNodes(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	oldNodeResource := make(map[v1.ResourceName]resource.Quantity)
	oldNodeResource["memory"] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	oldNodeResource["cpu"] = *resource.NewQuantity(2, resource.DecimalSI)
	oldNode := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
		Status: v1.NodeStatus{
			Allocatable: oldNodeResource,
		},
	}

	newNodeResource := make(map[v1.ResourceName]resource.Quantity)
	newNodeResource["memory"] = *resource.NewQuantity(2048*1000*1000, resource.DecimalSI)
	newNodeResource["cpu"] = *resource.NewQuantity(4, resource.DecimalSI)
	newNode := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
		Status: v1.NodeStatus{
			Allocatable: newNodeResource,
		},
	}

	ctx.addNode(&oldNode)
	ctx.updateNode(&oldNode, &newNode)
}

func TestDeleteNodes(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
	}

	ctx.addNode(&node)
	assert.Equal(t, true, ctx.schedulerCache.GetNode(Host1) != nil)

	ctx.deleteNode(&node)
	assert.Equal(t, true, ctx.schedulerCache.GetNode(Host1) == nil)

	ctx.addNode(&node)
	assert.Equal(t, true, ctx.schedulerCache.GetNode(Host1) != nil)

	ctx.deleteNode(cache.DeletedFinalStateUnknown{Key: uid1, Obj: &node})
	assert.Equal(t, true, ctx.schedulerCache.GetNode(Host1) == nil)
}

func TestAddApplications(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID1] != nil)
	assert.Equal(t, context.applications[appID1].GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, len(context.applications[appID1].GetPendingTasks()), 0)

	// add an app but app already exists
	app := context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameUnknown,
			User:          testUser,
			Tags:          nil,
		},
	})

	assert.Assert(t, app != nil)
	assert.Equal(t, app.GetQueue(), queueNameA)
}

func TestGetApplication(t *testing.T) {
	context := initContextForTest()
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID2,
			QueueName:     queueNameB,
			User:          testUser,
			Tags:          nil,
		},
	})

	app := context.GetApplication(appID1)
	assert.Assert(t, app != nil)
	assert.Equal(t, app.GetApplicationID(), appID1)
	assert.Equal(t, app.GetQueue(), queueNameA)
	assert.Equal(t, app.GetUser(), testUser)

	app = context.GetApplication(appID2)
	assert.Assert(t, app != nil)
	assert.Equal(t, app.GetApplicationID(), appID2)
	assert.Equal(t, app.GetQueue(), queueNameB)
	assert.Equal(t, app.GetUser(), testUser)

	// get a non-exist application
	app = context.GetApplication(non_existing_appID)
	assert.Assert(t, app == nil)
}

func TestRemoveApplication(t *testing.T) {
	context := initContextForTest()
	app1 := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, queueNameB, testUser, testGroups, map[string]string{}, newMockSchedulerAPI())
	context.applications[appID1] = app1
	context.applications[appID2] = app2
	assert.Equal(t, len(context.applications), 2)

	// remove non-exist app
	context.RemoveApplication(appID3)
	assert.Equal(t, len(context.applications), 2)

	// remove app1
	context.RemoveApplication(appID1)
	assert.Equal(t, len(context.applications), 1)
	_, ok := context.applications[appID1]
	assert.Equal(t, ok, false)

	// remove app2
	context.RemoveApplication(appID2)
	assert.Equal(t, len(context.applications), 0)
	_, ok = context.applications[appID2]
	assert.Equal(t, ok, false)
}

func TestAddPod(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID1,
			UID:  uid1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID1,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID2,
			UID:  uid2,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID2,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	context.AddPod(nil)  // no-op, but should not crash
	context.AddPod(pod1) // should be added
	context.AddPod(pod2) // should skip as pod is terminated

	pod := context.schedulerCache.GetPod(uid1)
	assert.Check(t, pod != nil, "active pod was not added")
	pod = context.schedulerCache.GetPod(uid2)
	assert.Check(t, pod == nil, "terminated pod was added")
}

func TestUpdatePod(t *testing.T) { //nolint:funlen
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID1,
			UID:  uid1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID1,
				"test.state":                      "new",
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID1,
			UID:  uid1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID1,
				"test.state":                      "updated",
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID1,
			UID:  uid1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID1,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	context.AddPod(pod1)
	pod := context.schedulerCache.GetPod(uid1)
	assert.Assert(t, pod != nil, "pod1 is not present after adding")

	// these should not fail, but are no-ops
	context.UpdatePod(nil, nil)
	context.UpdatePod(nil, pod1)
	context.UpdatePod(pod1, nil)
	context.UpdatePod(&v1.Node{}, pod2)
	podFromCache := context.schedulerCache.GetPod(uid1)
	assert.Equal(t, "new", podFromCache.Annotations["test.state"])

	// ensure a terminated pod is removed
	context.UpdatePod(pod1, pod3)
	pod = context.schedulerCache.GetPod(uid1)
	assert.Check(t, pod == nil, "pod still found after termination")
	app := context.getApplication(appID1)
	// ensure that an updated pod is updated inside the Task
	task := app.GetTask(uid1)
	assert.Assert(t, task.GetTaskPod() == pod3, "task pod has not been updated")

	// ensure a non-terminated pod is updated
	context.UpdatePod(pod1, pod2)
	found := context.schedulerCache.GetPod(uid1)
	if assert.Check(t, found != nil, "pod not found after update") {
		assert.Check(t, found.GetAnnotations()["test.state"] == "updated", "pod state not updated")
	}

	// scheduling gated pod
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())
	pod4 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID2,
			UID:  uid2,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID2,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: "yunikorn",
			SchedulingGates: []v1.PodSchedulingGate{
				{Name: "gate1"},
				{Name: "gate2"},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	context.UpdatePod(nil, pod4)
	// check that the event has been published
	select {
	case event := <-recorder.Events:
		assert.Assert(t, strings.Contains(event, "waiting for scheduling gates: gate1,gate2"), "unexpected k8s event received for gated pod: %s", event)
	default:
		t.Error("no k8s event received for gated pod")
	}
	assert.Assert(t, context.schedulerCache.GetPod(uid2) != nil, "pod was not found in scheduler cache")
	assert.Assert(t, context.getApplication(appID2) == nil, "application was created")
	pod5 := pod4.DeepCopy()
	pod5.Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: "gate1"}} // remove a single gate
	context.UpdatePod(pod4, pod5)
	// no events expected
	select {
	case event := <-recorder.Events:
		t.Errorf("no k8s event expected, got: %s", event)
	default:
	}
	pod5Cache := context.schedulerCache.GetPod(uid2)
	assert.Assert(t, pod5Cache != nil, "pod was removed from the scheduler cache")
	assert.Assert(t, context.getApplication(appID2) == nil, "application was created unexpectedly")
	assert.Equal(t, 1, len(pod5Cache.Spec.SchedulingGates), "pod was not updated properly in the cache")
	pod6 := pod5.DeepCopy()
	pod6.Spec.SchedulingGates = nil // remove remaining gate
	context.UpdatePod(pod5, pod6)
	// no events expected
	select {
	case event := <-recorder.Events:
		t.Errorf("no k8s event expected, got: %s", event)
	default:
	}
	pod6Cache := context.schedulerCache.GetPod(uid2)
	assert.Assert(t, pod6Cache != nil, "pod was removed from the scheduler cache")
	assert.Equal(t, 0, len(pod6Cache.Spec.SchedulingGates), "pod was not updated properly in the cache")
	assert.Assert(t, context.getApplication(appID2) != nil, "application was not created")
	assert.Assert(t, context.getTask(appID2, uid2) != nil, "task was not created")
}

func TestDeletePod(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID1,
			UID:  uid1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID1,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: appID2,
			UID:  uid2,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID2,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}

	context.AddPod(pod1)
	context.AddPod(pod2)
	pod := context.schedulerCache.GetPod(uid1)
	assert.Assert(t, pod != nil, "pod1 is not present after adding")
	pod = context.schedulerCache.GetPod(uid2)
	assert.Assert(t, pod != nil, "pod2 is not present after adding")

	// these should not fail, but here for completeness
	context.DeletePod(nil)
	context.DeletePod(cache.DeletedFinalStateUnknown{Key: uid0, Obj: nil})

	context.DeletePod(pod1)
	pod = context.schedulerCache.GetPod(uid1)
	assert.Check(t, pod == nil, "pod1 is still present")

	context.DeletePod(cache.DeletedFinalStateUnknown{Key: uid2, Obj: pod2})
	pod = context.schedulerCache.GetPod(uid2)
	assert.Check(t, pod == nil, "pod2 is still present")
}

//nolint:funlen
func TestAddUpdatePodForeign(t *testing.T) {
	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	var allocRequest *si.AllocationRequest
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		allocRequest = request
		return nil
	})
	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			if node.Action == si.NodeInfo_CREATE_DRAIN {
				dispatcher.Dispatch(CachedSchedulerNodeEvent{
					NodeID: node.NodeID,
					Event:  NodeAccepted,
				})
			}
		}
		return nil
	})

	host1 := nodeForTest(Host1, "10G", "10") // add existing foreign pod
	context.updateNode(nil, host1)

	// pod is not assigned to any node
	pod1 := foreignPod(podName1, "1G", "500m")
	pod1.Status.Phase = v1.PodPending
	pod1.Spec.NodeName = ""

	// validate add (pending, no node assigned)
	allocRequest = nil
	context.AddPod(pod1)
	assert.Assert(t, allocRequest == nil, "unexpected update")
	pod := context.schedulerCache.GetPod(string(pod1.UID))
	assert.Assert(t, pod == nil, "unassigned pod found in cache")

	// validate update (no change)
	allocRequest = nil
	context.UpdatePod(nil, pod1)
	assert.Assert(t, allocRequest == nil, "unexpected update")
	pod = context.schedulerCache.GetPod(string(pod1.UID))
	assert.Assert(t, pod == nil, "unassigned pod found in cache")

	// pod is assigned to a node but still in pending state, should update
	pod2 := foreignPod(podName2, "1G", "500m")
	pod2.Status.Phase = v1.PodPending
	pod2.Spec.NodeName = Host1

	// validate add
	context.AddPod(pod2)
	assert.Assert(t, allocRequest != nil, "update expected")
	assertAddForeignPod(t, podName2, Host1, allocRequest)
	pod = context.schedulerCache.GetPod(string(pod2.UID))
	assert.Assert(t, pod != nil, "pod not found in cache")

	// validate update (no change)
	allocRequest = nil
	context.UpdatePod(nil, pod2)
	assert.Assert(t, allocRequest != nil, "update expected")
	pod = context.schedulerCache.GetPod(string(pod2.UID))
	assert.Assert(t, pod != nil, "pod not found in cache")

	// validate update when not already in cache
	allocRequest = nil
	context.DeletePod(pod2)
	assertReleaseForeignPod(t, podName2, allocRequest)

	allocRequest = nil
	context.UpdatePod(nil, pod2)
	assert.Assert(t, allocRequest != nil, "expected update")
	pod = context.schedulerCache.GetPod(string(pod2.UID))
	assert.Assert(t, pod != nil, "pod not found in cache")
	assertAddForeignPod(t, podName2, Host1, allocRequest)

	// pod is failed, should trigger update if already in cache
	pod3 := pod2.DeepCopy()
	pod3.Status.Phase = v1.PodFailed

	// validate add
	allocRequest = nil
	context.AddPod(pod3)
	assert.Assert(t, allocRequest != nil, "expected update")
	pod = context.schedulerCache.GetPod(string(pod3.UID))
	assert.Assert(t, pod == nil, "failed pod found in cache")
	assert.Assert(t, allocRequest.Releases != nil) // expecting a release due to pod status
	assertReleaseForeignPod(t, podName2, allocRequest)
}

func assertAddForeignPod(t *testing.T, podName, host string, allocRequest *si.AllocationRequest) {
	t.Helper()
	assert.Equal(t, 1, len(allocRequest.Allocations))
	tags := allocRequest.Allocations[0].AllocationTags
	assert.Equal(t, 4, len(tags))
	assert.Equal(t, siCommon.AllocTypeDefault, tags[siCommon.Foreign])
	assert.Equal(t, tags["kubernetes.io/meta/namespace"], "testNamespace")
	assert.Equal(t, tags["kubernetes.io/meta/podName"], podName)
	assert.Equal(t, podName, allocRequest.Allocations[0].AllocationKey)
	assert.Equal(t, host, allocRequest.Allocations[0].NodeID)
}

func assertReleaseForeignPod(t *testing.T, podName string, allocRequest *si.AllocationRequest) {
	t.Helper()
	assert.Assert(t, allocRequest.Releases != nil)
	assert.Equal(t, 1, len(allocRequest.Releases.AllocationsToRelease))
	assert.Equal(t, podName, allocRequest.Releases.AllocationsToRelease[0].AllocationKey)
	assert.Equal(t, constants.DefaultPartition, allocRequest.Releases.AllocationsToRelease[0].PartitionName)
	assert.Equal(t, "", allocRequest.Releases.AllocationsToRelease[0].ApplicationID)
	assert.Equal(t, si.TerminationType_STOPPED_BY_RM, allocRequest.Releases.AllocationsToRelease[0].TerminationType)
}

func TestDeletePodForeign(t *testing.T) {
	context, apiProvider := initContextAndAPIProviderForTest()

	var allocRequest *si.AllocationRequest
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		allocRequest = request
		return nil
	})

	// add existing foreign pod
	pod1 := foreignPod(podName1, "1G", "500m")
	pod1.Status.Phase = v1.PodRunning
	pod1.Spec.NodeName = Host1
	context.AddPod(pod1)
	allocRequest = nil
	context.DeletePod(pod1)

	assert.Assert(t, allocRequest != nil, "update not executed")
	assert.Equal(t, 0, len(allocRequest.Allocations))
	assert.Assert(t, allocRequest.Releases != nil)
	assert.Equal(t, 1, len(allocRequest.Releases.AllocationsToRelease))
	assert.Equal(t, podName1, allocRequest.Releases.AllocationsToRelease[0].AllocationKey)
	assert.Equal(t, constants.DefaultPartition, allocRequest.Releases.AllocationsToRelease[0].PartitionName)
	assert.Equal(t, "", allocRequest.Releases.AllocationsToRelease[0].ApplicationID)
	assert.Equal(t, si.TerminationType_STOPPED_BY_RM, allocRequest.Releases.AllocationsToRelease[0].TerminationType)
	pod := context.schedulerCache.GetPod(string(pod1.UID))
	assert.Assert(t, pod == nil, "deleted pod found in cache")
}

func TestAddTask(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID1] != nil)
	assert.Equal(t, context.applications[appID1].GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, len(context.applications[appID1].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID1,
			Pod:           &v1.Pod{},
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID1)

	// add another task
	task = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID2,
			Pod:           &v1.Pod{},
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID2)

	// add a task with dup taskID
	task = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID2,
			Pod:           &v1.Pod{},
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID2)

	// add a task without app's appearance
	task = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: non_existing_appID,
			TaskID:        taskUID1,
			Pod:           &v1.Pod{},
		},
	})
	assert.Assert(t, task == nil)

	// verify number of tasks in cache
	assert.Equal(t, len(context.applications[appID1].GetNewTasks()), 2)
}

//nolint:funlen
func TestRecoverTask(t *testing.T) {
	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	dispatcher.RegisterEventHandler("TestAppHandler", dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler("TestTaskHandler", dispatcher.EventTypeTask, context.TaskEventHandler())
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		for _, alloc := range request.Allocations {
			if alloc.NodeID != "" {
				dispatcher.Dispatch(AllocatedTaskEvent{
					applicationID: alloc.ApplicationID,
					taskID:        alloc.AllocationKey,
					nodeID:        alloc.NodeID,
					allocationKey: alloc.AllocationKey,
					event:         TaskAllocated,
				})
			}
		}
		return nil
	})

	// add a new application
	app := context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          testUser,
			Tags:          nil,
		},
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID] != nil)
	assert.Equal(t, len(context.applications[appID].GetPendingTasks()), 0)

	// add a tasks to the existing application
	// this task was already allocated and Running
	task := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID1,
			Pod:           newPodHelper(podName1, podNamespace, taskUID1, fakeNodeName, appID, v1.PodRunning),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID1)

	app.SetState("Running")
	app.Schedule()

	// wait for task to transition to bound state
	err := utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Bound
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "failed to wait for allocation allocationKey being set for task")

	// add a tasks to the existing application
	// this task was already completed with state: Succeed
	task = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID2,
			Pod:           newPodHelper(podName2, podNamespace, taskUID2, fakeNodeName, appID, v1.PodSucceeded),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID2)
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)

	// add a tasks to the existing application
	// this task was already completed with state: Succeed
	task = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID3,
			Pod:           newPodHelper(podName3, podNamespace, taskUID3, fakeNodeName, appID, v1.PodFailed),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID3)
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)

	// add a tasks to the existing application
	// this task pod is still Pending
	task = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID4,
			Pod:           newPodHelper(podName4, podNamespace, taskUID4, "", appID, v1.PodPending),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID4)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// make sure the recovered task is added to the app
	app, exist := context.applications[appID]
	assert.Equal(t, exist, true)
	assert.Equal(t, len(app.getTasks(TaskStates().Bound)), 1)
	assert.Equal(t, len(app.getTasks(TaskStates().Completed)), 2)
	assert.Equal(t, len(app.getTasks(TaskStates().New)), 1)

	taskInfoVerifiers := []struct {
		taskID                string
		expectedState         string
		expectedAllocationKey string
		expectedPodName       string
		expectedNodeName      string
	}{
		{taskUID1, TaskStates().Bound, taskUID1, podName1, fakeNodeName},
		{taskUID2, TaskStates().Completed, taskUID2, podName2, fakeNodeName},
		{taskUID3, TaskStates().Completed, taskUID3, podName3, fakeNodeName},
		{taskUID4, TaskStates().New, "", podName4, ""},
	}

	for _, tt := range taskInfoVerifiers {
		t.Run(tt.taskID, func(t *testing.T) {
			// verify the info for the recovered task
			rt := app.GetTask(tt.taskID)
			assert.Equal(t, rt.GetTaskState(), tt.expectedState)
			assert.Equal(t, rt.allocationKey, tt.expectedAllocationKey)
			assert.Equal(t, rt.pod.Name, tt.expectedPodName)
			assert.Equal(t, rt.alias, fmt.Sprintf("%s/%s", podNamespace, tt.expectedPodName))
		})
	}
}

func TestTaskReleaseAfterRecovery(t *testing.T) {
	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.RegisterEventHandler("TestAppHandler", dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler("TestTaskHandler", dispatcher.EventTypeTask, context.TaskEventHandler())
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		for _, alloc := range request.Allocations {
			if alloc.NodeID != "" {
				dispatcher.Dispatch(AllocatedTaskEvent{
					applicationID: alloc.ApplicationID,
					taskID:        alloc.AllocationKey,
					nodeID:        alloc.NodeID,
					allocationKey: alloc.AllocationKey,
					event:         TaskAllocated,
				})
			}
		}
		return nil
	})

	// do app recovery, first recover app, then tasks
	// add application to recovery
	app := context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID] != nil)
	assert.Equal(t, len(context.applications[appID].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task0 := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID1,
			Pod:           newPodHelper(podName1, podNamespace, pod1UID, fakeNodeName, appID, v1.PodRunning),
		},
	})

	assert.Assert(t, task0 != nil)
	assert.Equal(t, task0.GetTaskID(), taskUID1)

	app.SetState("Running")
	app.Schedule()

	// wait for task to transition to bound state
	err := utils.WaitForCondition(func() bool {
		return task0.GetTaskState() == TaskStates().Bound
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "failed to wait for allocation allocationKey being set for task0")

	task1 := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID2,
			Pod:           newPodHelper(podName2, podNamespace, pod2UID, fakeNodeName, appID, v1.PodRunning),
		},
	})

	assert.Assert(t, task1 != nil)
	assert.Equal(t, task1.GetTaskID(), pod2UID)

	app.Schedule()

	// wait for task to transition to bound state
	err = utils.WaitForCondition(func() bool {
		return task1.GetTaskState() == TaskStates().Bound
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "failed to wait for allocation allocationKey being set for task1")

	// app should have 2 tasks recovered
	app, exist := context.applications[appID]
	assert.Equal(t, exist, true)
	assert.Equal(t, len(app.GetBoundTasks()), 2)

	// release one of the tasks
	context.notifyTaskComplete(app, pod2UID)

	// wait for release
	err = utils.WaitForCondition(func() bool {
		return task1.GetTaskState() == TaskStates().Completed
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "release should be completed for task1")

	// expect to see:
	//  - task0 is still there
	//  - task1 gets released
	assert.Equal(t, task0.GetTaskState(), TaskStates().Bound)
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)
}

func TestRemoveTask(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})

	// add 2 tasks
	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID1,
			Pod:           &v1.Pod{},
		},
	})
	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID2,
			Pod:           &v1.Pod{},
		},
	})

	// verify app and tasks
	app := context.GetApplication(appID1)
	assert.Assert(t, app != nil)

	// now app should have 2 tasks
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// try to remove a non-exist task
	context.RemoveTask(appID1, taskUnknown)
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// try to remove a task from non-exist application
	context.RemoveTask(non_existing_appID, taskUID1)
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// this should success
	context.RemoveTask(appID1, taskUID1)

	// now only 1 task left
	assert.Equal(t, len(app.GetNewTasks()), 1)

	// this should success
	context.RemoveTask(appID1, taskUID2)

	// now there is no task left
	assert.Equal(t, len(app.GetNewTasks()), 0)
}

func TestGetTask(t *testing.T) {
	// add 3 applications
	context := initContextForTest()
	app1 := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, queueNameB, testUser, testGroups, map[string]string{}, newMockSchedulerAPI())
	app3 := NewApplication(appID3, queueNameC, testUser, testGroups, map[string]string{}, newMockSchedulerAPI())
	context.applications[appID1] = app1
	context.applications[appID2] = app2
	context.applications[appID3] = app3
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: taskUID1,
			UID:  uid1,
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: taskUID2,
			UID:  uid2,
		},
	}
	// New task to application 1
	// set task state in Pending (non-terminated)
	task1 := NewTask(taskUID1, app1, context, pod1)
	app1.taskMap[taskUID1] = task1
	task1.sm.SetState(TaskStates().Pending)
	// New task to application 2
	// set task state in Failed (terminated)
	task2 := NewTask(taskUID2, app2, context, pod2)
	app2.taskMap[taskUID2] = task2
	task2.sm.SetState(TaskStates().Failed)

	task := context.getTask(appID1, taskUID1)
	assert.Assert(t, task == task1)

	task = context.getTask(non_existing_appID, taskUID1)
	assert.Assert(t, task == nil)

	task = context.getTask(appID1, taskUnknown)
	assert.Assert(t, task == nil)

	task = context.getTask(appID3, taskUID3)
	assert.Assert(t, task == nil)
}

func TestNodeEventFailsPublishingWithoutNode(t *testing.T) {
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())
	context := initContextForTest()

	eventRecords := make([]*si.EventRecord, 0)
	message := "non_existing_node_related_message"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:     si.EventRecord_NODE,
		ObjectID: "non_existing_host",
		Message:  message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has been published
	select {
	case event := <-recorder.Events:
		log.Log(log.Test).Info(event)
		if strings.Contains(event, message) {
			t.Fatal("event should not be published if the pod does not exist")
		}
	default:
		break
	}
}

func TestNodeEventPublishedCorrectly(t *testing.T) {
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
	}
	context.addNode(&node)
	err := waitForNodeAcceptedEvent(recorder)
	assert.NilError(t, err, "node accepted event was not sent")

	eventRecords := make([]*si.EventRecord, 0)
	message := "node_related_message"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_DETAILS_NONE,
		ObjectID:          Host1,
		Message:           message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has been published
	err = utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				log.Log(log.Test).Info(event)
				if strings.Contains(event, message) {
					return true
				}
			default:
				return false
			}
		}
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "event should have been emitted")
}

func TestFilteredEventsNotPublished(t *testing.T) {
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
	}
	context.addNode(&node)
	err := waitForNodeAcceptedEvent(recorder)
	assert.NilError(t, err, "node accepted event was not sent")

	eventRecords := make([]*si.EventRecord, 6)
	eventRecords[0] = &si.EventRecord{
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_NODE_SCHEDULABLE,
		ObjectID:          Host1,
		Message:           "",
	}
	eventRecords[1] = &si.EventRecord{
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_NODE_OCCUPIED,
		ObjectID:          Host1,
		Message:           "",
	}
	eventRecords[2] = &si.EventRecord{
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_NODE_CAPACITY,
		ObjectID:          Host1,
		Message:           "",
	}
	eventRecords[3] = &si.EventRecord{
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_NODE_ALLOC,
		ObjectID:          Host1,
		Message:           "",
	}
	eventRecords[4] = &si.EventRecord{
		Type:              si.EventRecord_APP,
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_APP_RUNNING,
		ObjectID:          "app-1",
		Message:           "",
	}
	eventRecords[5] = &si.EventRecord{
		Type:              si.EventRecord_QUEUE,
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_DETAILS_NONE,
		ObjectID:          "root.test",
		Message:           "",
	}
	context.PublishEvents(eventRecords)

	select {
	case e := <-recorder.Events:
		t.Errorf("received an unexpected event %s", e)
	default:
	}
}

func TestPublishEventsWithNotExistingAsk(t *testing.T) {
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	context := initContextForTest()
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app_event_12",
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	eventRecords := make([]*si.EventRecord, 0)
	message := "event_related_text_msg"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:        si.EventRecord_REQUEST,
		ObjectID:    taskUnknown,
		ReferenceID: "app_event_12",
		Message:     message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has not been published
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, message) {
					return false
				}
			default:
				return true
			}
		}
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "event should not have been published if the pod does not exist")
}

func TestPublishEventsCorrectly(t *testing.T) {
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	context := initContextForTest()

	// create fake application and task
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID1,
			Pod:           &v1.Pod{},
		},
	})

	// create an event belonging to that task
	eventRecords := make([]*si.EventRecord, 0)
	message := "event_related_message"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:        si.EventRecord_REQUEST,
		ObjectID:    taskUID1,
		ReferenceID: appID1,
		Message:     message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has been published
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, message) {
					return true
				}
			default:
				return false
			}
		}
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "event should have been emitted")
}

//nolint:funlen
func TestAddApplicationsWithTags(t *testing.T) {
	context := initContextForTest()

	lister, ok := context.apiProvider.GetAPIs().NamespaceInformer.Lister().(*test.MockNamespaceLister)
	if !ok {
		t.Fatalf("could not mock NamespaceLister")
	}

	// set up namespaces
	ns1 := v1.Namespace{
		ObjectMeta: apis.ObjectMeta{
			Name: "test1",
			Annotations: map[string]string{
				constants.DomainYuniKorn + "namespace.max.memory": "256M",
			},
		},
	}
	lister.Add(&ns1)
	ns2 := v1.Namespace{
		ObjectMeta: apis.ObjectMeta{
			Name: "test2",
			Annotations: map[string]string{
				constants.NamespaceQuota:                 "{\"cpu\": \"1\", \"memory\": \"256M\", \"nvidia.com/gpu\": \"1\"}",
				constants.DomainYuniKorn + "parentqueue": "root.test",
				constants.NamespaceGuaranteed:            "{\"cpu\": \"1\", \"memory\": \"256M\", \"nvidia.com/gpu\": \"1\"}",
				constants.NamespaceMaxApps:               "1000",
			},
		},
	}
	lister.Add(&ns2)

	// add application with empty namespace
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags: map[string]string{
				constants.AppTagNamespace: "",
			},
		},
	})

	// add application with non-existing namespace
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID2,
			QueueName:     queueNameA,
			User:          testUser,
			Tags: map[string]string{
				constants.AppTagNamespace: "non-existing",
			},
		},
	})

	// add application with unannotated namespace
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID3,
			QueueName:     queueNameA,
			User:          testUser,
			Tags: map[string]string{
				constants.AppTagNamespace: "test1",
			},
		},
	})

	// add application with annotated namespace
	request := &AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID4,
			QueueName:     queueNameA,
			User:          testUser,
			Tags: map[string]string{
				constants.AppTagNamespace: "test2",
			},
		},
	}
	context.AddApplication(request)

	// check that request has additional annotations
	quotaStr, ok := request.Metadata.Tags[siCommon.AppTagNamespaceResourceQuota]
	if !ok {
		t.Fatalf("resource quota tag is not updated from the namespace")
	}
	quotaRes := si.Resource{}
	if err := json.Unmarshal([]byte(quotaStr), &quotaRes); err == nil {
		if quotaRes.Resources == nil || quotaRes.Resources["memory"] == nil || quotaRes.Resources["vcore"] == nil || quotaRes.Resources["nvidia.com/gpu"] == nil {
			t.Fatalf("could not find parsed quota resource from annotation")
		}
		assert.Equal(t, quotaRes.Resources["memory"].Value, int64(256*1000*1000))
		assert.Equal(t, quotaRes.Resources["vcore"].Value, int64(1000))
		assert.Equal(t, quotaRes.Resources["nvidia.com/gpu"].Value, int64(1))
	} else {
		t.Fatalf("resource parsing failed")
	}

	guaranteedStr, ok := request.Metadata.Tags[siCommon.AppTagNamespaceResourceGuaranteed]
	if !ok {
		t.Fatalf("resource guaranteed tag is not updated from the namespace")
	}

	guaranteedRes := si.Resource{}
	if err := json.Unmarshal([]byte(guaranteedStr), &guaranteedRes); err == nil {
		if guaranteedRes.Resources == nil || guaranteedRes.Resources["memory"] == nil || guaranteedRes.Resources["nvidia.com/gpu"] == nil || guaranteedRes.Resources["vcore"] == nil {
			t.Fatalf("could not find parsed guaranteed resource from annotation")
		}
		assert.Equal(t, quotaRes.Resources["memory"].Value, int64(256*1000*1000))
		assert.Equal(t, quotaRes.Resources["vcore"].Value, int64(1000))
		assert.Equal(t, quotaRes.Resources["nvidia.com/gpu"].Value, int64(1))
	} else {
		t.Fatalf("resource parsing failed")
	}

	maxApps, ok := request.Metadata.Tags[siCommon.AppTagNamespaceResourceMaxApps]
	if !ok {
		t.Fatalf("max apps tag is not updated from the namespace")
	}
	assert.Equal(t, maxApps, "1000")

	parentQueue, ok := request.Metadata.Tags[constants.AppTagNamespaceParentQueue]
	if !ok {
		t.Fatalf("parent queue tag is not updated from the namespace")
	}
	assert.Equal(t, parentQueue, "root.test")

	// add application with annotated namespace to check the old quota annotation
	request = &AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID5,
			QueueName:     queueNameA,
			User:          testUser,
			Tags: map[string]string{
				constants.AppTagNamespace: "test1",
			},
		},
	}
	context.AddApplication(request)

	// check that request has additional annotations
	quotaStr, ok = request.Metadata.Tags[siCommon.AppTagNamespaceResourceQuota]
	if !ok {
		t.Fatalf("resource quota tag is not updated from the namespace")
	}
	quotaRes = si.Resource{}
	if err := json.Unmarshal([]byte(quotaStr), &quotaRes); err == nil {
		if quotaRes.Resources == nil || quotaRes.Resources["memory"] == nil {
			t.Fatalf("could not find parsed memory resource from annotation")
		}
		assert.Equal(t, quotaRes.Resources["memory"].Value, int64(256*1000*1000))
	} else {
		t.Fatalf("resource parsing failed")
	}
}

func TestPendingPodAllocations(t *testing.T) {
	utils.SetPluginMode(true)
	defer utils.SetPluginMode(false)

	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	node1 := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
	}
	context.addNode(&node1)

	node2 := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host2,
			Namespace: "default",
			UID:       uid2,
		},
	}
	context.addNode(&node2)

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: taskUID1,
			UID:  uid1,
		},
	}

	// add a tasks to the existing application
	task := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID1,
			Pod:           pod,
		},
	})
	assert.Assert(t, task != nil, "task was nil")

	// add the allocation
	context.AddPendingPodAllocation(uid1, Host1)

	// validate that the pending allocation matches
	nodeID, ok := context.GetPendingPodAllocation(uid1)
	if !ok {
		t.Fatalf("no pending pod allocation found")
	}
	assert.Equal(t, nodeID, Host1, "wrong host")

	// validate that there is not an in-progress allocation
	if _, ok = context.GetInProgressPodAllocation(uid1); ok {
		t.Fatalf("in-progress allocation exists when it should be pending")
	}

	if context.StartPodAllocation(uid1, Host2) {
		t.Fatalf("attempt to start pod allocation on wrong node succeeded")
	}

	if !context.StartPodAllocation(uid1, Host1) {
		t.Fatalf("attempt to start pod allocation on correct node failed")
	}

	if _, ok = context.GetPendingPodAllocation(uid1); ok {
		t.Fatalf("pending pod allocation still exists after transition to in-progress")
	}

	nodeID, ok = context.GetInProgressPodAllocation(uid1)
	if !ok {
		t.Fatalf("in-progress allocation does not exist")
	}
	assert.Equal(t, nodeID, Host1, "wrong host")

	context.RemovePodAllocation(uid1)
	if _, ok = context.GetInProgressPodAllocation(uid1); ok {
		t.Fatalf("in-progress pod allocation still exists after removal")
	}

	// re-add to validate pending pod removal
	context.AddPendingPodAllocation(uid1, Host1)
	context.RemovePodAllocation(uid1)

	if _, ok = context.GetPendingPodAllocation(uid1); ok {
		t.Fatalf("pending pod allocation still exists after removal")
	}
}

func TestGetStateDump(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Namespace: "default",
			Name:      appID1,
			UID:       uid1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID1,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	context.AddPod(pod1)

	stateDumpStr, err := context.GetStateDump()
	assert.NilError(t, err, "error during state dump")
	var stateDump = make(map[string]interface{})
	err = json.Unmarshal([]byte(stateDumpStr), &stateDump)
	assert.NilError(t, err, "unable to parse state dump")

	cacheObj, ok := stateDump["cache"]
	assert.Assert(t, ok, "cache not found")
	cache, ok := cacheObj.(map[string]interface{})
	assert.Assert(t, ok, "unable to cast cache")

	podsObj, ok := cache["pods"]
	assert.Assert(t, ok, "pods not found")
	pods, ok := podsObj.(map[string]interface{})
	assert.Assert(t, ok, "unable to cast pods")

	podObj, ok := pods["default/app00001"]
	assert.Assert(t, ok, "pod not found")
	pod, ok := podObj.(map[string]interface{})
	assert.Assert(t, ok, "unable to cast pod")

	uidObj, ok := pod["uid"]
	assert.Assert(t, ok, "uid not found")
	uid, ok := uidObj.(string)
	assert.Assert(t, ok, "Unable to cast uid")

	assert.Equal(t, string(pod1.UID), uid, "wrong uid")
}

func TestFilterPriorityClasses(t *testing.T) {
	context := initContextForTest()
	policy := v1.PreemptLowerPriority
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "pc-test",
		},
		Value:            100,
		GlobalDefault:    false,
		PreemptionPolicy: &policy,
	}

	assert.Check(t, !context.filterPriorityClasses(nil), "nil object was allowed")
	assert.Check(t, !context.filterPriorityClasses("test"), "non-pc class was allowed")
	assert.Check(t, context.filterPriorityClasses(pc), "pc was ignored")
}

func TestAddPriorityClass(t *testing.T) {
	context := initContextForTest()
	policy := v1.PreemptLowerPriority
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "pc-test",
		},
		Value:            100,
		GlobalDefault:    false,
		PreemptionPolicy: &policy,
	}
	context.addPriorityClass(pc)
	result := context.schedulerCache.GetPriorityClass("pc-test")
	assert.Assert(t, result != nil)
	assert.Equal(t, result.Value, int32(100))
}

func TestUpdatePriorityClass(t *testing.T) {
	context := initContextForTest()
	policy := v1.PreemptLowerPriority
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "pc-test",
		},
		Value:            100,
		GlobalDefault:    false,
		PreemptionPolicy: &policy,
	}
	policy2 := v1.PreemptNever
	pc2 := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "pc-test",
		},
		Value:            200,
		GlobalDefault:    false,
		PreemptionPolicy: &policy2,
	}

	context.addPriorityClass(pc)
	context.updatePriorityClass(pc, pc2)
	result := context.schedulerCache.GetPriorityClass("pc-test")
	assert.Assert(t, result != nil)
	assert.Equal(t, result.Value, int32(200))
}

func TestDeletePriorityClass(t *testing.T) {
	context := initContextForTest()
	policy := v1.PreemptLowerPriority
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "pc-test",
		},
		Value:            100,
		GlobalDefault:    false,
		PreemptionPolicy: &policy,
	}

	context.addPriorityClass(pc)
	result := context.schedulerCache.GetPriorityClass("pc-test")
	assert.Assert(t, result != nil)
	context.deletePriorityClass(pc)
	result = context.schedulerCache.GetPriorityClass("pc-test")
	assert.Assert(t, result == nil)
}

func TestCtxUpdatePodCondition(t *testing.T) {
	condition := v1.PodCondition{
		Type:   v1.ContainersReady,
		Status: v1.ConditionTrue,
	}
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				condition,
			},
		},
	}
	context := initContextForTest()
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID1,
			QueueName:     queueNameA,
			User:          testUser,
			Tags:          nil,
		},
	})
	task := context.AddTask(&AddTaskRequest{ //nolint:errcheck
		Metadata: TaskMetadata{
			ApplicationID: appID1,
			TaskID:        taskUID1,
			Pod:           pod,
		},
	})

	// task state is not Scheduling
	updated := context.updatePodCondition(task, &condition)
	assert.Equal(t, false, updated)

	// no update
	task.sm.SetState(TaskStates().Scheduling)
	updated = context.updatePodCondition(task, &condition)
	assert.Equal(t, false, updated)

	// update status
	condition.Status = v1.ConditionFalse
	updated = context.updatePodCondition(task, &condition)
	assert.Equal(t, true, updated)
}

func TestGetExistingAllocation(t *testing.T) {
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": appID1,
				"queue":         queueNameA,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "allocated-node",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// verifies the existing allocation is correctly returned
	alloc := getExistingAllocation(pod)
	assert.Equal(t, alloc.ApplicationID, appID1)
	assert.Equal(t, alloc.AllocationKey, string(pod.UID))
	assert.Equal(t, alloc.NodeID, "allocated-node")
}

//nolint:funlen
func TestInitializeState(t *testing.T) {
	context, apiProvider := initContextAndAPIProviderForTest()
	apiProvider.RunEventHandler()
	pcLister, ok := apiProvider.GetAPIs().PriorityClassInformer.Lister().(*test.MockPriorityClassLister)
	assert.Assert(t, ok, "unable to get mock priority class lister")
	nodeLister, ok := apiProvider.GetAPIs().NodeInformer.Lister().(*test.NodeListerMock)
	assert.Assert(t, ok, "unable to get mock node lister")
	podLister, ok := apiProvider.GetAPIs().PodInformer.Lister().(*test.PodListerMock)
	assert.Assert(t, ok, "unable to get mock pod lister")

	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	// add a preemption policy
	policy := v1.PreemptLowerPriority
	pcLister.Add(&schedulingv1.PriorityClass{
		ObjectMeta: apis.ObjectMeta{
			Name: "preempt-lower-1000",
			Annotations: map[string]string{
				constants.AnnotationAllowPreemption: constants.True,
			},
		},
		Value:            1000,
		PreemptionPolicy: &policy,
	})

	// add a test node
	nodeLister.AddNode(nodeForTest(nodeName1, "10G", "4"))

	// add a pending non-yunikorn pod
	foreignPending := foreignPod("foreignPending", "1G", "500m")
	foreignPending.Status.Phase = v1.PodPending
	podLister.AddPod(foreignPending)

	// add a running non-yunikorn pod
	foreignRunning := foreignPod("foreignRunning", "2G", "1500m")
	foreignRunning.Status.Phase = v1.PodRunning
	foreignRunning.Spec.NodeName = nodeName1
	podLister.AddPod(foreignRunning)

	// add a pending yunikorn-managed pod
	pending := newPodHelper("pending", "default", podName1, "", appID1, v1.PodPending)
	pending.Spec.Containers = []v1.Container{{
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"memory": resource.MustParse("1G"),
				"cpu":    resource.MustParse("500m"),
			},
		},
	}}

	podLister.AddPod(pending)

	// add a running yunikorn-managed pod
	running := newPodHelper("running", "default", podName2, nodeName1, appID2, v1.PodRunning)
	running.Spec.Containers = []v1.Container{{
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"memory": resource.MustParse("2G"),
				"cpu":    resource.MustParse("1"),
			},
		},
	}}
	podLister.AddPod(running)

	// add an orphaned yunikorn-managed pod
	orphaned := newPodHelper("running", "default", podName3, nodeName2, appID3, v1.PodRunning)
	orphaned.Spec.Containers = []v1.Container{{
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"memory": resource.MustParse("3G"),
				"cpu":    resource.MustParse("300m"),
			},
		},
	}}
	podLister.AddPod(orphaned)
	// add an orphan foreign pod
	orphanForeign := newPodHelper(podForeignName, "default", podForeignUID, nodeName2, "", v1.PodRunning)
	orphanForeign.Spec.SchedulerName = ""
	podLister.AddPod(orphanForeign)

	err := context.InitializeState()
	assert.NilError(t, err, "InitializeState failed")

	// verify that priorityclass was added to cache
	pc := context.schedulerCache.GetPriorityClass("preempt-lower-1000")
	assert.Assert(t, pc != nil, "priorityClass not found")
	assert.Equal(t, pc.Value, int32(1000), "wrong priority value")
	assert.Equal(t, *pc.PreemptionPolicy, policy, "wrong preemption policy")
	assert.Equal(t, pc.Annotations[constants.AnnotationAllowPreemption], constants.True, "wrong allow-preemption value")

	// check that pod orphan status is correct
	assert.Check(t, !context.schedulerCache.IsPodOrphaned(podName1), "pod1 should not be orphaned")
	assert.Check(t, !context.schedulerCache.IsPodOrphaned(podName2), "pod2 should not be orphaned")
	assert.Check(t, context.schedulerCache.IsPodOrphaned(podName3), "pod3 should be orphaned")
	assert.Check(t, context.schedulerCache.IsPodOrphaned(podForeignUID), "foreign pod should be orphaned")
	assert.Check(t, context.schedulerCache.GetPod("foreignRunning") != nil, "foreign running pod is not in the cache")
	assert.Check(t, context.schedulerCache.GetPod("foreignPending") == nil, "foreign pending pod should not be in the cache")
	assert.Check(t, !context.schedulerCache.IsPodOrphaned("foreignRunning"), "foreign running pod should not be orphaned")

	// pod1 is pending
	task1 := context.getTask(appID1, podName1)
	assert.Assert(t, task1 != nil, "pod1 not found")
	assert.Equal(t, task1.pod.Spec.NodeName, "", "wrong node for pod1")

	// pod 2 is running
	task2 := context.getTask(appID2, podName2)
	assert.Assert(t, task2 != nil, "pod2 not found")
	assert.Equal(t, task2.pod.Spec.NodeName, nodeName1, "wrong node for pod2")

	// pod3 is an orphan, should not be found
	task3 := context.getTask(appID3, podName3)
	assert.Assert(t, task3 == nil, "pod3 was found")
}

func TestPodAdoption(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	// add pods w/o node & check orphan status
	pod1 := newPodHelper(podName1, namespace, pod1UID, Host1, appID, v1.PodRunning)
	pod2 := newPodHelper(podForeignName, namespace, podForeignUID, Host1, "", v1.PodRunning)
	pod2.Spec.SchedulerName = ""
	ctx.AddPod(pod1)
	ctx.AddPod(pod2)
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(pod1UID), "Yunikorn pod is not orphan")
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(podForeignUID), "foreign pod is not orphan")

	// add node
	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host1,
			Namespace: "default",
			UID:       uid1,
		},
	}
	ctx.addNode(&node)

	// check that node has adopted the pods
	assert.Assert(t, !ctx.schedulerCache.IsPodOrphaned(pod1UID), "Yunikorn pod has not been adopted")
	assert.Assert(t, !ctx.schedulerCache.IsPodOrphaned(podForeignUID), "foreign pod has not been adopted")
}

func TestOrphanPodUpdate(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	var update atomic.Bool
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		update.Store(true)
		return nil
	})

	// add pods w/o node & check orphan status
	pod1 := newPodHelper(podName1, namespace, pod1UID, Host1, appID, v1.PodPending)
	pod2 := newPodHelper(podForeignName, namespace, podForeignUID, Host1, "", v1.PodPending)
	pod2.Spec.SchedulerName = ""
	ctx.AddPod(pod1)
	ctx.AddPod(pod2)
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(pod1UID), "Yunikorn pod is not orphan")
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(podForeignUID), "foreign pod is not orphan")
	assert.Assert(t, ctx.getApplication(appID) == nil)

	// update orphan pods
	pod1Upd := pod1.DeepCopy()
	pod1Upd.Status.Phase = v1.PodRunning
	pod2Upd := pod2.DeepCopy()
	pod2Upd.Status.Phase = v1.PodRunning

	ctx.UpdatePod(pod1, pod1Upd)
	assert.Assert(t, ctx.getApplication(appID) == nil)
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(pod1UID), "Yunikorn pod is not orphan after update")
	assert.Equal(t, v1.PodRunning, ctx.schedulerCache.GetPod(pod1UID).Status.Phase, "pod has not been updated in the cache")
	assert.Assert(t, !update.Load(), "allocation update has been triggered for Yunikorn orphan pod")

	ctx.UpdatePod(pod2, pod2Upd)
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(podForeignUID), "foreign pod is not orphan after update")
	assert.Equal(t, v1.PodRunning, ctx.schedulerCache.GetPod(podForeignUID).Status.Phase, "foreign pod has not been updated in the cache")
	assert.Assert(t, !update.Load(), "allocation update has been triggered for foreign orphan pod")
}

func TestOrphanPodDelete(t *testing.T) {
	ctx, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	var taskEventSent atomic.Bool
	dispatcher.RegisterEventHandler("TestTaskHandler", dispatcher.EventTypeTask, func(obj interface{}) {
		taskEventSent.Store(true)
	})
	var request *si.AllocationRequest
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(r *si.AllocationRequest) error {
		request = r
		return nil
	})
	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})

	// add pods w/o node & check orphan status
	pod1 := newPodHelper(podName1, namespace, pod1UID, Host1, appID, v1.PodPending)
	pod2 := newPodHelper(podForeignName, namespace, podForeignUID, Host1, "", v1.PodPending)
	pod2.Spec.SchedulerName = ""
	ctx.AddPod(pod1)
	ctx.AddPod(pod2)
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(pod1UID), "Yunikorn pod is not orphan")
	assert.Assert(t, ctx.schedulerCache.IsPodOrphaned(podForeignUID), "foreign pod is not orphan")
	assert.Assert(t, ctx.getApplication(appID) == nil)

	// add a node with pod - this creates the application object
	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      Host2,
			Namespace: "default",
			UID:       uid1,
		},
	}
	ctx.addNode(&node)
	pod3 := newPodHelper(podName2, namespace, pod2UID, Host2, appID, v1.PodPending)
	ctx.AddPod(pod3)
	assert.Assert(t, !ctx.schedulerCache.IsPodOrphaned(pod2UID), "Yunikorn pod is orphan")
	assert.Assert(t, ctx.getApplication(appID) != nil)

	// delete orphan YK pod
	ctx.DeletePod(pod1)
	err := utils.WaitForCondition(taskEventSent.Load, 100*time.Millisecond, time.Second)
	assert.NilError(t, err)

	// delete foreign pod
	ctx.DeletePod(pod2)
	assert.Assert(t, request != nil)
	assert.Assert(t, request.Releases != nil)
	assert.Equal(t, 1, len(request.Releases.AllocationsToRelease))
	assert.Equal(t, podForeignUID, request.Releases.AllocationsToRelease[0].AllocationKey)
}

func TestTaskRemoveOnCompletion(t *testing.T) {
	context := initContextForTest()
	dispatcher.Start()
	dispatcher.RegisterEventHandler("TestAppHandler", dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler("TestTaskHandler", dispatcher.EventTypeTask, context.TaskEventHandler())
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	app := context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          testUser,
			Tags:          nil,
		},
	})

	task := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID1,
			Pod:           newPodHelper(podName1, namespace, pod1UID, fakeNodeName, appID, v1.PodRunning),
		},
	})

	// task gets scheduled
	app.SetState("Running")
	app.Schedule()
	err := utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Scheduling
	}, 100*time.Millisecond, time.Second)
	assert.NilError(t, err)

	// mark completion
	context.notifyTaskComplete(app, taskUID1)
	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Completed
	}, 100*time.Millisecond, time.Second)
	assert.NilError(t, err)

	// check removal
	app.Schedule()
	appTask := app.GetTask(taskUID1)
	assert.Assert(t, appTask == nil)
}

func TestAssumePod(t *testing.T) {
	context := initAssumePodTest(test.NewVolumeBinderMock())
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod(pod1UID, fakeNodeName)
	assert.NilError(t, err)
	assert.Assert(t, context.schedulerCache.ArePodVolumesAllBound(pod1UID))
	assumedPod := context.schedulerCache.GetPod(pod1UID)
	assert.Assert(t, assumedPod != nil, "pod not found in cache")
	assert.Equal(t, assumedPod.Spec.NodeName, fakeNodeName)
	assert.Assert(t, context.schedulerCache.IsAssumedPod(pod1UID))
}

func TestAssumePod_GetPodVolumeClaimsError(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	const errMsg = "error getting volume claims"
	binder.EnableVolumeClaimsError(errMsg)
	context := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod(pod1UID, fakeNodeName)
	assert.Error(t, err, errMsg)
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(pod1UID))
	podInCache := context.schedulerCache.GetPod(pod1UID)
	assert.Assert(t, podInCache != nil, "pod not found in cache")
	assert.Equal(t, podInCache.Spec.NodeName, "", "NodeName in pod spec was set unexpectedly")
}

func TestAssumePod_FindPodVolumesError(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	const errMsg = "error getting pod volumes"
	binder.EnableFindPodVolumesError(errMsg)
	context := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod(pod1UID, fakeNodeName)
	assert.Error(t, err, errMsg)
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(pod1UID))
	podInCache := context.schedulerCache.GetPod(pod1UID)
	assert.Assert(t, podInCache != nil, "pod not found in cache")
	assert.Equal(t, podInCache.Spec.NodeName, "", "NodeName in pod spec was set unexpectedly")
}

func TestAssumePod_ConflictingVolumes(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	binder.SetConflictReasons("reason1", "reason2")
	context := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod(pod1UID, fakeNodeName)
	assert.Error(t, err, "pod pod1 has conflicting volume claims: reason1, reason2")
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(pod1UID))
	podInCache := context.schedulerCache.GetPod(pod1UID)
	assert.Assert(t, podInCache != nil, "pod not found in cache")
	assert.Equal(t, podInCache.Spec.NodeName, "", "NodeName in pod spec was set unexpectedly")
}

func TestAssumePod_AssumePodVolumesError(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	const errMsg = "error assuming pod volumes"
	binder.SetAssumePodVolumesError(errMsg)
	context := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod(pod1UID, fakeNodeName)
	assert.Error(t, err, errMsg)
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(pod1UID))
	podInCache := context.schedulerCache.GetPod(pod1UID)
	assert.Assert(t, podInCache != nil, "pod not found in cache")
	assert.Equal(t, podInCache.Spec.NodeName, "", "NodeName in pod spec was set unexpectedly")
}

func TestAssumePod_PodNotFound(t *testing.T) {
	context := initAssumePodTest(nil)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod("nonexisting", fakeNodeName)
	assert.NilError(t, err)
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(pod1UID))
	podInCache := context.schedulerCache.GetPod(pod1UID)
	assert.Assert(t, podInCache != nil)
	assert.Equal(t, podInCache.Spec.NodeName, "", "NodeName in pod spec was set unexpectedly")
}

// TestOriginatorPodAfterRestart Test to ensure originator pod remains same even after restart. After restart, ordering of pods may change which can lead to
// incorrect originator pod selection. Instead of doing actual restart, create a situation where in pods are being processed in any random order.
// For example, placeholders are processed first and then real driver pod.
// Ensure ordering of pods doesn't have any impact on the originator.
func TestOriginatorPodAfterRestart(t *testing.T) {
	context := initContextForTest()
	controller := false
	blockOwnerDeletion := true
	ref := apis.OwnerReference{
		APIVersion:         "v1",
		Kind:               "Pod",
		Name:               "originator-01",
		UID:                uid1,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
	ownerRefs := []apis.OwnerReference{ref}

	// Real driver pod
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "originator-01",
			UID:  uid1,
			Labels: map[string]string{
				"applicationId": "spark-app-01",
				"queue":         queueNameA,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}

	// Placeholder pod 1
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "placeholder-01",
			UID:  "placeholder-01",
			Labels: map[string]string{
				"applicationId": "spark-app-01",
				"queue":         queueNameA,
			},
			Annotations: map[string]string{
				constants.AnnotationPlaceholderFlag: "true",
			},
			OwnerReferences: ownerRefs, // Add owner references because every ph reuse the app placeholder owner references.
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}

	// Placeholder pod 1
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "placeholder-02",
			UID:  "placeholder-02",
			Labels: map[string]string{
				"applicationId": "spark-app-01",
				"queue":         queueNameA,
			},
			Annotations: map[string]string{
				constants.AnnotationPlaceholderFlag: "true",
			},
			OwnerReferences: ownerRefs, // Add owner references because every ph reuse the app placeholder owner references.
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}

	// Add the ph pods first and then real driver pod at the last
	context.AddPod(pod3)
	context.AddPod(pod2)
	context.AddPod(pod1)

	app := context.getApplication("spark-app-01")
	assert.Equal(t, app.originatingTask.taskID, uid1)
	assert.Equal(t, len(app.GetPlaceHolderTasks()), 2)
	assert.Equal(t, len(app.GetAllocatedTasks()), 0)
}

func initAssumePodTest(binder *test.VolumeBinderMock) *Context {
	context, apiProvider := initContextAndAPIProviderForTest()
	if binder != nil {
		setVolumeBinder(context, binder)
	}
	dispatcher.Start()
	dispatcher.RegisterEventHandler("TestAppHandler", dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler("TestTaskHandler", dispatcher.EventTypeTask, context.TaskEventHandler())
	apiProvider.MockSchedulerAPIUpdateNodeFn(func(request *si.NodeRequest) error {
		for _, node := range request.Nodes {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.NodeID,
				Event:  NodeAccepted,
			})
		}
		return nil
	})
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          testUser,
			Tags:          nil,
		},
	})
	pod := newPodHelper(podName1, namespace, pod1UID, "", appID, v1.PodRunning)
	context.AddPod(pod)
	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      fakeNodeName,
			Namespace: "default",
			UID:       uid1,
		},
	}
	context.addNode(&node)

	return context
}

func waitForNodeAcceptedEvent(recorder *k8sEvents.FakeRecorder) error {
	// fetch the "node accepted" event
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				log.Log(log.Test).Info(event)
				if strings.Contains(event, "accepted by the scheduler") {
					return true
				}
			default:
				return false
			}
		}
	}, 10*time.Millisecond, time.Second)
	return err
}

func nodeForTest(nodeID, memory, cpu string) *v1.Node {
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = resource.MustParse(memory)
	resourceList[v1.ResourceName("cpu")] = resource.MustParse(cpu)
	return &v1.Node{
		TypeMeta: apis.TypeMeta{
			Kind:       "Node",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      nodeID,
			Namespace: "default",
			UID:       uid1,
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
	}
}

func foreignPod(podName, memory, cpu string) *v1.Pod {
	containers := make([]v1.Container, 0)
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceMemory] = resource.MustParse(memory)
	c1Resources[v1.ResourceCPU] = resource.MustParse(cpu)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})

	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      podName,
			UID:       types.UID(podName),
			Namespace: "testNamespace",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

func TestRegisterPods(t *testing.T) {
	context := initContextForTest()

	pods, err := context.registerPods()
	assert.NilError(t, err, "register pods with empty setup should not fail")
	assert.Equal(t, len(pods), 0, "should have returned an empty pod list")

	var api *client.MockedAPIProvider
	switch v := context.apiProvider.(type) {
	case *client.MockedAPIProvider:
		api = v
	default:
		t.Fatalf("api type not recognized")
	}
	pod1 := newPodHelper(appID1, namespace, uid1, nodeName1, appID1, v1.PodRunning)
	pod2 := newPodHelper(appID2, namespace, uid2, nodeName1, appID2, v1.PodRunning)
	pod3 := newPodHelper(appID3, namespace, uid3, nodeName1, appID3, v1.PodSucceeded)
	pod4 := newPodHelper(appID4, namespace, uid4, nodeName1, appID4, v1.PodRunning)

	api.GetPodListerMock().AddPod(pod1)
	api.GetPodListerMock().AddPod(pod2)
	api.GetPodListerMock().AddPod(pod3)
	api.GetPodListerMock().AddPod(pod4)

	pods, err = context.registerPods()
	assert.NilError(t, err, "register pods should not have failed")
	assert.Assert(t, assertListerPods(pods, 3), "should have returned 3 running pods in the list")

	assert.Assert(t, context.schedulerCache.GetPod(string(pod1.UID)) != nil, "expected to find pod 1 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod2.UID)) != nil, "expected to find pod 2 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod3.UID)) == nil, "not expected to find pod 3 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod4.UID)) != nil, "expected to find pod 4 in cache")

	// prep for finalising the pods
	// new pod added (should be ignored)
	pod5 := newPodHelper(appID5, namespace, uid5, nodeName1, appID5, v1.PodRunning)
	api.GetPodListerMock().AddPod(pod5)
	// update pod 1 is now marked as terminated (will be removed)
	pod1.Status = v1.PodStatus{
		Phase: v1.PodSucceeded,
	}
	api.GetPodListerMock().AddPod(pod1)
	// remove pod 4 as if it was removed from K8s (will be removed)
	api.GetPodListerMock().DeletePod(pod4)

	err = context.finalizePods(pods)
	assert.NilError(t, err, "finalize pods should not have failed")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod1.UID)) == nil, "not expected to find pod 1 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod2.UID)) != nil, "expected to find pod 2 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod3.UID)) == nil, "not expected to find pod 3 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod4.UID)) == nil, "not expected to find pod 4 in cache")
	assert.Assert(t, context.schedulerCache.GetPod(string(pod5.UID)) == nil, "not expected to find pod 5 in cache")
}

func assertListerPods(pods []*v1.Pod, count int) bool {
	counted := 0
	for _, pod := range pods {
		if pod != nil {
			counted++
		}
	}
	return count == counted
}
