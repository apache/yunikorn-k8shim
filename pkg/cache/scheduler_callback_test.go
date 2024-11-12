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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sEvents "k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/predicates"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestUpdateAllocation_NewTask(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := callback.UpdateAllocation(&si.AllocationResponse{
		New: []*si.Allocation{
			{
				ApplicationID: appID,
				AllocationKey: taskUID1,
				NodeID:        fakeNodeName,
			},
		},
	})
	assert.NilError(t, err, "error updating allocation")
	assert.Assert(t, context.schedulerCache.IsAssumedPod(taskUID1))
	task := context.getTask(appID, taskUID1)
	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Bound
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "task has not transitioned to Bound state")
}

func TestUpdateAllocation_NewTask_TaskNotFound(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	pod := context.getTask(appID, taskUID1).pod
	context.RemoveTask(appID, taskUID1)
	context.DeletePod(pod)

	err := callback.UpdateAllocation(&si.AllocationResponse{
		New: []*si.Allocation{
			{
				ApplicationID: appID,
				AllocationKey: taskUID1,
				NodeID:        fakeNodeName,
			},
		},
	})
	assert.NilError(t, err, "error updating allocation")
}

func TestUpdateAllocation_NewTask_AssumePodFails(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	binder := test.NewVolumeBinderMock()
	const errMsg = "error assuming pod volumes"
	binder.SetAssumePodVolumesError(errMsg)
	setVolumeBinder(context, binder)

	err := callback.UpdateAllocation(&si.AllocationResponse{
		New: []*si.Allocation{
			{
				ApplicationID: appID,
				AllocationKey: taskUID1,
				NodeID:        fakeNodeName,
			},
		},
	})
	assert.Error(t, err, errMsg)
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(taskUID1))
	task := context.getTask(appID, taskUID1)
	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Failed
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "task has not transitioned to Failed state")
}

func TestUpdateAllocation_NewTask_PodAlreadyAssigned(t *testing.T) {
	callback, context := initCallbackTest(t, true, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := callback.UpdateAllocation(&si.AllocationResponse{
		New: []*si.Allocation{
			{
				ApplicationID: appID,
				AllocationKey: taskUID1,
				NodeID:        fakeNodeName,
			},
		},
	})
	assert.NilError(t, err, "error updating allocation")
	assert.Assert(t, context.schedulerCache.IsAssumedPod(taskUID1))
	task := context.getTask(appID, taskUID1)
	assert.Equal(t, TaskStates().Bound, task.GetTaskState())
	assert.Equal(t, fakeNodeName, task.nodeName)
	assert.Equal(t, TaskSchedAllocated, task.schedulingState)
}

func TestUpdateAllocation_AllocationRejected(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := callback.UpdateAllocation(&si.AllocationResponse{
		RejectedAllocations: []*si.RejectedAllocation{
			{
				ApplicationID: appID,
				AllocationKey: taskUID1,
			},
		},
	})
	assert.NilError(t, err, "error updating allocation")
	task := context.getTask(appID, taskUID1)
	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Failed
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "task has not transitioned to Failed state")
}

func TestUpdateAllocation_AllocationReleased(t *testing.T) {
	// release allocation where terminationType != TerminationType_STOPPED_BY_RM
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	err := context.AssumePod(taskUID1, fakeNodeName)
	assert.NilError(t, err, "could not assume pod")
	app := context.getApplication(appID)
	assert.Assert(t, app != nil)
	app.sm.SetState(ApplicationStates().Running)
	var deleteCalled atomic.Bool
	context.apiProvider.(*client.MockedAPIProvider).MockDeleteFn(func(pod *v1.Pod) error { //nolint:errcheck
		deleteCalled.Store(true)
		return nil
	})
	task := context.getTask(appID, taskUID1)
	task.allocationKey = taskUID1

	err = callback.UpdateAllocation(&si.AllocationResponse{
		Released: []*si.AllocationRelease{
			{
				ApplicationID:   appID,
				AllocationKey:   taskUID1,
				TerminationType: si.TerminationType_PREEMPTED_BY_SCHEDULER,
			},
		},
	})
	assert.NilError(t, err, "error updating allocation")
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(taskUID1))
	err = utils.WaitForCondition(deleteCalled.Load, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "pod has not been deleted")
}

func TestUpdateAllocation_AllocationReleased_StoppedByRM(t *testing.T) {
	// release allocation where terminationType == TerminationType_STOPPED_BY_RM
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	err := context.AssumePod(taskUID1, fakeNodeName)
	assert.NilError(t, err, "could not assume pod")
	app := context.getApplication(appID)
	assert.Assert(t, app != nil)
	app.sm.SetState(ApplicationStates().Running)
	var deleteCalled atomic.Bool
	context.apiProvider.(*client.MockedAPIProvider).MockDeleteFn(func(pod *v1.Pod) error { //nolint:errcheck
		deleteCalled.Store(true)
		return nil
	})

	err = callback.UpdateAllocation(&si.AllocationResponse{
		Released: []*si.AllocationRelease{
			{
				ApplicationID:   appID,
				AllocationKey:   taskUID1,
				TerminationType: si.TerminationType_STOPPED_BY_RM,
			},
		},
	})
	assert.NilError(t, err, "error updating allocation")
	assert.Assert(t, !context.schedulerCache.IsAssumedPod(taskUID1))
	err = utils.WaitForCondition(deleteCalled.Load, 10*time.Millisecond, 500*time.Millisecond)
	assert.Error(t, err, "timeout waiting for condition") // pod is not expected to be deleted
}

func TestUpdateApplication_Accepted(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	app := context.getApplication(appID)
	app.sm.SetState(ApplicationStates().Submitted)

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Accepted: []*si.AcceptedApplication{
			{
				ApplicationID: appID,
			},
		},
	})
	assert.NilError(t, err, "error updating application")
	err = utils.WaitForCondition(func() bool {
		return app.sm.Current() == ApplicationStates().Accepted
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "application has not transitioned to Accepted state")
}

func TestUpdateApplication_Rejected(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	app := context.getApplication(appID)
	app.sm.SetState(ApplicationStates().Submitted)
	NewPlaceholderManager(context.apiProvider.GetAPIs())
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Rejected: []*si.RejectedApplication{
			{
				ApplicationID: appID,
				Reason:        "test failure",
			},
		},
	})
	assert.NilError(t, err, "error updating application")
	err = utils.WaitForCondition(func() bool {
		return app.sm.Current() == ApplicationStates().Failed
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "application has not transitioned to Failed state")
	assert.Equal(t, 1, len(recorder.Events), "no K8s event received")
	event := <-recorder.Events
	assert.Assert(t, strings.Contains(event, "test failure"), "event does not contain 'test failure': %s", event)
}

func TestUpdateApplication_Completed(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Updated: []*si.UpdatedApplication{
			{
				ApplicationID: appID,
				State:         ApplicationStates().Completed,
			},
		},
	})
	assert.NilError(t, err, "error updating application")
	assert.Assert(t, context.getApplication(appID) == nil, "application was not removed")
}

func TestUpdateApplication_Resuming_AppReserving(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	app := context.getApplication(appID)
	app.sm.SetState(ApplicationStates().Reserving)

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Updated: []*si.UpdatedApplication{
			{
				ApplicationID: appID,
				State:         ApplicationStates().Resuming,
			},
		},
	})
	assert.NilError(t, err, "error updating application")
	err = utils.WaitForCondition(func() bool {
		return app.sm.Current() == ApplicationStates().Resuming
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "application has not transitioned to Resuming state")
}

func TestUpdateApplication_Resuming_AppNotReserving(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	app := context.getApplication(appID)
	app.sm.SetState(ApplicationStates().Submitted) // not in Reserving - nothing should happen

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Updated: []*si.UpdatedApplication{
			{
				ApplicationID: appID,
				State:         ApplicationStates().Resuming,
			},
		},
	})
	assert.NilError(t, err, "error updating application")
	time.Sleep(time.Second) // wait and make sure that the app is still submitted
	assert.Equal(t, ApplicationStates().Submitted, app.GetApplicationState(), "application state has been changed unexpectedly")
}

func TestUpdateApplication_Resuming_AppNotFound(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	app := context.getApplication(appID)
	app.sm.SetState(ApplicationStates().Reserving)

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Updated: []*si.UpdatedApplication{
			{
				ApplicationID: "nonExisting",
				State:         ApplicationStates().Resuming,
			},
		},
	})
	assert.NilError(t, err, "error updating application")
}

func TestUpdateApplication_Failing(t *testing.T) {
	testUpdateApplicationFailure(t, ApplicationStates().Failing)
}

func TestUpdateApplication_Failed(t *testing.T) {
	testUpdateApplicationFailure(t, ApplicationStates().Failed)
}

func testUpdateApplicationFailure(t *testing.T, state string) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	app := context.getApplication(appID)
	app.sm.SetState(ApplicationStates().Running)
	NewPlaceholderManager(context.apiProvider.GetAPIs())
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	err := callback.UpdateApplication(&si.ApplicationResponse{
		Updated: []*si.UpdatedApplication{
			{
				ApplicationID: appID,
				State:         state,
				Message:       "test failure",
			},
		},
	})
	assert.NilError(t, err, "error updating application")
	err = utils.WaitForCondition(func() bool {
		return app.sm.Current() == ApplicationStates().Failing
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err, "application has not transitioned to %s state", state)
	assert.Equal(t, 1, len(recorder.Events), "no K8s event received")
	event := <-recorder.Events
	assert.Assert(t, strings.Contains(event, "test failure"), "event does not contain 'test failure': %s", event)
}

func TestUpdateNode_Accepted(t *testing.T) {
	testUpdateNode(t, "NodeAccepted", &si.NodeResponse{
		Accepted: []*si.AcceptedNode{
			{
				NodeID: "testNode",
			},
		},
	})
}

func TestUpdateNode_Rejected(t *testing.T) {
	testUpdateNode(t, "NodeRejected", &si.NodeResponse{
		Rejected: []*si.RejectedNode{
			{
				NodeID: "testNode",
			},
		},
	})
}

func testUpdateNode(t *testing.T, expectedEvent string, response *si.NodeResponse) {
	callback, _ := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	var nodeName atomic.Value
	dispatcher.RegisterEventHandler("testNodeHandler", dispatcher.EventTypeNode, func(event interface{}) {
		nodeEvent := event.(CachedSchedulerNodeEvent) //nolint:errcheck
		if nodeEvent.GetEvent() == expectedEvent {
			nodeName.Store(nodeEvent.GetNodeID())
		}
	})

	err := callback.UpdateNode(response)
	assert.NilError(t, err, "error updating node")
	err = utils.WaitForCondition(func() bool {
		if val, ok := nodeName.Load().(string); ok {
			return val == "testNode"
		}
		return false
	}, 10*time.Millisecond, time.Second)
	assert.NilError(t, err)
}

func TestPredicates(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	context.predManager = &mockPredicateManager{}

	// pod not found
	err := callback.Predicates(&si.PredicatesArgs{AllocationKey: "unknown", NodeID: fakeNodeName, Allocate: true})
	assert.Error(t, err, "predicates were not run because pod was not found in cache")

	// pod found
	err = callback.Predicates(&si.PredicatesArgs{AllocationKey: taskUID1, NodeID: fakeNodeName, Allocate: true})
	assert.NilError(t, err)
}

func TestPreemptionPredicates(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	context.predManager = &mockPredicateManager{}

	// pod not found
	resp := callback.PreemptionPredicates(&si.PreemptionPredicatesArgs{AllocationKey: "unknown", NodeID: fakeNodeName, StartIndex: 0})
	assert.Assert(t, !resp.Success, "response should have failed")

	// pod found
	resp = callback.PreemptionPredicates(&si.PreemptionPredicatesArgs{AllocationKey: taskUID1, NodeID: fakeNodeName, StartIndex: 0, PreemptAllocationKeys: []string{taskUID1}})
	assert.Assert(t, resp.Success, "response should have succeeded")
	assert.Equal(t, int32(0), resp.Index)
}

func TestSendEvent(t *testing.T) {
	callback, _ := initCallbackTest(t, false, false)
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	callback.SendEvent([]*si.EventRecord{
		{
			Type:        si.EventRecord_REQUEST,
			ObjectID:    taskUID1,
			ReferenceID: appID,
			Message:     "request test message",
		},
		{
			Type:              si.EventRecord_NODE,
			EventChangeType:   si.EventRecord_ADD,
			EventChangeDetail: si.EventRecord_DETAILS_NONE,
			ObjectID:          fakeNodeName,
			Message:           "node test message",
		},
	})

	assert.Equal(t, 2, len(recorder.Events))
	event := <-recorder.Events
	assert.Assert(t, strings.Contains(event, "request test message"), "event does not contain 'request test message': %s", event)
	event = <-recorder.Events
	assert.Assert(t, strings.Contains(event, "node test message"), "event does not contain 'node test message': %s", event)
}

func TestUpdateContainerSchedulingState(t *testing.T) {
	callback, context := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	callback.UpdateContainerSchedulingState(&si.UpdateContainerSchedulingStateRequest{
		State:         si.UpdateContainerSchedulingStateRequest_FAILED,
		ApplicationID: appID,
		AllocationKey: taskUID1,
	})

	task := context.getTask(appID, taskUID1)
	assert.Equal(t, TaskSchedFailed, task.GetTaskSchedulingState())
}

func TestCallbackGetStateDump(t *testing.T) {
	callback, _ := initCallbackTest(t, false, false)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	stateDumpStr, err := callback.GetStateDump()
	assert.NilError(t, err, "unable to get state dump")
	stateDump := make(map[string]interface{})
	err = json.Unmarshal([]byte(stateDumpStr), &stateDump)
	assert.NilError(t, err)
	assert.Assert(t, stateDump["cache"] != nil, "no 'cache' entry in the state dump")
}

var _ predicates.PredicateManager = &mockPredicateManager{}

type mockPredicateManager struct{}

func (m *mockPredicateManager) EventsToRegister(_ framework.QueueingHintFn) []framework.ClusterEventWithHint {
	return nil
}

func (m *mockPredicateManager) Predicates(_ *v1.Pod, _ *framework.NodeInfo, _ bool) (plugin string, error error) {
	return "", nil
}

func (m *mockPredicateManager) PreemptionPredicates(_ *v1.Pod, _ *framework.NodeInfo, _ []*v1.Pod, _ int) (index int) {
	return 0
}

func initCallbackTest(t *testing.T, podAssigned, placeholder bool) (*AsyncRMCallback, *Context) {
	context, apiProvider := initContextAndAPIProviderForTest()
	dispatcher.Start()
	dispatcher.RegisterEventHandler("TestAppHandler", dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler("TestTaskHandler", dispatcher.EventTypeTask, context.TaskEventHandler())
	callback := NewAsyncRMCallback(context)
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
			Name:      fakeNodeName,
			Namespace: "default",
			UID:       "uid_0001",
		},
	}
	context.addNode(&node1)
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00001",
			UID:  taskUID1,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID,
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	if podAssigned {
		pod.Spec.NodeName = fakeNodeName
	}
	if placeholder {
		pod.Annotations[constants.AnnotationPlaceholderFlag] = constants.True
	}
	context.AddPod(pod)
	task := context.getTask(appID, taskUID1)
	assert.Assert(t, task != nil)
	task.sm.SetState(TaskStates().Scheduling)

	return callback, context
}
