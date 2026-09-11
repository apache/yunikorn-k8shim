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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sEvents "k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestTaskStateTransitions(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// submit task to the scheduler-core
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Scheduling)

	// allocated
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Allocated)

	// bound
	event3 := NewBindTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event3)
	assert.NilError(t, err, "failed to handle BindTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Bound)

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
}

func TestTaskIllegalEventHandling(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// verify illegal event handling logic
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	if err == nil {
		t.Fatal("expecting an error, event AllocateTask is illegal when task is Pending")
	}

	// task state should not have changed
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)
}

//nolint:funlen
func TestReleaseTaskAllocation(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	apiProvider := mockedContext.apiProvider
	mockedApiProvider, ok := apiProvider.(*client.MockedAPIProvider)
	assert.Assert(t, ok, "expecting MockedAPIProvider")

	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "task01",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// submit task to the scheduler-core
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Scheduling)

	// allocated
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Allocated)
	// bind a task is a async process, wait for it to happen
	err = utils.WaitForCondition(
		func() bool {
			return task.GetNodeName() == "node-1"
		},
		100*time.Millisecond,
		3*time.Second,
	)
	assert.NilError(t, err, "failed to wait for allocation allocationKey being set for task")

	// bound
	event3 := NewBindTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event3)
	assert.NilError(t, err, "failed to handle BindTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Bound)

	// the mocked update function does nothing than verify the coming messages
	// this is to verify we are sending correct info to the scheduler core
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
		return nil
	})

	// complete
	task.application.sm.SetState(ApplicationStates().Running)
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerAPIUpdateAllocationCount(), int32(2))

	// New to Failed, no AllocationKey is set (only ask is released)
	task = NewTask("task01", app, mockedContext, pod)
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].TerminationType, si.TerminationType_STOPPED_BY_RM)
		return nil
	})
	err = task.handle(NewFailTaskEvent(app.applicationID, "task01", "test failure"))
	assert.NilError(t, err, "failed to handle FailTask event")

	// Scheduling to Failed, AllocationKey is set (ask+allocation are both released)
	task = NewTask("task01", app, mockedContext, pod)
	task.setAllocationKey("task01")
	task.sm.SetState(TaskStates().Scheduling)
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].TerminationType, si.TerminationType_STOPPED_BY_RM)
		return nil
	})
	err = task.handle(NewFailTaskEvent(app.applicationID, "task01", "test failure"))
	assert.NilError(t, err, "failed to handle FailTask event")
}

func TestReleaseTaskAsk(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	apiProvider := mockedContext.apiProvider
	mockedApiProvider, ok := apiProvider.(*client.MockedAPIProvider)
	if !ok {
		t.Fatal("expecting MockedAPIProvider")
	}

	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")

	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// submit task to the scheduler-core
	// the task will be at scheduling state from this point on
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Scheduling)

	// the mocked update function does nothing than verify the coming messages
	// this is to verify we are sending correct info to the scheduler core
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, task.taskID)
		return nil
	})

	// complete
	task.application.sm.SetState(ApplicationStates().Running)
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerAPIUpdateAllocationCount(), int32(2))
}

func TestCreateTask(t *testing.T) {
	time0 := time.Now()
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)

	// pod has timestamp defined
	pod0 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-00",
			UID:               "UID-00",
			CreationTimestamp: metav1.Time{Time: time0},
		},
	}

	// pod has no timestamp defined
	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-00",
			UID:  "UID-00",
		},
	}

	// make sure the time is passed in to the task
	task0 := NewTask("task00", app, mockedContext, pod0)
	assert.Equal(t, task0.createTime, time0)

	// if pod doesn't have timestamp defined, uses the default value
	task1 := NewTask("task01", app, mockedContext, pod1)
	assert.Equal(t, task1.createTime, time.Time{})
}

func TestSortTasks(t *testing.T) {
	time0 := time.Now()
	time1 := time0.Add(10 * time.Millisecond)
	time2 := time1.Add(10 * time.Millisecond)

	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)

	pod0 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-00",
			UID:               "UID-00",
			CreationTimestamp: metav1.Time{Time: time0},
		},
	}

	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-01",
			UID:               "UID-01",
			CreationTimestamp: metav1.Time{Time: time1},
		},
	}

	pod2 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-02",
			UID:               "UID-02",
			CreationTimestamp: metav1.Time{Time: time2},
		},
	}

	task0 := NewTask("task00", app, mockedContext, pod0)
	task1 := NewTask("task01", app, mockedContext, pod1)
	task2 := NewTask("task02", app, mockedContext, pod2)
	app.addTask(task0)
	app.addTask(task1)
	app.addTask(task2)

	tasks := app.GetNewTasks()
	assert.Equal(t, len(tasks), 3)
	assert.Equal(t, tasks[0], task0)
	assert.Equal(t, tasks[1], task1)
	assert.Equal(t, tasks[2], task2)
}

func TestIsTerminated(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	task := NewTask("task01", app, mockedContext, pod)
	// set task states to non-terminated
	task.sm.SetState(TaskStates().Pending)
	res := task.isTerminated()
	assert.Equal(t, res, false)

	// set task states to terminated
	task.sm.SetState(TaskStates().Failed)
	res = task.isTerminated()
	assert.Equal(t, res, true)
}

func TestSetTaskGroup(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	task := NewTask("task01", app, mockedContext, pod)
	task.setTaskGroupName("test-group")
	assert.Equal(t, task.GetTaskGroupName(), "test-group")
}

//nolint:funlen
func TestHandleSubmitTaskEvent(t *testing.T) {
	mockedContext, mockedSchedulerAPI := initContextAndAPIProviderForTest()
	var allocRequest *si.AllocationRequest
	mockedSchedulerAPI.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		allocRequest = request
		return nil
	})

	preemptNever := v1.PreemptNever
	preemptLowerPriority := v1.PreemptLowerPriority
	priorityClass := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "preempt-self-1000",
			Annotations: map[string]string{
				constants.AnnotationAllowPreemption: "true",
			},
		},
		Value:            1000,
		PreemptionPolicy: &preemptNever,
	}
	mockedContext.addPriorityClass(priorityClass)
	priorityClass2 := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "preempt-others-1001",
			Annotations: map[string]string{
				constants.AnnotationAllowPreemption: "false",
			},
		},
		Value:            1001,
		PreemptionPolicy: &preemptLowerPriority,
	}
	mockedContext.addPriorityClass(priorityClass2)
	rt := &recorderTime{
		time: int64(0),
		lock: &locking.RWMutex{},
	}
	mr := events.NewMockedRecorder()
	mr.OnEventf = func() {
		rt.lock.Lock()
		defer rt.lock.Unlock()
		rt.time++
	}
	events.SetRecorder(mr)
	defer events.SetRecorder(events.NewMockedRecorder())
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	var priority int32 = 1000
	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers:        containers,
			Priority:          &priority,
			PriorityClassName: "preempt-self-1000",
			PreemptionPolicy:  &preemptNever,
		},
	}
	var priority2 int32 = 1001
	pod2 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00002",
			UID:  "UID-00002",
			Annotations: map[string]string{
				constants.AnnotationTaskGroupName: "test-task-group",
			},
		},
		Spec: v1.PodSpec{
			Containers:        containers,
			Priority:          &priority2,
			PriorityClassName: "preempt-others-1001",
			PreemptionPolicy:  &preemptLowerPriority,
		},
	}
	appID := "app-test-001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, mockedSchedulerAPI.GetAPIs().SchedulerAPI)
	task1 := NewTask("task01", app, mockedContext, pod1)
	task2 := NewTask("task02", app, mockedContext, pod2)
	task1.sm.SetState(TaskStates().Pending)
	task2.sm.SetState(TaskStates().Pending)
	// pod without taskGroup name
	event1 := NewSubmitTaskEvent(app.applicationID, task1.taskID)
	err := task1.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Scheduling)
	assert.Equal(t, rt.time, int64(1))
	assert.Assert(t, allocRequest != nil)
	assert.Equal(t, len(allocRequest.Allocations), 1)
	assert.Equal(t, allocRequest.Allocations[0].Priority, int32(1000))
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy != nil)
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptSelf)
	assert.Assert(t, !allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptOther)
	allocRequest = nil
	rt.time = 0
	// pod with taskGroup name
	event2 := NewSubmitTaskEvent(app.applicationID, task2.taskID)
	err = task2.handle(event2)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task2.GetTaskState(), TaskStates().Scheduling)
	assert.Equal(t, rt.time, int64(2))
	assert.Assert(t, allocRequest != nil)
	assert.Equal(t, len(allocRequest.Allocations), 1)
	assert.Equal(t, allocRequest.Allocations[0].Priority, int32(1001))
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy != nil)
	assert.Assert(t, !allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptSelf)
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptOther)
}

func TestSimultaneousTaskCompleteAndAllocate(t *testing.T) {
	const (
		podUID    = "UID-00001"
		appID     = "app-test-001"
		queueName = "root.abc"
	)
	mockedContext := initContextForTest()
	mockedAPIProvider, ok := mockedContext.apiProvider.(*client.MockedAPIProvider)
	assert.Equal(t, ok, true)

	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})

	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
			UID:  podUID,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// simulate app has one task waiting for core's allocation
	app := NewApplication(appID, queueName, "user", testGroups, map[string]string{}, mockedAPIProvider.GetAPIs().SchedulerAPI)
	task1 := NewTask(podUID, app, mockedContext, pod1)
	task1.sm.SetState(TaskStates().Scheduling)

	// notify task complete
	// because the task is in Scheduling state,
	// here we expect to trigger a UpdateRequest that contains a releaseAllocationAsk request
	mockedAPIProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Equal(t, len(request.Releases.AllocationsToRelease), 1,
			"allocationsToRelease is not in the expected length")
		askToRelease := request.Releases.AllocationsToRelease[0]
		assert.Equal(t, askToRelease.ApplicationID, appID)
		assert.Equal(t, askToRelease.AllocationKey, podUID)
		return nil
	})
	ev := NewSimpleTaskEvent(appID, task1.GetTaskID(), CompleteTask)
	err := task1.handle(ev)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)

	// simulate the core responses us an allocation
	// the task is already completed, we need to make sure the allocation
	// can be released from the core to avoid resource leak
	alloc := &si.Allocation{
		AllocationKey: string(pod1.UID),
		NodeID:        "fake-node",
		ApplicationID: appID,
		PartitionName: "default",
	}
	mockedAPIProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Equal(t, len(request.Releases.AllocationsToRelease), 1,
			"allocationsToRelease is not in the expected length")
		allocToRelease := request.Releases.AllocationsToRelease[0]
		assert.Equal(t, allocToRelease.ApplicationID, alloc.ApplicationID)
		assert.Equal(t, allocToRelease.AllocationKey, alloc.AllocationKey)
		return nil
	})
	ev1 := NewAllocateTaskEvent(app.GetApplicationID(), alloc.AllocationKey, alloc.AllocationKey, alloc.NodeID)
	err = task1.handle(ev1)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)
}

func TestUpdatePodCondition(t *testing.T) {
	condition := v1.PodCondition{
		Type:   v1.ContainersReady,
		Status: v1.ConditionTrue,
		Reason: v1.PodReasonSchedulingGated,
	}

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	app := NewApplication(appID, "root.default", "user", testGroups, map[string]string{}, nil)
	task := NewTask("pod-1", app, nil, pod)
	updated, podCopy := task.UpdatePodCondition(&condition)
	assert.Equal(t, true, updated)
	assert.Equal(t, 1, len(podCopy.Status.Conditions))
	assert.Equal(t, v1.ConditionTrue, podCopy.Status.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, podCopy.Status.Conditions[0].Type)
	assert.Equal(t, v1.PodReasonSchedulingGated, podCopy.Status.Conditions[0].Reason)
	assert.Equal(t, v1.PodPending, podCopy.Status.Phase)
	assert.Equal(t, 1, len(task.podStatus.Conditions))
	assert.Equal(t, v1.ConditionTrue, task.podStatus.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, task.podStatus.Conditions[0].Type)
	assert.Equal(t, v1.PodReasonSchedulingGated, task.podStatus.Conditions[0].Reason)
	assert.Equal(t, v1.PodPending, task.podStatus.Phase)

	podWithCondition := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				condition,
			},
		},
	}
	app = NewApplication(appID, "root.default", "user", testGroups, map[string]string{}, nil)
	task = NewTask("pod-1", app, nil, podWithCondition)

	// no update
	updated, podCopy = task.UpdatePodCondition(&condition)
	assert.Equal(t, false, updated)
	assert.Equal(t, 1, len(task.podStatus.Conditions))
	assert.Equal(t, v1.ConditionTrue, task.podStatus.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, task.podStatus.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, task.podStatus.Phase)
	assert.Equal(t, v1.PodReasonSchedulingGated, task.podStatus.Conditions[0].Reason)
	assert.Equal(t, 1, len(podCopy.Status.Conditions))
	assert.Equal(t, v1.ConditionTrue, podCopy.Status.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, podCopy.Status.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, podCopy.Status.Phase)
	assert.Equal(t, v1.PodReasonSchedulingGated, podCopy.Status.Conditions[0].Reason)

	// update status & reason
	condition.Status = v1.ConditionFalse
	condition.Reason = v1.PodReasonUnschedulable
	updated, podCopy = task.UpdatePodCondition(&condition)
	assert.Equal(t, true, updated)
	assert.Equal(t, 1, len(task.podStatus.Conditions))
	assert.Equal(t, v1.ConditionFalse, task.podStatus.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, task.podStatus.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, task.podStatus.Phase)
	assert.Equal(t, v1.PodReasonUnschedulable, task.podStatus.Conditions[0].Reason)
	assert.Equal(t, 1, len(podCopy.Status.Conditions))
	assert.Equal(t, v1.ConditionFalse, podCopy.Status.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, podCopy.Status.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, podCopy.Status.Phase)
	assert.Equal(t, v1.PodReasonUnschedulable, podCopy.Status.Conditions[0].Reason)
}

//nolint:funlen
func TestCheckPodMetadataBeforeScheduling(t *testing.T) {
	app := NewApplication(appID1, "root.default", "user", testGroups, map[string]string{}, nil)

	rt := &recorderTime{
		time: int64(0),
		lock: &locking.RWMutex{},
	}
	mr := events.NewMockedRecorder()
	mr.OnEventf = func() {
		rt.lock.Lock()
		defer rt.lock.Unlock()
		rt.time++
	}
	events.SetRecorder(mr)
	defer events.SetRecorder(events.NewMockedRecorder())

	testCases := []struct {
		name                      string
		pod                       *v1.Pod
		expectedWarningEventCount int64
	}{
		{
			name: "regular",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 0,
		},
		{
			name: "inconsistent app id label",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
						constants.LabelApplicationID:          appID2,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent app id annotation",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
					},
					Annotations: map[string]string{
						constants.AnnotationApplicationID: appID2,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent queue label",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
						constants.LabelQueueName:              queueNameB,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent queue annotation",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
					},
					Annotations: map[string]string{
						constants.CanonicalLabelQueueName: queueNameB,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent app id and queue",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
						constants.LabelApplicationID:          appID2,
						constants.LabelQueueName:              queueNameB,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// reset to 0 before every iteration
			rt.time = 0
			task := NewTask("task", app, nil, tc.pod)
			task.checkPodMetadataBeforeScheduling()
			assert.Equal(t, rt.time, tc.expectedWarningEventCount)
		})
	}
}

// newRollbackTask creates a task in Scheduling state with allocationKey and nodeName set,
// ready to exercise rollbackOnAssumePodFailure.
func newRollbackTask(ctx *Context, allocationKey, nodeID string) *Task {
	app := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{},
		ctx.apiProvider.GetAPIs().SchedulerAPI)
	pod := &v1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "rollback-pod", UID: "rollback-uid"},
	}
	task := NewTask(allocationKey, app, ctx, pod)
	task.sm.SetState(TaskStates().Scheduling)
	task.allocationKey = allocationKey
	task.nodeName = nodeID
	return task
}

// TestRollbackOnAssumePodFailure_ClearsStateAndSendsRelease verifies the happy-path:
// allocationKey and nodeName are cleared, a Warning "AssumePodFailed" event is posted,
// and a SCHEDULING_FAILED_ON_RM release request is sent to the scheduler core.
func TestRollbackOnAssumePodFailure_ClearsStateAndSendsRelease(t *testing.T) {
	mockedContext, apiProvider := initContextAndAPIProviderForTest()

	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	var rollbackSent atomic.Bool
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		if request.Releases != nil {
			for _, rel := range request.Releases.AllocationsToRelease {
				if rel.TerminationType == si.TerminationType_SCHEDULING_FAILED_ON_RM &&
					rel.AllocationKey == taskUID1 {
					rollbackSent.Store(true)
				}
			}
		}
		return nil
	})

	task := newRollbackTask(mockedContext, taskUID1, fakeNodeName)
	task.rollbackOnAssumePodFailure(taskUID1, fakeNodeName)

	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared after rollback")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared after rollback")
	assert.Assert(t, rollbackSent.Load(), "SCHEDULING_FAILED_ON_RM release request should be sent to scheduler")

	assert.Equal(t, 1, len(recorder.Events), "expected one AssumePodFailed event")
	event := <-recorder.Events
	assert.Assert(t, strings.Contains(event, "AssumePodFailed"), "event should contain AssumePodFailed reason")
}

// TestRollbackOnAssumePodFailure_UpdateAllocationError verifies that if the scheduler
// API returns an error, the task state (allocationKey, nodeName) is still cleaned up
// and the function does not panic.
func TestRollbackOnAssumePodFailure_UpdateAllocationError(t *testing.T) {
	mockedContext, apiProvider := initContextAndAPIProviderForTest()
	events.SetRecorder(events.NewMockedRecorder())
	defer events.SetRecorder(events.NewMockedRecorder())

	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		return fmt.Errorf("scheduler unavailable")
	})

	task := newRollbackTask(mockedContext, taskUID1, fakeNodeName)
	// Must not panic even when UpdateAllocation fails.
	task.rollbackOnAssumePodFailure(taskUID1, fakeNodeName)

	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared even when UpdateAllocation errors")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared even when UpdateAllocation errors")
}

// TestRollbackOnAssumePodFailure_NilSchedulerAPI verifies that rollbackOnAssumePodFailure
// does not panic when the scheduler API is nil, and still clears the allocation state.
func TestRollbackOnAssumePodFailure_NilSchedulerAPI(t *testing.T) {
	mockedContext, _ := initContextAndAPIProviderForTest()
	events.SetRecorder(events.NewMockedRecorder())
	defer events.SetRecorder(events.NewMockedRecorder())

	// Simulate a context where the scheduler API is not available.
	mockedContext.apiProvider.GetAPIs().SchedulerAPI = nil

	task := newRollbackTask(mockedContext, taskUID1, fakeNodeName)
	// Must not panic with a nil SchedulerAPI.
	task.rollbackOnAssumePodFailure(taskUID1, fakeNodeName)

	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared even with nil SchedulerAPI")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared even with nil SchedulerAPI")
}

// newBindTestPod returns a minimal pod usable for exercising the bind path.
func newBindTestPod(name, uid string) *v1.Pod {
	return &v1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
	}
}

func newDeleteTaskForTest() (*Task, *client.MockedAPIProvider) {
	mockedContext, apiProvider := initContextAndAPIProviderForTest()
	app := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	task := NewTask(taskUID1, app, mockedContext, newBindTestPod("delete-task-pod", taskUID1))
	task.sm.SetState(TaskStates().Bound)
	return task, apiProvider
}

type layer3ConflictKubeClient struct {
	client.KubeClient
	deleteFn func(pod *v1.Pod) error
	getFn    func(namespace, name string) (*v1.Pod, error)
}

func (c *layer3ConflictKubeClient) Delete(pod *v1.Pod) error {
	return c.deleteFn(pod)
}

func (c *layer3ConflictKubeClient) Get(namespace, name string) (*v1.Pod, error) {
	return c.getFn(namespace, name)
}

// Use one channel so buffered notifications preserve API call order.
type layer3KubeCall struct {
	method    string
	namespace string
	name      string
	uid       types.UID
}

func assertLayer3KubeCall(t *testing.T, calls <-chan layer3KubeCall, method string, timeout time.Duration) {
	t.Helper()
	select {
	case call := <-calls:
		assert.Equal(t, call.method, method, "unexpected API call order")
		assert.Equal(t, call.namespace, "ns")
		assert.Equal(t, call.name, "victim")
		if method == "DELETE" {
			assert.Equal(t, call.uid, types.UID("A"), "DELETE targeted a replacement Pod")
		}
	case <-time.After(timeout):
		t.Fatalf("expected %s call", method)
	}
}

func newLayer3ConflictTaskForTest() (*Task, *client.MockedAPIProvider) {
	apiProvider := client.NewMockedAPIProvider(false)
	// Deletion only needs the API provider; avoid starting unrelated DRA trackers.
	mockedContext := &Context{apiProvider: apiProvider}
	app := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	pod := newBindTestPod("victim", "A")
	pod.Namespace = "ns"
	task := NewTask(string(pod.UID), app, mockedContext, pod)
	task.sm.SetState(TaskStates().Bound)
	return task, apiProvider
}

func TestDeleteTaskPodConflictWithReplacementStopsRetry(t *testing.T) {
	task, apiProvider := newLayer3ConflictTaskForTest()
	calls := make(chan layer3KubeCall, 4)
	var deleteAttempts atomic.Int32
	var getAttempts atomic.Int32
	pods := schema.GroupResource{Resource: "pods"}
	replacement := newBindTestPod("victim", "B")
	replacement.Namespace = "ns"

	baseClient := apiProvider.GetAPIs().KubeClient
	apiProvider.GetAPIs().KubeClient = &layer3ConflictKubeClient{
		KubeClient: baseClient,
		deleteFn: func(pod *v1.Pod) error {
			attempt := deleteAttempts.Add(1)
			calls <- layer3KubeCall{method: "DELETE", namespace: pod.Namespace, name: pod.Name, uid: pod.UID}
			if attempt == 1 {
				return apierrors.NewConflict(pods, pod.Name, fmt.Errorf("delete conflict"))
			}
			return nil
		},
		getFn: func(namespace, name string) (*v1.Pod, error) {
			getAttempts.Add(1)
			calls <- layer3KubeCall{method: "GET", namespace: namespace, name: name}
			return replacement.DeepCopy(), nil
		},
	}

	err := task.DeleteTaskPod()
	assert.NilError(t, err, "replacement UID should resolve the initial Conflict")
	assertLayer3KubeCall(t, calls, "DELETE", 4*deleteTaskPodRetryInitialDelay)

	assertLayer3KubeCall(t, calls, "GET", 4*deleteTaskPodRetryInitialDelay)

	// A replacement resolves the old UID's delete obligation, but ownership must
	// remain claimed so a duplicate release cannot reopen the stale DELETE.
	err = task.DeleteTaskPod()
	assert.NilError(t, err, "duplicate release should remain idempotent")
	select {
	case call := <-calls:
		t.Fatalf("resolved Conflict triggered another API call: %+v", call)
	case <-time.After(2 * deleteTaskPodRetryInitialDelay):
	}

	assert.Equal(t, int32(1), deleteAttempts.Load(), "replacement should stop DELETE re-drive")
	assert.Equal(t, int32(1), getAttempts.Load(), "Conflict should be classified with exactly one GET")
	assert.Assert(t, task.deletePodRequested.Load(), "replacement should retain delete ownership")
	assert.Equal(t, TaskStates().Bound, task.GetTaskState(), "replacement classification synthesized Task completion")
	assert.Equal(t, int32(0), apiProvider.GetSchedulerAPIUpdateAllocationCount(),
		"replacement classification synthesized a core release confirmation")
}

func TestDeleteTaskPodConflictWithNotFoundStopsRetry(t *testing.T) {
	task, apiProvider := newLayer3ConflictTaskForTest()
	calls := make(chan layer3KubeCall, 4)
	var deleteAttempts atomic.Int32
	var getAttempts atomic.Int32
	pods := schema.GroupResource{Resource: "pods"}

	baseClient := apiProvider.GetAPIs().KubeClient
	apiProvider.GetAPIs().KubeClient = &layer3ConflictKubeClient{
		KubeClient: baseClient,
		deleteFn: func(pod *v1.Pod) error {
			attempt := deleteAttempts.Add(1)
			calls <- layer3KubeCall{method: "DELETE", namespace: pod.Namespace, name: pod.Name, uid: pod.UID}
			if attempt == 1 {
				return apierrors.NewConflict(pods, pod.Name, fmt.Errorf("delete conflict"))
			}
			return nil
		},
		getFn: func(namespace, name string) (*v1.Pod, error) {
			getAttempts.Add(1)
			calls <- layer3KubeCall{method: "GET", namespace: namespace, name: name}
			return nil, apierrors.NewNotFound(pods, name)
		},
	}

	err := task.DeleteTaskPod()
	assert.NilError(t, err, "GET NotFound should resolve the initial Conflict")
	assertLayer3KubeCall(t, calls, "DELETE", 4*deleteTaskPodRetryInitialDelay)

	assertLayer3KubeCall(t, calls, "GET", 4*deleteTaskPodRetryInitialDelay)

	err = task.DeleteTaskPod()
	assert.NilError(t, err, "duplicate release should remain idempotent")
	select {
	case call := <-calls:
		t.Fatalf("resolved Conflict triggered another API call: %+v", call)
	case <-time.After(2 * deleteTaskPodRetryInitialDelay):
	}

	assert.Equal(t, int32(1), deleteAttempts.Load(), "NotFound should stop DELETE re-drive")
	assert.Equal(t, int32(1), getAttempts.Load(), "Conflict should be classified with exactly one GET")
	assert.Assert(t, task.deletePodRequested.Load(), "NotFound should retain delete ownership")
	assert.Equal(t, TaskStates().Bound, task.GetTaskState(), "NotFound classification synthesized Task completion")
	assert.Equal(t, int32(0), apiProvider.GetSchedulerAPIUpdateAllocationCount(),
		"NotFound classification synthesized a core release confirmation")
}

func TestDeleteTaskPodConflictWithOriginalUIDRetries(t *testing.T) {
	original := newBindTestPod("victim", "A")
	original.Namespace = "ns"
	testDeleteTaskPodConflictAllowsRetry(t, original, nil)
}

func TestDeleteTaskPodConflictWithGetErrorRetries(t *testing.T) {
	testDeleteTaskPodConflictAllowsRetry(t, nil, apierrors.NewServiceUnavailable("classification GET failed"))
}

func TestDeleteTaskPodRetryWorkerConflictWithReplacementStopsRetry(t *testing.T) {
	task, apiProvider := newLayer3ConflictTaskForTest()
	calls := make(chan layer3KubeCall, 4)
	allowGetReturn := make(chan struct{})
	getReturning := make(chan struct{})
	var releaseGetOnce sync.Once
	releaseGet := func() { releaseGetOnce.Do(func() { close(allowGetReturn) }) }
	t.Cleanup(releaseGet)
	var deleteAttempts atomic.Int32
	var getAttempts atomic.Int32
	pods := schema.GroupResource{Resource: "pods"}
	replacement := newBindTestPod("victim", "B")
	replacement.Namespace = "ns"

	baseClient := apiProvider.GetAPIs().KubeClient
	apiProvider.GetAPIs().KubeClient = &layer3ConflictKubeClient{
		KubeClient: baseClient,
		deleteFn: func(pod *v1.Pod) error {
			attempt := deleteAttempts.Add(1)
			calls <- layer3KubeCall{method: "DELETE", namespace: pod.Namespace, name: pod.Name, uid: pod.UID}
			switch attempt {
			case 1:
				return apierrors.NewServiceUnavailable("initial DELETE failed")
			case 2:
				return apierrors.NewConflict(pods, pod.Name, fmt.Errorf("delete conflict"))
			default:
				return nil
			}
		},
		getFn: func(namespace, name string) (*v1.Pod, error) {
			attempt := getAttempts.Add(1)
			calls <- layer3KubeCall{method: "GET", namespace: namespace, name: name}
			if attempt == 1 {
				<-allowGetReturn
				defer close(getReturning)
			}
			return replacement.DeepCopy(), nil
		},
	}

	err := task.DeleteTaskPod()
	assert.Assert(t, apierrors.IsServiceUnavailable(err), "initial DELETE should return ServiceUnavailable")
	assertLayer3KubeCall(t, calls, "DELETE", 4*deleteTaskPodRetryInitialDelay)

	assertLayer3KubeCall(t, calls, "DELETE", 4*deleteTaskPodRetryInitialDelay)

	assertLayer3KubeCall(t, calls, "GET", 6*deleteTaskPodRetryInitialDelay)

	releaseGet()
	select {
	case <-getReturning:
	case <-time.After(4 * deleteTaskPodRetryInitialDelay):
		t.Fatal("classification GET did not reach its return")
	}

	// The handshake marks the callback's return, not worker exit. Observe for
	// unexpected API calls while the worker processes the replacement result.
	select {
	case call := <-calls:
		t.Fatalf("resolved Conflict triggered another API call: %+v", call)
	case <-time.After(4 * deleteTaskPodRetryInitialDelay):
	}

	assert.Equal(t, int32(2), deleteAttempts.Load(), "replacement should stop retry worker after its Conflict")
	assert.Equal(t, int32(1), getAttempts.Load(), "retry-worker Conflict should be classified with exactly one GET")
	assert.Assert(t, task.deletePodRequested.Load(), "replacement should retain retry-worker delete ownership")
	assert.Equal(t, TaskStates().Bound, task.GetTaskState(), "retry-worker reconciliation synthesized Task completion")
	assert.Equal(t, int32(0), apiProvider.GetSchedulerAPIUpdateAllocationCount(),
		"retry-worker reconciliation synthesized a core release confirmation")
}

func testDeleteTaskPodConflictAllowsRetry(t *testing.T, getPod *v1.Pod, getErr error) {
	t.Helper()
	task, apiProvider := newLayer3ConflictTaskForTest()
	calls := make(chan layer3KubeCall, 4)
	allowRetryReturn := make(chan struct{})
	retryReturning := make(chan struct{})
	var releaseRetryOnce sync.Once
	releaseRetry := func() { releaseRetryOnce.Do(func() { close(allowRetryReturn) }) }
	t.Cleanup(releaseRetry)
	var deleteAttempts atomic.Int32
	var getAttempts atomic.Int32
	pods := schema.GroupResource{Resource: "pods"}

	baseClient := apiProvider.GetAPIs().KubeClient
	apiProvider.GetAPIs().KubeClient = &layer3ConflictKubeClient{
		KubeClient: baseClient,
		deleteFn: func(pod *v1.Pod) error {
			attempt := deleteAttempts.Add(1)
			calls <- layer3KubeCall{method: "DELETE", namespace: pod.Namespace, name: pod.Name, uid: pod.UID}
			if attempt == 1 {
				return apierrors.NewConflict(pods, pod.Name, fmt.Errorf("delete conflict"))
			}
			if attempt == 2 {
				<-allowRetryReturn
				defer close(retryReturning)
			}
			return nil
		},
		getFn: func(namespace, name string) (*v1.Pod, error) {
			getAttempts.Add(1)
			calls <- layer3KubeCall{method: "GET", namespace: namespace, name: name}
			if getPod == nil {
				return nil, getErr
			}
			return getPod.DeepCopy(), getErr
		},
	}

	err := task.DeleteTaskPod()
	assert.Assert(t, apierrors.IsConflict(err), "unresolved initial DELETE should return Conflict")
	assertLayer3KubeCall(t, calls, "DELETE", 4*deleteTaskPodRetryInitialDelay)

	assertLayer3KubeCall(t, calls, "GET", 4*deleteTaskPodRetryInitialDelay)

	assertLayer3KubeCall(t, calls, "DELETE", 4*deleteTaskPodRetryInitialDelay)

	releaseRetry()
	select {
	case <-retryReturning:
	case <-time.After(4 * deleteTaskPodRetryInitialDelay):
		t.Fatal("retry DELETE did not reach its return")
	}

	// The callback handshake does not join the worker. Check for further API
	// calls while it processes the successful DELETE result.
	select {
	case call := <-calls:
		t.Fatalf("successful retry triggered another API call: %+v", call)
	case <-time.After(4 * deleteTaskPodRetryInitialDelay):
	}

	assert.Equal(t, int32(2), deleteAttempts.Load(), "unresolved Conflict should allow one automatic retry")
	assert.Equal(t, int32(1), getAttempts.Load(), "Conflict should be classified with exactly one GET")
	assert.Assert(t, task.deletePodRequested.Load(), "unresolved Conflict should retain delete ownership")
	assert.Equal(t, TaskStates().Bound, task.GetTaskState(), "Conflict reconciliation synthesized Task completion")
	assert.Equal(t, int32(0), apiProvider.GetSchedulerAPIUpdateAllocationCount(),
		"Conflict reconciliation synthesized a core release confirmation")
}

func TestDeleteTaskPodPermanentErrorReleasesOwnership(t *testing.T) {
	pods := schema.GroupResource{Resource: "pods"}
	tests := []struct {
		name string
		err  error
	}{
		{name: "Forbidden", err: apierrors.NewForbidden(pods, "delete-task-pod", fmt.Errorf("forbidden"))},
		{name: "Invalid", err: apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "delete-task-pod", nil)},
		{name: "BadRequest", err: apierrors.NewBadRequest("bad delete request")},
		{name: "MethodNotSupported", err: apierrors.NewMethodNotSupported(pods, "delete")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, apiProvider := newDeleteTaskForTest()
			var attempts atomic.Int32
			unexpectedDelete := make(chan struct{}, 1)
			apiProvider.MockDeleteFn(func(_ *v1.Pod) error {
				if attempts.Add(1) == 1 {
					return tt.err
				}
				select {
				case unexpectedDelete <- struct{}{}:
				default:
				}
				return nil
			})

			err := task.DeleteTaskPod()
			assert.Assert(t, err != nil, "first DELETE should return the permanent error")
			assert.Equal(t, int32(1), attempts.Load(), "permanent error should not start an automatic retry")
			assert.Assert(t, !shouldRetryDeleteTaskPod(err), "permanent error was classified as retryable")
			assert.Assert(t, !task.deletePodRequested.Load(), "permanent error retained delete ownership")
			select {
			case <-unexpectedDelete:
				t.Fatal("permanent error triggered an automatic retry")
			case <-time.After(2 * deleteTaskPodRetryInitialDelay):
			}

			err = task.DeleteTaskPod()
			assert.NilError(t, err, "later explicit DELETE should acquire released ownership")
			assert.Equal(t, int32(2), attempts.Load(), "later explicit DELETE was suppressed")
			assert.Assert(t, task.deletePodRequested.Load(), "successful DELETE should retain ownership")
		})
	}

	t.Run("RetryWorker", func(t *testing.T) {
		task, apiProvider := newDeleteTaskForTest()
		var attempts atomic.Int32
		workerPermanentErrorReturned := make(chan struct{})
		apiProvider.MockDeleteFn(func(_ *v1.Pod) error {
			switch attempts.Add(1) {
			case 1:
				return apierrors.NewServiceUnavailable("transient delete failure")
			case 2:
				close(workerPermanentErrorReturned)
				return apierrors.NewForbidden(pods, "delete-task-pod", fmt.Errorf("forbidden"))
			default:
				return nil
			}
		})

		err := task.DeleteTaskPod()
		assert.Assert(t, apierrors.IsServiceUnavailable(err), "first DELETE should return ServiceUnavailable")
		select {
		case <-workerPermanentErrorReturned:
		case <-time.After(time.Second):
			t.Fatal("retry worker did not receive the permanent error")
		}
		err = utils.WaitForCondition(func() bool {
			return !task.deletePodRequested.Load()
		}, time.Millisecond, time.Second)
		assert.NilError(t, err, "retry worker did not release delete ownership")
		assert.Equal(t, int32(2), attempts.Load(), "permanent worker error triggered another automatic retry")

		err = task.DeleteTaskPod()
		assert.NilError(t, err, "later explicit DELETE should acquire ownership released by the worker")
		assert.Equal(t, int32(3), attempts.Load(), "later explicit DELETE was suppressed after worker failure")
		assert.Assert(t, task.deletePodRequested.Load(), "successful DELETE should retain ownership")
	})
}

func TestDeleteTaskPodDuplicateCallsUseSingleOwner(t *testing.T) {
	task, apiProvider := newDeleteTaskForTest()
	var attempts atomic.Int32
	secondDeleteStarted := make(chan struct{})
	allowSecondDeleteToFinish := make(chan struct{})
	unexpectedDelete := make(chan struct{}, 1)
	var closeSecondDeleteGate sync.Once
	defer closeSecondDeleteGate.Do(func() { close(allowSecondDeleteToFinish) })
	apiProvider.MockDeleteFn(func(_ *v1.Pod) error {
		switch attempts.Add(1) {
		case 1:
			return apierrors.NewServiceUnavailable("transient delete failure")
		case 2:
			close(secondDeleteStarted)
			<-allowSecondDeleteToFinish
			return nil
		default:
			select {
			case unexpectedDelete <- struct{}{}:
			default:
			}
			return nil
		}
	})

	err := task.DeleteTaskPod()
	assert.Assert(t, apierrors.IsServiceUnavailable(err), "first DELETE should return ServiceUnavailable")
	select {
	case <-secondDeleteStarted:
	case <-time.After(time.Second):
		t.Fatal("retry owner did not issue the second DELETE")
	}

	duplicateResults := make(chan error, 3)
	for i := 0; i < cap(duplicateResults); i++ {
		go func() {
			duplicateResults <- task.DeleteTaskPod()
		}()
	}
	for i := 0; i < cap(duplicateResults); i++ {
		select {
		case duplicateErr := <-duplicateResults:
			assert.NilError(t, duplicateErr, "duplicate idempotent delete command should be accepted")
		case <-time.After(time.Second):
			t.Fatal("duplicate DELETE call blocked behind the active owner")
		}
	}
	assert.Equal(t, int32(2), attempts.Load(), "duplicate call issued an independent DELETE")

	// Give any incorrectly created duplicate workers enough time to reach the
	// blocked client call before releasing the legitimate owner.
	select {
	case <-unexpectedDelete:
		t.Fatal("duplicate call created an independent delete worker")
	case <-time.After(2 * deleteTaskPodRetryInitialDelay):
	}
	closeSecondDeleteGate.Do(func() { close(allowSecondDeleteToFinish) })
	apiProvider.GetAPIs().KubeClient.GetClientSet()
	select {
	case <-unexpectedDelete:
		t.Fatal("duplicate delete worker issued a later DELETE")
	case <-time.After(2 * deleteTaskPodRetryInitialDelay):
	}
	assert.Equal(t, int32(2), attempts.Load(), "more than one delete owner issued retries")
}

func TestDeleteTaskPodTerminalTaskStopsRetry(t *testing.T) {
	task, apiProvider := newDeleteTaskForTest()
	var attempts atomic.Int32
	retryStarted := make(chan struct{}, 1)
	apiProvider.MockDeleteFn(func(_ *v1.Pod) error {
		if attempts.Add(1) == 1 {
			return apierrors.NewServiceUnavailable("transient delete failure")
		}
		retryStarted <- struct{}{}
		return nil
	})

	err := task.DeleteTaskPod()
	assert.Assert(t, apierrors.IsServiceUnavailable(err), "first DELETE should return ServiceUnavailable")
	task.sm.SetState(TaskStates().Completed)
	select {
	case <-retryStarted:
		t.Fatal("terminal Task issued another DELETE")
	case <-time.After(2 * deleteTaskPodRetryInitialDelay):
	}
	assert.Equal(t, int32(1), attempts.Load(), "terminal Task did not stop deletion retry")
	assert.Assert(t, task.deletePodRequested.Load(), "terminal Task should retain delete ownership")
}

// setShortBindBackoff shrinks the bind retry backoff so tests do not wait for the
// production schedule, and returns a restore function.
func setShortBindBackoff(steps int) func() {
	original := retryBackoff
	retryBackoff = wait.Backoff{Steps: steps, Duration: time.Millisecond}
	return func() { retryBackoff = original }
}

// TestRescheduleOnBindFailure_ClearsStateAndReschedules verifies that a bind failure
// clears the allocation, moves the task back to Scheduling, and sends a
// SCHEDULING_FAILED_ON_RM release so the core can re-schedule on a different node.
func TestRescheduleOnBindFailure_ClearsStateAndReschedules(t *testing.T) {
	mockedContext, apiProvider := initContextAndAPIProviderForTest()
	recorder := k8sEvents.NewFakeRecorder(1024)
	events.SetRecorder(recorder)
	defer events.SetRecorder(events.NewMockedRecorder())

	var rollbackSent atomic.Bool
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		if request.Releases != nil {
			for _, rel := range request.Releases.AllocationsToRelease {
				if rel.TerminationType == si.TerminationType_SCHEDULING_FAILED_ON_RM &&
					rel.AllocationKey == taskUID1 {
					rollbackSent.Store(true)
				}
			}
		}
		return nil
	})

	task := newRollbackTask(mockedContext, taskUID1, fakeNodeName)
	task.sm.SetState(TaskStates().Allocated)
	task.rescheduleOnBindFailure(taskUID1, fakeNodeName, "PodBindFailure", "bind failed, it will be retried")

	assert.Equal(t, TaskStates().Scheduling, task.GetTaskState(), "task should be back in Scheduling after bind failure")
	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared")
	assert.Assert(t, rollbackSent.Load(), "SCHEDULING_FAILED_ON_RM release request should be sent")
	assert.Assert(t, len(recorder.Events) >= 1, "a bind failure event should be posted")
}

// TestPostTaskAllocated_BindRetrySucceeds verifies that a transient pod bind failure is
// retried and, once it succeeds, the task stays Allocated without being rolled back.
func TestPostTaskAllocated_BindRetrySucceeds(t *testing.T) {
	mockedContext, apiProvider := initContextAndAPIProviderForTest()
	events.SetRecorder(events.NewMockedRecorder())
	defer events.SetRecorder(events.NewMockedRecorder())
	defer setShortBindBackoff(5)()

	var bindCalls atomic.Int32
	apiProvider.MockBindFn(func(_ *v1.Pod, _ string) error {
		if bindCalls.Add(1) < 3 {
			return fmt.Errorf("transient bind error")
		}
		return nil
	})

	app := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	task := NewTask(taskUID1, app, mockedContext, newBindTestPod("bind-retry-pod", "bind-retry-uid"))
	task.sm.SetState(TaskStates().Scheduling)

	err := task.handle(NewAllocateTaskEvent(app.applicationID, task.taskID, taskUID1, fakeNodeName))
	assert.NilError(t, err, "failed to handle AllocateTask event")

	err = utils.WaitForCondition(func() bool {
		return bindCalls.Load() == 3 && task.GetTaskSchedulingState() == TaskSchedAllocated
	}, 10*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "pod bind did not succeed after retries")
	assert.Equal(t, TaskStates().Allocated, task.GetTaskState(), "task should remain Allocated after a successful bind")
	assert.Equal(t, fakeNodeName, task.GetNodeName(), "node assignment should be kept after a successful bind")
}

// TestPostTaskAllocated_BindExhaustedReschedules verifies that when pod binding keeps
// failing, the allocation is rolled back and the task returns to Scheduling.
func TestPostTaskAllocated_BindExhaustedReschedules(t *testing.T) {
	mockedContext, apiProvider := initContextAndAPIProviderForTest()
	events.SetRecorder(events.NewMockedRecorder())
	defer events.SetRecorder(events.NewMockedRecorder())
	defer setShortBindBackoff(2)()

	apiProvider.MockBindFn(func(_ *v1.Pod, _ string) error {
		return fmt.Errorf("permanent bind error")
	})

	var rollbackSent atomic.Bool
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		if request.Releases != nil {
			for _, rel := range request.Releases.AllocationsToRelease {
				if rel.TerminationType == si.TerminationType_SCHEDULING_FAILED_ON_RM {
					rollbackSent.Store(true)
				}
			}
		}
		return nil
	})

	app := NewApplication(appID1, queueNameA, testUser, testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	task := NewTask(taskUID1, app, mockedContext, newBindTestPod("bind-fail-pod", "bind-fail-uid"))
	task.sm.SetState(TaskStates().Scheduling)

	err := task.handle(NewAllocateTaskEvent(app.applicationID, task.taskID, taskUID1, fakeNodeName))
	assert.NilError(t, err, "failed to handle AllocateTask event")

	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Scheduling
	}, 10*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "task should be moved back to Scheduling after bind exhaustion")
	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared after rollback")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared after rollback")
	assert.Assert(t, rollbackSent.Load(), "SCHEDULING_FAILED_ON_RM release request should be sent")
}

// TestPostTaskAllocated_VolumeBindExhaustedReschedules verifies that when volume binding
// keeps failing, the allocation is rolled back and the task returns to Scheduling.
func TestPostTaskAllocated_VolumeBindExhaustedReschedules(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	binder.SetAllBound(false)
	binder.SetPodVolumes(&volumebinding.PodVolumes{})
	binder.EnableBindPodVolumesError("permanent volume bind error")
	mockedContext := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()
	defer setShortBindBackoff(2)()

	apiProvider := mockedContext.apiProvider.(*client.MockedAPIProvider) //nolint:errcheck
	var rollbackSent atomic.Bool
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		if request.Releases != nil {
			for _, rel := range request.Releases.AllocationsToRelease {
				if rel.TerminationType == si.TerminationType_SCHEDULING_FAILED_ON_RM {
					rollbackSent.Store(true)
				}
			}
		}
		return nil
	})

	// assume the pod so it is present in the scheduler cache with volumes not fully bound
	err := mockedContext.AssumePod(pod1UID, fakeNodeName)
	assert.NilError(t, err, "failed to assume pod")

	app := NewApplication(appID, queue, testUser, testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	task := NewTask(pod1UID, app, mockedContext, newBindTestPod(podName1, pod1UID))
	task.sm.SetState(TaskStates().Scheduling)

	err = task.handle(NewAllocateTaskEvent(app.applicationID, task.taskID, pod1UID, fakeNodeName))
	assert.NilError(t, err, "failed to handle AllocateTask event")

	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Scheduling
	}, 10*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "task should be moved back to Scheduling after volume bind exhaustion")
	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared after rollback")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared after rollback")
	assert.Assert(t, rollbackSent.Load(), "SCHEDULING_FAILED_ON_RM release request should be sent")
}

// TestPostTaskAllocated_VolumeBindNoRetryOnBindingVolumesError verifies that when volume binding
// fails with a "binding volumes:" error, retries are stopped immediately and the allocation is rolled back.
func TestPostTaskAllocated_VolumeBindNoRetryOnBindingVolumesError(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	binder.SetAllBound(false)
	binder.SetPodVolumes(&volumebinding.PodVolumes{})
	binder.EnableBindPodVolumesError("binding volumes: timed out waiting for volume binding")
	mockedContext := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	// Using a long backoff to prove it doesn't wait for retries
	origBackoff := retryBackoff
	retryBackoff = wait.Backoff{
		Steps:    5,
		Duration: 10 * time.Second,
		Factor:   2,
	}
	defer func() {
		retryBackoff = origBackoff
	}()

	apiProvider := mockedContext.apiProvider.(*client.MockedAPIProvider) //nolint:errcheck
	var rollbackSent atomic.Bool
	apiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		if request.Releases != nil {
			for _, rel := range request.Releases.AllocationsToRelease {
				if rel.TerminationType == si.TerminationType_SCHEDULING_FAILED_ON_RM {
					rollbackSent.Store(true)
				}
			}
		}
		return nil
	})

	// assume the pod so it is present in the scheduler cache with volumes not fully bound
	err := mockedContext.AssumePod(pod1UID, fakeNodeName)
	assert.NilError(t, err, "failed to assume pod")

	app := NewApplication(appID, queue, testUser, testGroups, map[string]string{},
		apiProvider.GetAPIs().SchedulerAPI)
	task := NewTask(pod1UID, app, mockedContext, newBindTestPod(podName1, pod1UID))
	task.sm.SetState(TaskStates().Scheduling)

	err = task.handle(NewAllocateTaskEvent(app.applicationID, task.taskID, pod1UID, fakeNodeName))
	assert.NilError(t, err, "failed to handle AllocateTask event")

	err = utils.WaitForCondition(func() bool {
		return task.GetTaskState() == TaskStates().Scheduling
	}, 10*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "task should be moved back to Scheduling without retrying for binding volumes error")
	assert.Equal(t, int32(1), binder.GetBindCount(), "bindPodVolumes should only be called once when error starts with 'binding volumes:'")
	assert.Equal(t, "", task.GetAllocationKey(), "allocationKey should be cleared after rollback")
	assert.Equal(t, "", task.GetNodeName(), "nodeName should be cleared after rollback")
	assert.Assert(t, rollbackSent.Load(), "SCHEDULING_FAILED_ON_RM release request should be sent")
}
