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
	"sync"
	"testing"
	"time"

	"gotest.tools/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sEvents "k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
	// bind a task is a async process, wait for it to happen
	err = common.WaitFor(100*time.Millisecond, 3*time.Second, func() bool {
		return task.getTaskAllocationUUID() == string(pod.UID)
	})
	assert.NilError(t, err, "failed to wait for allocation UUID being set for task")

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
		return nil
	})

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerAPIUpdateAllocationCount(), int32(2))
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		assert.Assert(t, request.Releases.AllocationsToRelease == nil)
		assert.Assert(t, request.Releases.AllocationAsksToRelease != nil)
		assert.Equal(t, request.Releases.AllocationAsksToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationAsksToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationAsksToRelease[0].AllocationKey, task.taskID)
		return nil
	})

	// complete
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:              "pod-00",
			UID:               "UID-00",
			CreationTimestamp: metav1.Time{Time: time0},
		},
	}

	// pod has no timestamp defined
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:              "pod-00",
			UID:               "UID-00",
			CreationTimestamp: metav1.Time{Time: time0},
		},
	}

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:              "pod-01",
			UID:               "UID-01",
			CreationTimestamp: metav1.Time{Time: time1},
		},
	}

	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	task := NewTask("task01", app, mockedContext, pod)
	task.setTaskGroupName("test-group")
	assert.Equal(t, task.getTaskGroupName(), "test-group")
}

func TestHandleSubmitTaskEvent(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	rt := &recorderTime{
		time: int64(0),
		lock: &sync.RWMutex{},
	}
	conf.GetSchedulerConf().SetTestMode(true)
	mr := events.NewMockedRecorder()
	mr.OnEventf = func() {
		rt.lock.Lock()
		defer rt.lock.Unlock()
		rt.time++
	}
	events.SetRecorder(mr)
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00002",
			UID:  "UID-00002",
			Annotations: map[string]string{
				constants.AnnotationTaskGroupName: "test-task-group",
			},
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	appID := "app-test-001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, mockedSchedulerAPI)
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
	rt.time = 0
	// pod with taskGroup name
	event2 := NewSubmitTaskEvent(app.applicationID, task2.taskID)
	err = task2.handle(event2)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task2.GetTaskState(), TaskStates().Scheduling)
	assert.Equal(t, rt.time, int64(2))

	// Test over, set Recorder back fake type
	events.SetRecorder(k8sEvents.NewFakeRecorder(1024))
}

func TestSimultaneousTaskCompleteAndAllocate(t *testing.T) {
	const (
		podUID         = "UID-00001"
		appID          = "app-test-001"
		queueName      = "root.abc"
		allocationUUID = "uuid-xyz"
	)
	mockedContext := initContextForTest()
	mockedAPIProvider, ok := mockedContext.apiProvider.(*client.MockedAPIProvider)
	assert.Equal(t, ok, true)

	conf.GetSchedulerConf().SetTestMode(true)
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
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
		assert.Equal(t, len(request.Releases.AllocationAsksToRelease), 1,
			"allocationAskToRelease is not in the expected length")
		assert.Equal(t, len(request.Releases.AllocationsToRelease), 0,
			"allocationsToRelease is not in the expected length")
		askToRelease := request.Releases.AllocationAsksToRelease[0]
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
		UUID:          allocationUUID,
		NodeID:        "fake-node",
		ApplicationID: appID,
		PartitionName: "default",
	}
	mockedAPIProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Equal(t, len(request.Releases.AllocationAsksToRelease), 0,
			"allocationAskToRelease is not in the expected length")
		assert.Equal(t, len(request.Releases.AllocationsToRelease), 1,
			"allocationsToRelease is not in the expected length")
		allocToRelease := request.Releases.AllocationsToRelease[0]
		assert.Equal(t, allocToRelease.ApplicationID, alloc.ApplicationID)
		assert.Equal(t, allocToRelease.UUID, alloc.UUID)
		return nil
	})
	ev1 := NewAllocateTaskEvent(app.GetApplicationID(), alloc.AllocationKey, alloc.UUID, alloc.NodeID)
	err = task1.handle(ev1)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)
}
