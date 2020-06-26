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

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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
		"bob", map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, events.InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Pending)

	// submit task to the scheduler-core
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Scheduling)

	// allocated
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Allocated)

	// bound
	event3 := NewBindTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event3)
	assert.NilError(t, err, "failed to handle BindTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Bound)

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, events.CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
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
		"bob", map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, events.InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Pending)

	// verify illegal event handling logic
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	if err == nil {
		t.Fatal("expecting an error, event AllocateTask is illegal when task is Pending")
	}

	// task state should not have changed
	assert.Equal(t, task.GetTaskState(), events.States().Task.Pending)
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
		"bob", map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, events.InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Pending)

	// submit task to the scheduler-core
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Scheduling)

	// allocated
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Allocated)
	// bind a task is a async process, wait for it to happen
	err = common.WaitFor(100*time.Millisecond, 3*time.Second, func() bool {
		return task.getTaskAllocationUUID() == string(pod.UID)
	})
	assert.NilError(t, err, "failed to wait for allocation UUID being set for task")

	// bound
	event3 := NewBindTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event3)
	assert.NilError(t, err, "failed to handle BindTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Bound)

	// the mocked update function does nothing than verify the coming messages
	// this is to verify we are sending correct info to the scheduler core
	mockedApiProvider.MockSchedulerApiUpdateFn(func(request *si.UpdateRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		return nil
	})

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, events.CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerApiUpdateCount(), int32(2))
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
		"bob", map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, events.InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")

	assert.Equal(t, task.GetTaskState(), events.States().Task.Pending)

	// submit task to the scheduler-core
	// the task will be at scheduling state from this point on
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Scheduling)

	// the mocked update function does nothing than verify the coming messages
	// this is to verify we are sending correct info to the scheduler core
	mockedApiProvider.MockSchedulerApiUpdateFn(func(request *si.UpdateRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease == nil)
		assert.Assert(t, request.Releases.AllocationAsksToRelease != nil)
		assert.Equal(t, request.Releases.AllocationAsksToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationAsksToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationAsksToRelease[0].Allocationkey, task.taskID)
		return nil
	})

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, events.CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerApiUpdateCount(), int32(2))
}
