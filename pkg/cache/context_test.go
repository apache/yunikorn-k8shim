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
	"testing"
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
)

func initContextForTest() *Context {
	conf.GetSchedulerConf().SetTestMode(true)
	context := NewContext(client.NewMockedAPIProvider())
	return context
}

func TestAddApplications(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications["app00001"] != nil)
	assert.Equal(t, context.applications["app00001"].GetApplicationState(), events.States().Application.New)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 0)

	// add an app but app already exists
	app := context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.other",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	assert.Assert(t, app != nil)
	assert.Equal(t, app.GetQueue(), "root.a")
}

func TestGetApplication(t *testing.T) {
	context := initContextForTest()
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00002",
			QueueName:     "root.b",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	app := context.GetApplication("app00001")
	assert.Assert(t, app != nil)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, app.GetUser(), "test-user")

	app = context.GetApplication("app00002")
	assert.Assert(t, app != nil)
	assert.Equal(t, app.GetApplicationID(), "app00002")
	assert.Equal(t, app.GetQueue(), "root.b")
	assert.Equal(t, app.GetUser(), "test-user")

	// get a non-exist application
	app = context.GetApplication("app-none-exist")
	assert.Assert(t, app == nil)
}

func TestRemoveApplication(t *testing.T) {
	// add 2 applications
	context := initContextForTest()
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00002",
			QueueName:     "root.b",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	// remove application
	// this should be successful
	err := context.RemoveApplication("app00001")
	assert.Assert(t, err == nil)

	app := context.GetApplication("app00001")
	assert.Assert(t, app == nil)

	// try remove again
	// this should fail
	err = context.RemoveApplication("app00001")
	assert.Assert(t, err != nil)

	// make sure the other app is not affected
	app = context.GetApplication("app00002")
	assert.Assert(t, app != nil)
}

func TestAddTask(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications["app00001"] != nil)
	assert.Equal(t, context.applications["app00001"].GetApplicationState(), events.States().Application.New)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), "task00001")

	// add another task
	task = context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), "task00002")

	// add a task with dup taskID
	task = context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), "task00002")

	// add a task without app's appearance
	task = context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app-non-exist",
			TaskID:        "task00003",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Assert(t, task == nil)

	// verify number of tasks in cache
	assert.Equal(t, len(context.applications["app00001"].GetNewTasks()), 2)
}

func TestRecoverTask(t *testing.T) {
	context := initContextForTest()

	const appID = "app00001"
	const queue = "root.a"
	const podUID = "task00001"
	const podName = "my-pod"

	// add a new application
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: true,
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID] != nil)
	assert.Equal(t, len(context.applications[appID].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        podUID,
			Pod: &v1.Pod{
				TypeMeta: apis.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: apis.ObjectMeta{
					Name:      podName,
					Namespace: "yk",
					UID:       podUID,
				},
				Spec: v1.PodSpec{},
			},
		},
		Recovery: true,
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), podUID)
	assert.Equal(t, task.GetTaskState(), events.States().Task.Allocated)

	// make sure the recovered task is added to the app
	app, exist := context.applications[appID]
	assert.Equal(t, exist, true)
	assert.Equal(t, len(app.GetAllocatedTasks()), 1)

	// verify the info for the recovered task
	recoveredTask, err := app.GetTask(podUID)
	assert.NilError(t, err)
	tt, ok := recoveredTask.(*Task)
	assert.Equal(t, ok, true)
	assert.Equal(t, tt.taskID, podUID)
	assert.Equal(t, tt.allocationUUID, podUID)
	assert.Equal(t, tt.GetTaskState(), events.States().Task.Allocated)
	assert.Equal(t, tt.alias, fmt.Sprintf("yk/%s", podName))
	assert.Equal(t, tt.pod.UID, types.UID(podUID))
}

func TestTaskReleaseAfterRecovery(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, context.TaskEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	const appID = "app00001"
	const queue = "root.a"
	const pod1UID = "task00001"
	const pod1Name = "my-pod-1"
	const pod2UID = "task00002"
	const pod2Name = "my-pod-2"
	const namespace = "yk"

	// do app recovery, first recover app, then tasks
	// add application to recovery
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: true,
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID] != nil)
	assert.Equal(t, len(context.applications[appID].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task0 := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        pod1UID,
			Pod: &v1.Pod{
				TypeMeta: apis.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: apis.ObjectMeta{
					Name:      pod1Name,
					Namespace: namespace,
					UID:       pod1UID,
				},
				Spec: v1.PodSpec{},
			},
		},
		Recovery: true,
	})

	assert.Assert(t, task0 != nil)
	assert.Equal(t, task0.GetTaskID(), pod1UID)
	assert.Equal(t, task0.GetTaskState(), events.States().Task.Allocated)

	task1 := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        pod2UID,
			Pod: &v1.Pod{
				TypeMeta: apis.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: apis.ObjectMeta{
					Name:      pod2Name,
					Namespace: namespace,
					UID:       pod2UID,
				},
				Spec: v1.PodSpec{},
			},
		},
		Recovery: true,
	})

	assert.Assert(t, task1 != nil)
	assert.Equal(t, task1.GetTaskID(), pod2UID)
	assert.Equal(t, task1.GetTaskState(), events.States().Task.Allocated)

	// app should have 2 tasks recovered
	app, exist := context.applications[appID]
	assert.Equal(t, exist, true)
	assert.Equal(t, len(app.GetAllocatedTasks()), 2)

	// release one of the tasks
	context.NotifyTaskComplete(appID, pod2UID)

	// wait for release
	t0, ok := task0.(*Task)
	assert.Equal(t, ok, true)
	t1, ok := task1.(*Task)
	assert.Equal(t, ok, true)

	err := common.WaitFor(100*time.Millisecond, 3*time.Second, func() bool {
		return t1.GetTaskState() == events.States().Task.Completed
	})
	assert.NilError(t, err, "release should be completed for task1")

	// expect to see:
	//  - task0 is still there
	//  - task1 gets released
	assert.Equal(t, t0.GetTaskState(), events.States().Task.Allocated)
	assert.Equal(t, t1.GetTaskState(), events.States().Task.Completed)
}

func TestRemoveTask(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	// add 2 tasks
	context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})

	// verify app and tasks
	managedApp := context.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)

	app, valid := managedApp.(*Application)
	if !valid {
		t.Errorf("expecting application type")
	}

	assert.Assert(t, app != nil)

	// now app should have 2 tasks
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// try to remove a non-exist task
	// this should fail
	err := context.RemoveTask("app00001", "non-exist-task")
	assert.Assert(t, err != nil)

	// try to remove a task from non-exist application
	// this should also fail
	err = context.RemoveTask("app-non-exist", "task00001")
	assert.Assert(t, err != nil)

	// this should success
	err = context.RemoveTask("app00001", "task00001")
	assert.Assert(t, err == nil)

	// now only 1 task left
	assert.Equal(t, len(app.GetNewTasks()), 1)

	// this should success
	err = context.RemoveTask("app00001", "task00002")
	assert.Assert(t, err == nil)

	// now there is no task left
	assert.Equal(t, len(app.GetNewTasks()), 0)
}
