/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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
	"testing"

	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
)

const fakeClusterID = "test-cluster"
const fakeClusterVersion = "0.1.0"
const fakeClusterSchedulerName = "yunikorn-test"
const fakeClusterSchedulingInterval = 1

func initContextForTest() *Context {
	configs := conf.SchedulerConf{
		ClusterID:      fakeClusterID,
		ClusterVersion: fakeClusterVersion,
		SchedulerName:  fakeClusterSchedulerName,
		Interval:       fakeClusterSchedulingInterval,
		KubeConfig:     "",
		TestMode:       true,
	}

	conf.Set(&configs)

	context := NewContext(client.NewMockedAPIProvider())
	return context
}

func TestAddApplications(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
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
	app, ok := context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.other",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	assert.Equal(t, ok, false)
	assert.Equal(t, app.queue, "root.a")
}

func TestGetApplication(t *testing.T) {
	context := initContextForTest()
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app00002",
			QueueName:     "root.b",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	app, ok := context.GetApplication("app00001")
	assert.Equal(t, ok, true)
	assert.Equal(t, app.applicationID, "app00001")
	assert.Equal(t, app.queue, "root.a")
	assert.Equal(t, app.user, "test-user")

	app, ok = context.GetApplication("app00002")
	assert.Equal(t, ok, true)
	assert.Equal(t, app.applicationID, "app00002")
	assert.Equal(t, app.queue, "root.b")
	assert.Equal(t, app.user, "test-user")

	// get a non-exist application
	app, ok = context.GetApplication("app-none-exist")
	assert.Equal(t, ok, false)
	assert.Assert(t, app == nil)
}

func TestRemoveApplication(t *testing.T) {
	// add 2 applications
	context := initContextForTest()
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
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

	app, ok := context.GetApplication("app00001")
	assert.Equal(t, ok, false)
	assert.Assert(t, app == nil)

	// try remove again
	// this should fail
	err = context.RemoveApplication("app00001")
	assert.Assert(t, err != nil)

	// make sure the other app is not affected
	app, ok = context.GetApplication("app00002")
	assert.Equal(t, ok, true)
	assert.Assert(t, app != nil)
}

func TestAddTask(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
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
	task, taskAdded := context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Assert(t, taskAdded, true)
	assert.Equal(t, task.taskID, "task00001")

	// add another task
	task, taskAdded = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Assert(t, taskAdded, true)
	assert.Equal(t, task.taskID, "task00002")

	// add a task with dup taskID
	task, taskAdded = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Equal(t, taskAdded, false)
	assert.Equal(t, task.taskID, "task00002")

	// add a task without app's appearance
	task, taskAdded = context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app-non-exist",
			TaskID:        "task00003",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	assert.Equal(t, taskAdded, false)
	assert.Assert(t, task == nil)

	// verify number of tasks in cache
	assert.Equal(t, len(context.applications["app00001"].GetNewTasks()), 2)
}

func TestRemoveTask(t *testing.T) {
	context := initContextForTest()

	// add a new application
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})

	// add 2 tasks
	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})
	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})

	// verify app and tasks
	app, ok := context.GetApplication("app00001")
	assert.Equal(t, ok, true)
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