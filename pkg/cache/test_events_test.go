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
	"fmt"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/utils"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
	"time"
)

func TestAllocateTaskEventArgs(t *testing.T) {
	alloc := NewAllocateTaskEvent("app-0001", "task-0001", "UID-0001", "node-0001")
	args := alloc.GetArgs()

	assert.Equal(t, len(args), 2)
	assert.Equal(t, fmt.Sprint(args[0]), "UID-0001")
	assert.Equal(t, fmt.Sprint(args[1]), "node-0001")
}

func TestGetAllocateTaskEventArgs(t *testing.T) {
	alloc := NewAllocateTaskEvent("app-0001", "task-0001", "UID-0001", "node-0001")
	args := alloc.GetArgs()
	assert.Equal(t, len(args), 2)
	assert.Equal(t, fmt.Sprint(args[0]), "UID-0001")
	assert.Equal(t, fmt.Sprint(args[1]), "node-0001")

	out := make([]string, 2)
	err := events.GetEventArgsAsStrings(out, args)
	assert.Assert(t, err == nil)
	assert.Equal(t, out[0], "UID-0001")
	assert.Equal(t, out[1], "node-0001")

	out = make([]string, 0)
	err = events.GetEventArgsAsStrings(out, args)
	assert.Assert(t, err != nil)

	out = make([]string, 5)
	err = events.GetEventArgsAsStrings(out, args)
	assert.Assert(t, err != nil)

	err = events.GetEventArgsAsStrings(nil, args)
	assert.Assert(t, err != nil)
}

func TestSubmitTaskSucceed(t *testing.T) {
	schedulerConf := conf.GetSchedulerConf()
	schedulerConf.TestMode = true
	schedulerApi := newMockSchedulerApi()
	app := NewApplication("app00001", "root.queue", "user", map[string]string{}, schedulerApi)
	task := newTask("task00001", app, &test.KubeClientMock{}, schedulerApi, &v1.Pod{})
	if err := task.handle(NewSubmitTaskEvent(app.applicationId, task.taskId)); err != nil {
		t.Fail()
	}
	assert.Equal(t, task.GetTaskState(), events.States().Task.Scheduling)
}

func TestSubmitTaskFailed(t *testing.T) {
	// set things up, use some mocked clients
	schedulerConf := conf.GetSchedulerConf()
	schedulerConf.TestMode = true
	schedulerApi := newMockSchedulerApi()
	schedulerApi.updateFn = func(request *si.UpdateRequest) error {
		return fmt.Errorf("mocked error")
	}
	app := NewApplication("app00001", "root.queue", "user", map[string]string{}, schedulerApi)
	task := newTask("task00001", app, &test.KubeClientMock{}, schedulerApi, &v1.Pod{})

	// launch the dispatcher
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, func(obj interface{}) {
		if event, ok := obj.(events.TaskEvent); ok {
			if task.canHandle(event) {
				if err := task.handle(event); err != nil {
					t.Fail()
				}
			}
		}
	})
	dispatcher.Start()

	// task submission should fail, because we mock the error in client api
	if err := task.handle(NewSubmitTaskEvent(app.applicationId, task.taskId)); err != nil {
		t.Fail()
	}

	// task submission failed, it should become to pending state again
	if err := utils.WaitForCondition(func() bool {
		return task.GetTaskState() == events.States().Task.Pending
	}, time.Second, 3*time.Second); err != nil {
		t.Fail()
	}

	// the pending task cache should contain the task
	contains := false
	for _, p := range app.GetPendingTasks() {
		if p.taskId == task.taskId {
			contains = true
		}
	}
	assert.Equal(t, contains, true)
}