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

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
)

// completing an application spawns a goroutine that calls cleanUp on the placeholder manager
// singleton, which panics if it was never built
func initPlaceholderManagerForTest() {
	placeholderMgr = NewPlaceholderManager(client.NewMockedAPIProvider(false).GetAPIs())
}

// The core can revive an application it had already completed, so the shim must not tear its own
// application down while tasks are still live.
func TestCompleteApplicationIgnoredWhileTasksActive(t *testing.T) {
	context := initContextForTest()
	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	pending := NewTask("task0001", app, context, &v1.Pod{})
	pending.sm.SetState(TaskStates().Pending)
	app.addTask(pending)
	app.SetState(ApplicationStates().Running)

	err := app.handle(NewSimpleApplicationEvent(app.applicationID, CompleteApplication))
	assert.NilError(t, err, "an ignored event must not surface as an error")
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().Running,
		"app must stay Running while a task is pending")
}

func TestCompleteApplicationAllowedWhenAllTasksTerminated(t *testing.T) {
	initPlaceholderManagerForTest()
	context := initContextForTest()
	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	done := NewTask("task0001", app, context, &v1.Pod{})
	done.sm.SetState(TaskStates().Completed)
	failed := NewTask("task0002", app, context, &v1.Pod{})
	failed.sm.SetState(TaskStates().Failed)
	app.addTask(done)
	app.addTask(failed)
	app.SetState(ApplicationStates().Running)

	err := app.handle(NewSimpleApplicationEvent(app.applicationID, CompleteApplication))
	assert.NilError(t, err)
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().Completed,
		"app should complete once every task is terminated")
}

func TestCompleteApplicationAllowedWithNoTasks(t *testing.T) {
	initPlaceholderManagerForTest()
	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	app.SetState(ApplicationStates().Running)

	err := app.handle(NewSimpleApplicationEvent(app.applicationID, CompleteApplication))
	assert.NilError(t, err)
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().Completed)
}

func TestHasActiveTasks(t *testing.T) {
	context := initContextForTest()
	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	assert.Assert(t, !app.hasActiveTasks(), "no tasks means nothing active")

	task := NewTask("task0001", app, context, &v1.Pod{})
	app.addTask(task)
	for _, state := range []string{TaskStates().New, TaskStates().Pending, TaskStates().Scheduling,
		TaskStates().Allocated, TaskStates().Bound, TaskStates().Killing} {
		task.sm.SetState(state)
		assert.Assert(t, app.hasActiveTasks(), "%s is a live task state", state)
	}
	for _, state := range TaskStates().Terminated {
		task.sm.SetState(state)
		assert.Assert(t, !app.hasActiveTasks(), "%s is a terminal task state", state)
	}
}
