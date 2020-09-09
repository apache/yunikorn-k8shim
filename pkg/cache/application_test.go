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
	is "gotest.tools/assert/cmp"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestNewApplication(t *testing.T) {
	app := NewApplication("app00001", "root.queue", "testuser", map[string]string{}, newMockSchedulerAPI())
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.partition, constants.DefaultPartition)
	assert.Equal(t, len(app.taskMap), 0)
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.queue, "root.queue")
}

func TestSubmitApplication(t *testing.T) {
	app := NewApplication("app00001", "root.abc", "testuser", map[string]string{}, newMockSchedulerAPI())

	err := app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, events.States().Application.Submitted, 10*time.Second)

	// app already submitted
	err = app.handle(NewSubmitApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, events.States().Application.Submitted, 10*time.Second)
}

func TestRunApplication(t *testing.T) {
	ms := &mockSchedulerAPI{}
	ms.updateFn = func(request *si.UpdateRequest) error {
		assert.Equal(t, len(request.NewApplications), 1)
		assert.Equal(t, request.NewApplications[0].ApplicationID, "app00001")
		assert.Equal(t, request.NewApplications[0].QueueName, "root.abc")
		return nil
	}

	app := NewApplication("app00001", "root.abc", "testuser", map[string]string{}, ms)

	// app must be submitted before being able to run
	err := app.handle(NewRunApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, events.States().Application.New, 3*time.Second)

	// submit the app
	err = app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, events.States().Application.Submitted, 3*time.Second)

	// app must be accepted first
	err = app.handle(NewRunApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, events.States().Application.Submitted, 3*time.Second)
}

func newMockSchedulerAPI() *mockSchedulerAPI {
	return &mockSchedulerAPI{
		registerFn: func(request *si.RegisterResourceManagerRequest, callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, nil
		},
		updateFn: func(request *si.UpdateRequest) error {
			return nil
		},
	}
}

type mockSchedulerAPI struct {
	callback   api.ResourceManagerCallback //nolint:structcheck,unused
	registerFn func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	updateFn func(request *si.UpdateRequest) error
}

func (ms *mockSchedulerAPI) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
	return ms.registerFn(request, callback)
}

func (ms *mockSchedulerAPI) Update(request *si.UpdateRequest) error {
	return ms.updateFn(request)
}

func (ms *mockSchedulerAPI) ReloadConfiguration(rmID string) error {
	return nil
}

func assertAppState(t *testing.T, app *Application, expectedState string, duration time.Duration) {
	deadline := time.Now().Add(duration)
	for {
		if app.sm.Current() == expectedState {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for app %s reach to state %s", app.applicationID, expectedState)
		}
	}
}

func TestGetNonTerminatedTaskAlias(t *testing.T) {
	context := initContextForTest()
	appID := "app00001"
	app := NewApplication(appID, "root.a", "testuser", map[string]string{}, newMockSchedulerAPI())
	context.applications[appID] = app
	// app doesn't have any task
	res := app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 0)

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "test-00001",
			UID:  "UID-00001",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "test-00002",
			UID:  "UID-00002",
		},
	}
	// set two task to non-terminated states
	taskID1 := "task01"
	task1 := NewTask(taskID1, app, context, pod1)
	app.taskMap[taskID1] = task1
	task1.sm.SetState(events.States().Task.Pending)
	taskID2 := "task02"
	task2 := NewTask(taskID2, app, context, pod2)
	app.taskMap[taskID2] = task2
	task2.sm.SetState(events.States().Task.Pending)
	// check the tasks both in non-terminated states
	// res should return both task's alias
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 2)
	assert.Assert(t, is.Contains(res, "/test-00001"))
	assert.Assert(t, is.Contains(res, "/test-00002"))

	//set two tasks to terminated states
	task1.sm.SetState(events.States().Task.Rejected)
	task2.sm.SetState(events.States().Task.Rejected)
	// check the tasks both in terminated states
	// res should retuen empty
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 0)

	//set two tasks to one is terminated, another is non-terminated
	task1.sm.SetState(events.States().Task.Rejected)
	task2.sm.SetState(events.States().Task.Allocated)
	// check the task, should only return task2's alias
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 1)
	assert.Equal(t, res[0], "/test-00002")
}
