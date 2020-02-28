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
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestNewApplication(t *testing.T) {
	app := NewApplication("app00001", "root.queue", "testuser", map[string]string{}, newMockSchedulerAPI())
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.partition, common.DefaultPartition)
	assert.Equal(t, len(app.taskMap), 0)
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.queue, "root.queue")
}

func TestSubmitApplication(t *testing.T) {
	app := NewApplication("app00001", "root.abc", "testuser", map[string]string{}, newMockSchedulerAPI())

	err := app.handle(NewSubmitApplicationEvent(app.applicationID))
	if err != nil {
		t.Fatalf("%v", err)
	}
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
	if err != nil {
		t.Fatalf("%v", err)
	}
	assertAppState(t, app, events.States().Application.Submitted, 3*time.Second)

	// app must be accepted first
	err = app.handle(NewRunApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, events.States().Application.Submitted, 3*time.Second)
}

func TestGetApplicationIDFromPod(t *testing.T) {
	// defined in label
	pod := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	appID, err := utils.GetApplicationIDFromPod(&pod)
	assert.Equal(t, appID, "app00001")
	assert.Equal(t, err, nil)

	// defined in annotations
	pod = v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Annotations: map[string]string{
				"applicationId": "app00002",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	appID, err = utils.GetApplicationIDFromPod(&pod)
	assert.Equal(t, appID, "app00002")
	assert.Equal(t, err, nil)

	// spark app-id
	pod = v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"spark-app-selector": "spark-0001",
				"queue":              "root.a",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	appID, err = utils.GetApplicationIDFromPod(&pod)
	assert.Equal(t, appID, "spark-0001")
	assert.Equal(t, err, nil)

	// not found
	pod = v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	appID, err = utils.GetApplicationIDFromPod(&pod)
	assert.Equal(t, appID, "")
	assert.Assert(t, err != nil)
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
