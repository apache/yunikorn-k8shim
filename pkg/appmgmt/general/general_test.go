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

package general

import (
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
)

func TestGetAppMetadata(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok := am.getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00001")
	assert.Equal(t, app.QueueName, "root.a")
	assert.Equal(t, app.User, "")
	assert.DeepEqual(t, app.Tags, map[string]string{"namespace": "default"})

	pod = v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00002",
			Namespace: "app-namespace-01",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": "app00002",
				"queue":         "root.b",
			},
		},
		Spec: v1.PodSpec{
			SchedulerName:      "yunikorn",
			ServiceAccountName: "bob",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00002")
	assert.Equal(t, app.QueueName, "root.b")
	assert.Equal(t, app.User, "bob")
	assert.DeepEqual(t, app.Tags, map[string]string{"namespace": "app-namespace-01"})

	pod = v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00002",
			Namespace: "app-namespace-01",
			UID:       "UID-POD-00001",
		},
		Spec: v1.PodSpec{
			SchedulerName:      "yunikorn",
			ServiceAccountName: "bob",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod)
	assert.Equal(t, ok, false)
}

func TestGetTaskMetadata(t *testing.T) {
	am := NewManager(&cache.MockedAMProtocol{}, client.NewMockedAPIProvider())

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	task, ok := am.getTaskMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, task.ApplicationID, "app00001")
	assert.Equal(t, task.TaskID, "UID-POD-00001")

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	task, ok = am.getTaskMetadata(&pod)
	assert.Equal(t, ok, false)
}

func TestAddPod(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.addPod(&pod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// add another pod for same application
	pod1 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00002",
			Namespace: "default",
			UID:       "UID-POD-00002",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	am.addPod(&pod1)
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// add another pod from another app
	pod2 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00004",
			Namespace: "default",
			UID:       "UID-POD-00004",
			Labels: map[string]string{
				"applicationId": "app00002",
				"queue":         "root.a",
			},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	am.addPod(&pod2)
	app02 := am.amProtocol.GetApplication("app00002")
	assert.Assert(t, app02 != nil)
	app, valid = toApplication(app02)
	assert.Equal(t, valid, true)
	assert.Equal(t, len(app.GetNewTasks()), 1)
	assert.Equal(t, app.GetApplicationID(), "app00002")
	assert.Equal(t, app.GetNewTasks()[0].GetTaskPod().Name, "pod00004")
}

func TestUpdatePodWhenSucceed(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.addPod(&pod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// try update the pod

	newPod := v1.Pod{
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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	am.updatePod(&pod, &newPod)

	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
}

func TestUpdatePodWhenFailed(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.addPod(&pod)

	// try update the pod to Failed status
	newPod := v1.Pod{
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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	am.updatePod(&pod, &newPod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
}

func TestDeletePod(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.addPod(&pod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// try delete the pod
	am.deletePod(&pod)

	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
}

func toApplication(something interface{}) (*cache.Application, bool) {
	if app, valid := something.(*cache.Application); valid {
		return app, true
	}
	return nil, false
}

func TestGetExistingAllocation(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

	pod := &v1.Pod{
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
		Spec: v1.PodSpec{
			SchedulerName: "yunikorn",
			NodeName:      "allocated-node",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// verifies the existing allocation is correctly returned
	alloc := am.GetExistingAllocation(pod)
	assert.Equal(t, alloc.ApplicationID, "app00001")
	assert.Equal(t, alloc.QueueName, "root.a")
	assert.Equal(t, alloc.AllocationKey, string(pod.UID))
	assert.Equal(t, alloc.UUID, string(pod.UID))
	assert.Equal(t, alloc.NodeID, "allocated-node")
}
