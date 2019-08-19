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
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

const fakeClusterId = "test-cluster"
const fakeClusterVersion = "0.1.0"
const fakeClusterSchedulerName = "yunikorn-test"
const fakeClusterSchedulingInterval = 1

func initContextForTest() *Context {
	configs := conf.SchedulerConf{
		ClusterId:      fakeClusterId,
		ClusterVersion: fakeClusterVersion,
		SchedulerName:  fakeClusterSchedulerName,
		Interval:       fakeClusterSchedulingInterval,
		KubeConfig:     "",
		TestMode:       true,
	}

	conf.Set(&configs)
	client := test.NewKubeClientMock()

	context := NewContextInternal(nil, &configs, client, true)
	return context
}
func TestAddApplications(t *testing.T) {
	context := initContextForTest()
	app01 := NewApplication("app00001", "root.a", "testuser", map[string]string{}, nil)
	context.AddApplication(app01)
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications["app00001"] != nil)
	assert.Equal(t, context.applications["app00001"].GetApplicationState(), events.States().Application.New)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 0)

	task01 := CreateTaskForTest("task00001", app01, nil, nil, nil)
	task02 := CreateTaskForTest("task00002", app01, nil, nil, nil)
	app01.AddTask(&task01)
	app01.AddTask(&task02)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 2)
}

func TestAddPod(t *testing.T) {
	context := initContextForTest()

	pod := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:         "pod00001",
			Namespace:    "default",
			UID:          "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{ SchedulerName: fakeClusterSchedulerName },
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	context.addPod(&pod)
	app01 := context.getOrCreateApplication(&pod)
	assert.Equal(t, len(context.applications), 1)
	assert.Equal(t, app01.GetApplicationId(), "app00001")
	assert.Equal(t, len(app01.GetPendingTasks()), 1)
	assert.Equal(t, app01.GetPendingTasks()[0].GetTaskPod().Name, "pod00001")
	assert.Equal(t, string(app01.GetPendingTasks()[0].GetTaskPod().UID), "UID-POD-00001")
	assert.Equal(t, app01.GetPendingTasks()[0].GetTaskPod().Namespace, "default")

	// add same pod again
	context.addPod(&pod)
	assert.Equal(t, len(context.applications), 1)
	assert.Equal(t, context.getOrCreateApplication(&pod), app01)

	// add another pod for same application
	pod1 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:         "pod00002",
			Namespace:    "default",
			UID:          "UID-POD-00002",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{ SchedulerName: fakeClusterSchedulerName },
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	context.addPod(&pod1)
	assert.Equal(t, len(context.applications), 1)
	assert.Equal(t, len(app01.GetPendingTasks()), 2)

	for _, pt := range app01.GetPendingTasks() {
		switch pt.GetTaskPod().Name {
		case "pod00001" :
			assert.Equal(t, string(pt.GetTaskPod().UID), "UID-POD-00001")
			assert.Equal(t, pt.GetTaskPod().Namespace, "default")
		case "pod00002" :
			assert.Equal(t, string(pt.GetTaskPod().UID), "UID-POD-00002")
			assert.Equal(t, pt.GetTaskPod().Namespace, "default")
		default:
			t.Fatalf("pending tasks contain unexpected task %s", pt.GetTaskPod().Name)
		}
	}

	// add a invalid pod
	pod2 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:         "pod00003",
			Namespace:    "default",
			UID:          "UID-POD-00003",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{}, // scheduler name missing
		Status: v1.PodStatus{
			Phase:  v1.PodPending,
		},
	}

	context.addPod(&pod2)
	assert.Equal(t, len(context.applications), 1)
	assert.Equal(t, len(app01.GetPendingTasks()), 2)

	// add another pod from another app
	pod3 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:         "pod00004",
			Namespace:    "default",
			UID:          "UID-POD-00004",
			Labels: map[string]string{
				"applicationId": "app00002",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{ SchedulerName: fakeClusterSchedulerName },
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	context.addPod(&pod3)
	assert.Equal(t, len(context.applications), 2)
	assert.Equal(t, len(app01.GetPendingTasks()), 2)
	app02 := context.getOrCreateApplication(&pod3)
	assert.Equal(t, len(app02.GetPendingTasks()), 1)
	assert.Equal(t, app02.GetApplicationId(), "app00002")
}

func TestPodRejected(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, context.TaskEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	pod := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:         "pod00001",
			Namespace:    "default",
			UID:          "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec:   v1.PodSpec{ SchedulerName: fakeClusterSchedulerName },
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	context.addPod(&pod)
	app01 := context.getOrCreateApplication(&pod)
	assert.Equal(t, len(context.applications), 1)
	assert.Equal(t, app01.GetApplicationId(), "app00001")
	assert.Equal(t, len(app01.GetPendingTasks()), 1)
	assert.Equal(t, app01.GetPendingTasks()[0].GetTaskPod().Name, "pod00001")
	assert.Equal(t, string(app01.GetPendingTasks()[0].GetTaskPod().UID), "UID-POD-00001")
	assert.Equal(t, app01.GetPendingTasks()[0].GetTaskPod().Namespace, "default")

	// reject the task
	task, _ := app01.GetTask("UID-POD-00001")
	err := task.handle(NewRejectTaskEvent("app00001", "UID-POD-00001", ""))
	assert.Assert(t, err == nil)
	assert.Equal(t, len(app01.GetPendingTasks()), 0)

	task01, err := app01.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assertTaskState(t, task01, events.States().Task.Failed, 3*time.Second)
}

func assertTaskState(t *testing.T, task *Task, expectedState string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		if task.GetTaskState() == expectedState {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				task.taskId, expectedState, task.GetTaskState())
		}
	}
}

