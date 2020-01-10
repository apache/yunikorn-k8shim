package general

import (
	"testing"

	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetAppMetadata(t *testing.T) {
	am := New(cache.NewMockedAMProtocol(), test.NewMockedAPIProvider())

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

	app, ok := am.GetAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00001")
	assert.Equal(t, app.QueueName, "root.a")
	assert.Equal(t, app.User, "")
	assert.DeepEqual(t, app.Tags, map[string]string {"namespace" : "default"})

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
			SchedulerName: "yunikorn",
			ServiceAccountName: "bob",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.GetAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00002")
	assert.Equal(t, app.QueueName, "root.b")
	assert.Equal(t, app.User, "bob")
	assert.DeepEqual(t, app.Tags, map[string]string {"namespace" : "app-namespace-01"})

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
			SchedulerName: "yunikorn",
			ServiceAccountName: "bob",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.GetAppMetadata(&pod)
	assert.Equal(t, ok, false)
}

func TestGetTaskMetadata(t *testing.T) {
	am := New(&cache.MockedAMProtocol{}, test.NewMockedAPIProvider())

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

	task, ok := am.GetTaskMetadata(&pod)
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

	task, ok = am.GetTaskMetadata(&pod)
	assert.Equal(t, ok, false)
}


func TestAddPod(t *testing.T) {
	am := New(cache.NewMockedAMProtocol(), test.NewMockedAPIProvider())

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

	app, ok := am.amProtocol.GetApplication("app00001")
	assert.Equal(t, ok, true)
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
	app02, ok := am.amProtocol.GetApplication("app00002")
	assert.Equal(t, len(app02.GetNewTasks()), 1)
	assert.Equal(t, app02.GetApplicationID(), "app00002")
	assert.Equal(t, app02.GetNewTasks()[0].GetTaskPod().Name, "pod00004")
}

/**
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
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"applicationId": "app00001",
				"queue":         "root.a",
			},
		},
		Spec: v1.PodSpec{SchedulerName: fakeClusterSchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	context.addPod(&pod)
	app01 := context.getOrCreateApplication(&pod)
	assert.Equal(t, len(context.applications), 1)
	assert.Equal(t, app01.GetApplicationID(), "app00001")
	assert.Equal(t, len(app01.GetNewTasks()), 1)
	assert.Equal(t, app01.GetNewTasks()[0].GetTaskPod().Name, "pod00001")
	assert.Equal(t, string(app01.GetNewTasks()[0].GetTaskPod().UID), "UID-POD-00001")
	assert.Equal(t, app01.GetNewTasks()[0].GetTaskPod().Namespace, "default")

	// reject the task
	task, err := app01.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	err = task.handle(NewRejectTaskEvent("app00001", "UID-POD-00001", ""))
	assert.Assert(t, err == nil)
	assert.Equal(t, len(app01.GetNewTasks()), 0)

	task, err = app01.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assertTaskState(t, task, events.States().Task.Failed, 3*time.Second)
}

func assertTaskState(t *testing.T, task *Task, expectedState string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		if task.GetTaskState() == expectedState {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				task.taskID, expectedState, task.GetTaskState())
		}
	}
}
**/