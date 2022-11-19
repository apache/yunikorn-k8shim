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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	k8sEvents "k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var (
	testGroups = []string{"dev", "yunikorn"}
)

func initContextForTest() *Context {
	conf.GetSchedulerConf().SetTestMode(true)
	context := NewContext(client.NewMockedAPIProvider(false))
	return context
}

func newPodHelper(name, namespace, podUID, nodeName string, podPhase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(podUID),
		},
		Spec: v1.PodSpec{
			NodeName:      nodeName,
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: podPhase,
		},
	}
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
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications["app00001"] != nil)
	assert.Equal(t, context.applications["app00001"].GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 0)

	// add an app but app already exists
	app := context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.other",
			User:          "test-user",
			Tags:          nil,
		},
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
	})
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00002",
			QueueName:     "root.b",
			User:          "test-user",
			Tags:          nil,
		},
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
	// add 3 applications
	context := initContextForTest()
	appID1 := "app00001"
	appID2 := "app00002"
	appID3 := "app00003"
	app1 := NewApplication(appID1, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, "root.b", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	app3 := NewApplication(appID3, "root.c", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	context.applications[appID1] = app1
	context.applications[appID2] = app2
	context.applications[appID3] = app3
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "remove-test-00001",
			UID:  "UID-00001",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "remove-test-00002",
			UID:  "UID-00002",
		},
	}
	// New task to application 1
	// set task state in Pending (non-terminated)
	task1 := NewTask("task01", app1, context, pod1)
	app1.taskMap["task01"] = task1
	task1.sm.SetState(TaskStates().Pending)
	// New task to application 2
	// set task state in Failed (terminated)
	task2 := NewTask("task02", app2, context, pod2)
	app2.taskMap["task02"] = task2
	task2.sm.SetState(TaskStates().Failed)

	// remove application 1 which have non-terminated task
	// this should fail
	assert.Equal(t, len(context.applications), 3)
	err := context.RemoveApplication(appID1)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "application app00001 because it still has task in non-terminated task, tasks: /remove-test-00001")

	app := context.GetApplication(appID1)
	assert.Assert(t, app != nil)

	// remove application 2 which have terminated task
	// this should be successful
	err = context.RemoveApplication(appID2)
	assert.Assert(t, err == nil)

	app = context.GetApplication(appID2)
	assert.Assert(t, app == nil)

	// try remove again
	// this should fail
	err = context.RemoveApplication(appID2)
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "application app00002 is not found in the context")

	// make sure the other app is not affected
	app = context.GetApplication(appID3)
	assert.Assert(t, app != nil)
}

func TestRemoveApplicationInternal(t *testing.T) {
	context := initContextForTest()
	appID1 := "app00001"
	appID2 := "app00002"
	app1 := NewApplication(appID1, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, "root.b", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	context.applications[appID1] = app1
	context.applications[appID2] = app2
	assert.Equal(t, len(context.applications), 2)

	// remove non-exist app
	context.RemoveApplicationInternal("app00003")
	assert.Equal(t, len(context.applications), 2)

	// remove app1
	context.RemoveApplicationInternal(appID1)
	assert.Equal(t, len(context.applications), 1)
	_, ok := context.applications[appID1]
	assert.Equal(t, ok, false)

	// remove app2
	context.RemoveApplicationInternal(appID2)
	assert.Equal(t, len(context.applications), 0)
	_, ok = context.applications[appID2]
	assert.Equal(t, ok, false)
}

func TestFilterPods(t *testing.T) {
	context := initContextForTest()
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{SchedulerName: "default-scheduler"},
	}
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:   "yunikorn-test-00003",
			UID:    "UID-00003",
			Labels: map[string]string{"applicationId": "test-00003"},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	assert.Check(t, !context.filterPods(nil), "nil object was allowed")
	assert.Check(t, !context.filterPods(pod1), "yunikorn-managed pod with no app id was allowed")
	assert.Check(t, !context.filterPods(pod2), "non-yunikorn-managed pod was allowed")
	assert.Check(t, context.filterPods(pod3), "yunikorn-managed pod was filtered")
}

func TestAddPodToCache(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	context.addPodToCache(nil)  // no-op, but should not crash
	context.addPodToCache(pod1) // should be added
	context.addPodToCache(pod2) // should skip as pod is terminated

	_, ok := context.schedulerCache.GetPod("UID-00001")
	assert.Check(t, ok, "active pod was not added")
	_, ok = context.schedulerCache.GetPod("UID-00002")
	assert.Check(t, !ok, "terminated pod was added")
}

func TestUpdatePodInCache(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        "yunikorn-test-00001",
			UID:         "UID-00001",
			Annotations: map[string]string{"test.state": "new"},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        "yunikorn-test-00001",
			UID:         "UID-00001",
			Annotations: map[string]string{"test.state": "updated"},
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	context.addPodToCache(pod1)
	_, ok := context.schedulerCache.GetPod("UID-00001")
	assert.Assert(t, ok, "pod1 is not present after adding")

	// these should not fail, but are no-ops
	context.updatePodInCache(nil, nil)
	context.updatePodInCache(nil, pod1)
	context.updatePodInCache(pod1, nil)

	// ensure a terminated pod is removed
	context.updatePodInCache(pod1, pod3)
	found, ok := context.schedulerCache.GetPod("UID-00001")
	assert.Check(t, !ok, "pod still found after termination")

	// ensure a non-terminated pod is updated
	context.updatePodInCache(pod1, pod2)
	found, ok = context.schedulerCache.GetPod("UID-00001")
	if assert.Check(t, ok, "pod not found after update") {
		assert.Check(t, found.GetAnnotations()["test.state"] == "updated", "pod state not updated")
	}
}

func TestRemovePodFromCache(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "yunikorn-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}

	context.addPodToCache(pod1)
	context.addPodToCache(pod2)
	_, ok := context.schedulerCache.GetPod("UID-00001")
	assert.Assert(t, ok, "pod1 is not present after adding")
	_, ok = context.schedulerCache.GetPod("UID-00002")
	assert.Assert(t, ok, "pod2 is not present after adding")

	// these should not fail, but here for completeness
	context.removePodFromCache(nil)
	context.removePodFromCache(cache.DeletedFinalStateUnknown{Key: "UID-00000", Obj: nil})

	context.removePodFromCache(pod1)
	_, ok = context.schedulerCache.GetPod("UID-00001")
	assert.Check(t, !ok, "pod1 is still present")

	context.removePodFromCache(cache.DeletedFinalStateUnknown{Key: "UID-00002", Obj: pod2})
	_, ok = context.schedulerCache.GetPod("UID-00002")
	assert.Check(t, !ok, "pod2 is still present")
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
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications["app00001"] != nil)
	assert.Equal(t, context.applications["app00001"].GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
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
	})
	assert.Assert(t, task == nil)

	// verify number of tasks in cache
	assert.Equal(t, len(context.applications["app00001"].GetNewTasks()), 2)
}

func TestRecoverTask(t *testing.T) {
	context := initContextForTest()

	const (
		appID        = "app00001"
		queue        = "root.a"
		podNamespace = "yk"
		user         = "test-user"
		taskUID1     = "task00001"
		taskUID2     = "task00002"
		taskUID3     = "task00003"
		taskUID4     = "task00004"
		fakeNodeName = "fake-node"
	)

	// add a new application
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          user,
			Tags:          nil,
		},
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID] != nil)
	assert.Equal(t, len(context.applications[appID].GetPendingTasks()), 0)

	// add a tasks to the existing application
	// this task was already allocated and Running
	task := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID1,
			Pod:           newPodHelper("pod1", podNamespace, taskUID1, fakeNodeName, v1.PodRunning),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID1)
	assert.Equal(t, task.GetTaskState(), TaskStates().Allocated)

	// add a tasks to the existing application
	// this task was already completed with state: Succeed
	task = context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID2,
			Pod:           newPodHelper("pod2", podNamespace, taskUID2, fakeNodeName, v1.PodSucceeded),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID2)
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)

	// add a tasks to the existing application
	// this task was already completed with state: Succeed
	task = context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID3,
			Pod:           newPodHelper("pod3", podNamespace, taskUID3, fakeNodeName, v1.PodFailed),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID3)
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)

	// add a tasks to the existing application
	// this task pod is still Pending
	task = context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskUID4,
			Pod:           newPodHelper("pod4", podNamespace, taskUID4, "", v1.PodPending),
		},
	})
	assert.Assert(t, task != nil)
	assert.Equal(t, task.GetTaskID(), taskUID4)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// make sure the recovered task is added to the app
	app, exist := context.applications[appID]
	assert.Equal(t, exist, true)
	assert.Equal(t, len(app.getTasks(TaskStates().Allocated)), 1)
	assert.Equal(t, len(app.getTasks(TaskStates().Completed)), 2)
	assert.Equal(t, len(app.getTasks(TaskStates().New)), 1)

	taskInfoVerifiers := []struct {
		taskID                 string
		expectedState          string
		expectedAllocationUUID string
		expectedPodName        string
		expectedNodeName       string
	}{
		{taskUID1, TaskStates().Allocated, taskUID1, "pod1", fakeNodeName},
		{taskUID2, TaskStates().Completed, taskUID2, "pod2", fakeNodeName},
		{taskUID3, TaskStates().Completed, taskUID3, "pod3", fakeNodeName},
		{taskUID4, TaskStates().New, "", "pod4", ""},
	}

	for _, tt := range taskInfoVerifiers {
		t.Run(tt.taskID, func(t *testing.T) {
			// verify the info for the recovered task
			recoveredTask, err := app.GetTask(tt.taskID)
			assert.NilError(t, err)
			rt, ok := recoveredTask.(*Task)
			assert.Equal(t, ok, true)
			assert.Equal(t, rt.GetTaskState(), tt.expectedState)
			assert.Equal(t, rt.allocationUUID, tt.expectedAllocationUUID)
			assert.Equal(t, rt.pod.Name, tt.expectedPodName)
			assert.Equal(t, rt.alias, fmt.Sprintf("%s/%s", podNamespace, tt.expectedPodName))
		})
	}
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
	const fakeNodeName = "fake-node"

	// do app recovery, first recover app, then tasks
	// add application to recovery
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: appID,
			QueueName:     queue,
			User:          "test-user",
			Tags:          nil,
		},
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications[appID] != nil)
	assert.Equal(t, len(context.applications[appID].GetPendingTasks()), 0)

	// add a tasks to the existing application
	task0 := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        pod1UID,
			Pod:           newPodHelper(pod1Name, namespace, pod1UID, fakeNodeName, v1.PodRunning),
		},
	})

	assert.Assert(t, task0 != nil)
	assert.Equal(t, task0.GetTaskID(), pod1UID)
	assert.Equal(t, task0.GetTaskState(), TaskStates().Allocated)

	task1 := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        pod2UID,
			Pod:           newPodHelper(pod2Name, namespace, pod2UID, fakeNodeName, v1.PodRunning),
		},
	})

	assert.Assert(t, task1 != nil)
	assert.Equal(t, task1.GetTaskID(), pod2UID)
	assert.Equal(t, task1.GetTaskState(), TaskStates().Allocated)

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
		return t1.GetTaskState() == TaskStates().Completed
	})
	assert.NilError(t, err, "release should be completed for task1")

	// expect to see:
	//  - task0 is still there
	//  - task1 gets released
	assert.Equal(t, t0.GetTaskState(), TaskStates().Allocated)
	assert.Equal(t, t1.GetTaskState(), TaskStates().Completed)
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
	})

	// add 2 tasks
	context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
	})
	context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
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
	context.RemoveTask("app00001", "non-exist-task")
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// try to remove a task from non-exist application
	context.RemoveTask("app-non-exist", "task00001")
	assert.Equal(t, len(app.GetNewTasks()), 2)

	// this should success
	context.RemoveTask("app00001", "task00001")

	// now only 1 task left
	assert.Equal(t, len(app.GetNewTasks()), 1)

	// this should success
	context.RemoveTask("app00001", "task00002")

	// now there is no task left
	assert.Equal(t, len(app.GetNewTasks()), 0)
}

func TestGetTask(t *testing.T) {
	// add 3 applications
	context := initContextForTest()
	appID1 := "app00001"
	appID2 := "app00002"
	appID3 := "app00003"
	app1 := NewApplication(appID1, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, "root.b", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	app3 := NewApplication(appID3, "root.c", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	context.applications[appID1] = app1
	context.applications[appID2] = app2
	context.applications[appID3] = app3
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "getTask-test-00001",
			UID:  "UID-00001",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "getTask-test-00002",
			UID:  "UID-00002",
		},
	}
	// New task to application 1
	// set task state in Pending (non-terminated)
	task1 := NewTask("task01", app1, context, pod1)
	app1.taskMap["task01"] = task1
	task1.sm.SetState(TaskStates().Pending)
	// New task to application 2
	// set task state in Failed (terminated)
	task2 := NewTask("task02", app2, context, pod2)
	app2.taskMap["task02"] = task2
	task2.sm.SetState(TaskStates().Failed)

	task := context.getTask(appID1, "task01")
	assert.Assert(t, task == task1)

	task = context.getTask("non_existing_appID", "task01")
	assert.Assert(t, task == nil)

	task = context.getTask(appID1, "non_existing_taskID")
	assert.Assert(t, task == nil)

	task = context.getTask(appID3, "task03")
	assert.Assert(t, task == nil)
}

func TestNodeEventFailsPublishingWithoutNode(t *testing.T) {
	conf.GetSchedulerConf().SetTestMode(true)
	recorder, ok := events.GetRecorder().(*k8sEvents.FakeRecorder)
	if !ok {
		t.Fatal("the EventRecorder is expected to be of type FakeRecorder")
	}
	context := initContextForTest()

	eventRecords := make([]*si.EventRecord, 0)
	message := "non_existing_node_related_message"
	reason := "non_existing_node_related_reason"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:     si.EventRecord_NODE,
		ObjectID: "non_existing_host",
		Reason:   reason,
		Message:  message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has been published
	select {
	case event := <-recorder.Events:
		log.Logger().Info(event)
		if strings.Contains(event, reason) && strings.Contains(event, message) {
			t.Fatal("event should not be published if the pod does not exist")
		}
	default:
		break
	}
}

func TestNodeEventPublishedCorrectly(t *testing.T) {
	conf.GetSchedulerConf().SetTestMode(true)
	recorder, ok := events.GetRecorder().(*k8sEvents.FakeRecorder)
	if !ok {
		t.Fatal("the EventRecorder is expected to be of type FakeRecorder")
	}
	context := initContextForTest()

	node := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
	}
	context.addNode(&node)

	eventRecords := make([]*si.EventRecord, 0)
	message := "node_related_message"
	reason := "node_related_reason"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:     si.EventRecord_NODE,
		ObjectID: "host0001",
		Reason:   reason,
		Message:  message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has been published
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				log.Logger().Info(event)
				if strings.Contains(event, reason) && strings.Contains(event, message) {
					return true
				}
			default:
				return false
			}
		}
	}, 5*time.Millisecond, 20*time.Millisecond)
	assert.NilError(t, err, "event should have been emitted")
}

func TestPublishEventsWithNotExistingAsk(t *testing.T) {
	conf.GetSchedulerConf().SetTestMode(true)
	recorder, ok := events.GetRecorder().(*k8sEvents.FakeRecorder)
	if !ok {
		t.Fatal("the EventRecorder is expected to be of type FakeRecorder")
	}
	context := initContextForTest()
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app_event_12",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
	})
	eventRecords := make([]*si.EventRecord, 0)
	message := "event_related_text_msg"
	reason := "event_related_text"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "non_existing_task_event",
		GroupID:  "app_event_12",
		Reason:   reason,
		Message:  message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has not been published
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, reason) && strings.Contains(event, message) {
					return false
				}
			default:
				return true
			}
		}
	}, 5*time.Millisecond, 20*time.Millisecond)
	assert.NilError(t, err, "event should not have been published if the pod does not exist")
}

func TestPublishEventsCorrectly(t *testing.T) {
	conf.GetSchedulerConf().SetTestMode(true)
	recorder, ok := events.GetRecorder().(*k8sEvents.FakeRecorder)
	if !ok {
		t.Fatal("the EventRecorder is expected to be of type FakeRecorder")
	}
	context := initContextForTest()

	// create fake application and task
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app_event",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
	})
	context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app_event",
			TaskID:        "task_event",
			Pod:           &v1.Pod{},
		},
	})

	// create an event belonging to that task
	eventRecords := make([]*si.EventRecord, 0)
	message := "event_related_message"
	reason := "event_related_reason"
	eventRecords = append(eventRecords, &si.EventRecord{
		Type:     si.EventRecord_REQUEST,
		ObjectID: "task_event",
		GroupID:  "app_event",
		Reason:   reason,
		Message:  message,
	})
	context.PublishEvents(eventRecords)

	// check that the event has been published
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, reason) && strings.Contains(event, message) {
					return true
				}
			default:
				return false
			}
		}
	}, 5*time.Millisecond, 20*time.Millisecond)
	assert.NilError(t, err, "event should have been emitted")
}

func TestAddApplicationsWithTags(t *testing.T) {
	context := initContextForTest()

	lister, ok := context.apiProvider.GetAPIs().NamespaceInformer.Lister().(*test.MockNamespaceLister)
	if !ok {
		t.Fatalf("could not mock NamespaceLister")
	}

	// set up namespaces
	ns1 := v1.Namespace{
		ObjectMeta: apis.ObjectMeta{
			Name: "test1",
		},
	}
	lister.Add(&ns1)
	ns2 := v1.Namespace{
		ObjectMeta: apis.ObjectMeta{
			Name: "test2",
			Annotations: map[string]string{
				"yunikorn.apache.org/namespace.max.memory": "256M",
				"yunikorn.apache.org/parentqueue":          "root.test",
			},
		},
	}
	lister.Add(&ns2)

	// add application with empty namespace
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags: map[string]string{
				constants.AppTagNamespace: "",
			},
		},
	})

	// add application with non-existing namespace
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00002",
			QueueName:     "root.a",
			User:          "test-user",
			Tags: map[string]string{
				constants.AppTagNamespace: "non-existing",
			},
		},
	})

	// add application with unannotated namespace
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00003",
			QueueName:     "root.a",
			User:          "test-user",
			Tags: map[string]string{
				constants.AppTagNamespace: "test1",
			},
		},
	})

	// add application with annotated namespace
	request := &interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00004",
			QueueName:     "root.a",
			User:          "test-user",
			Tags: map[string]string{
				constants.AppTagNamespace: "test2",
			},
		},
	}
	context.AddApplication(request)

	// check that request has additional annotations
	quotaStr, ok := request.Metadata.Tags[siCommon.AppTagNamespaceResourceQuota]
	if !ok {
		t.Fatalf("resource quota tag is not updated from the namespace")
	}
	quotaRes := si.Resource{}
	if err := json.Unmarshal([]byte(quotaStr), &quotaRes); err == nil {
		if quotaRes.Resources == nil || quotaRes.Resources["memory"] == nil {
			t.Fatalf("could not find parsed memory resource from annotation")
		}
		assert.Equal(t, quotaRes.Resources["memory"].Value, int64(256*1000*1000))
	} else {
		t.Fatalf("resource parsing failed")
	}
	parentQueue, ok := request.Metadata.Tags[constants.AppTagNamespaceParentQueue]
	if !ok {
		t.Fatalf("parent queue tag is not updated from the namespace")
	}
	assert.Equal(t, parentQueue, "root.test")
}

func TestPendingPodAllocations(t *testing.T) {
	context := initContextForTest()
	context.SetPluginMode(true)

	node1 := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
	}
	context.addNode(&node1)

	node2 := v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0002",
			Namespace: "default",
			UID:       "uid_0002",
		},
	}
	context.addNode(&node2)

	// add a new application
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
	})

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "test-00001",
			UID:  "UID-00001",
		},
	}

	// add a tasks to the existing application
	task := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           pod,
		},
	})
	assert.Assert(t, task != nil, "task was nil")

	// add the allocation
	context.AddPendingPodAllocation("UID-00001", "host0001")

	// validate that the pending allocation matches
	nodeID, ok := context.GetPendingPodAllocation("UID-00001")
	if !ok {
		t.Fatalf("no pending pod allocation found")
	}
	assert.Equal(t, nodeID, "host0001", "wrong host")

	// validate that there is not an in-progress allocation
	if _, ok = context.GetInProgressPodAllocation("UID-00001"); ok {
		t.Fatalf("in-progress allocation exists when it should be pending")
	}

	if context.StartPodAllocation("UID-00001", "host0002") {
		t.Fatalf("attempt to start pod allocation on wrong node succeeded")
	}

	if !context.StartPodAllocation("UID-00001", "host0001") {
		t.Fatalf("attempt to start pod allocation on correct node failed")
	}

	if _, ok = context.GetPendingPodAllocation("UID-00001"); ok {
		t.Fatalf("pending pod allocation still exists after transition to in-progress")
	}

	nodeID, ok = context.GetInProgressPodAllocation("UID-00001")
	if !ok {
		t.Fatalf("in-progress allocation does not exist")
	}
	assert.Equal(t, nodeID, "host0001", "wrong host")

	context.RemovePodAllocation("UID-00001")
	if _, ok = context.GetInProgressPodAllocation("UID-00001"); ok {
		t.Fatalf("in-progress pod allocation still exists after removal")
	}

	// re-add to validate pending pod removal
	context.AddPendingPodAllocation("UID-00001", "host0001")
	context.RemovePodAllocation("UID-00001")

	if _, ok = context.GetPendingPodAllocation("UID-00001"); ok {
		t.Fatalf("pending pod allocation still exists after removal")
	}
}

func TestGetStateDump(t *testing.T) {
	context := initContextForTest()

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Namespace: "default",
			Name:      "yunikorn-test-00001",
			UID:       "UID-00001",
		},
		Spec: v1.PodSpec{SchedulerName: "yunikorn"},
	}
	context.addPodToCache(pod1)

	stateDumpStr, err := context.GetStateDump()
	assert.NilError(t, err, "error during state dump")
	var stateDump = make(map[string]interface{})
	err = json.Unmarshal([]byte(stateDumpStr), &stateDump)
	assert.NilError(t, err, "unable to parse state dump")

	cacheObj, ok := stateDump["cache"]
	assert.Assert(t, ok, "cache not found")
	cache, ok := cacheObj.(map[string]interface{})
	assert.Assert(t, ok, "unable to cast cache")

	podsObj, ok := cache["pods"]
	assert.Assert(t, ok, "pods not found")
	pods, ok := podsObj.(map[string]interface{})
	assert.Assert(t, ok, "unable to cast pods")

	podObj, ok := pods["default/yunikorn-test-00001"]
	assert.Assert(t, ok, "pod not found")
	pod, ok := podObj.(map[string]interface{})
	assert.Assert(t, ok, "unable to cast pod")

	uidObj, ok := pod["uid"]
	assert.Assert(t, ok, "uid not found")
	uid, ok := uidObj.(string)
	assert.Assert(t, ok, "Unable to cast uid")

	assert.Equal(t, string(pod1.UID), uid, "wrong uid")
}
