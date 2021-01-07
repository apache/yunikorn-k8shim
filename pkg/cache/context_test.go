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
	"k8s.io/client-go/tools/record"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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
	// add 3 applications
	context := initContextForTest()
	appID1 := "app00001"
	appID2 := "app00002"
	appID3 := "app00003"
	app1 := NewApplication(appID1, "root.a", "testuser", map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, "root.b", "testuser", map[string]string{}, newMockSchedulerAPI())
	app3 := NewApplication(appID3, "root.c", "testuser", map[string]string{}, newMockSchedulerAPI())
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
	task1.sm.SetState(events.States().Task.Pending)
	//New task to application 2
	// set task state in Failed (terminated)
	task2 := NewTask("task02", app2, context, pod2)
	app2.taskMap["task02"] = task2
	task2.sm.SetState(events.States().Task.Failed)

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

	//try remove again
	//this should fail
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
	app1 := NewApplication(appID1, "root.a", "testuser", map[string]string{}, newMockSchedulerAPI())
	app2 := NewApplication(appID2, "root.b", "testuser", map[string]string{}, newMockSchedulerAPI())
	context.applications[appID1] = app1
	context.applications[appID2] = app2
	assert.Equal(t, len(context.applications), 2)
	// remove non-exist app
	err := context.RemoveApplicationInternal("app00003")
	assert.Assert(t, err != nil)
	assert.Equal(t, len(context.applications), 2)
	// remove app1
	err = context.RemoveApplicationInternal(appID1)
	assert.NilError(t, err)
	assert.Equal(t, len(context.applications), 1)
	_, ok := context.applications[appID1]
	assert.Equal(t, ok, false)
	// remove app2
	err = context.RemoveApplicationInternal(appID2)
	assert.NilError(t, err)
	assert.Equal(t, len(context.applications), 0)
	_, ok = context.applications[appID2]
	assert.Equal(t, ok, false)
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

func TestNodeEventFailsPublishingWithoutNode(t *testing.T) {
	conf.GetSchedulerConf().SetTestMode(true)
	recorder, ok := events.GetRecorder().(*record.FakeRecorder)
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
	recorder, ok := events.GetRecorder().(*record.FakeRecorder)
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
	recorder, ok := events.GetRecorder().(*record.FakeRecorder)
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
		Recovery: false,
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
	recorder, ok := events.GetRecorder().(*record.FakeRecorder)
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
		Recovery: false,
	})
	context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app_event",
			TaskID:        "task_event",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
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
		Recovery: false,
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
		Recovery: false,
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
		Recovery: false,
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
		Recovery: false,
	}
	context.AddApplication(request)

	// check that request has additional annotations
	quotaStr, ok := request.Metadata.Tags[constants.AppTagNamespaceResourceQuota]
	if !ok {
		t.Fatalf("resource quota tag is not updated from the namespace")
	}
	quotaRes := si.Resource{}
	if err := json.Unmarshal([]byte(quotaStr), &quotaRes); err == nil {
		if quotaRes.Resources == nil || quotaRes.Resources["memory"] == nil {
			t.Fatalf("could not find parsed memory resource from annotation")
		}
		assert.Equal(t, quotaRes.Resources["memory"].Value, int64(256))
	} else {
		t.Fatalf("resource parsing failed")
	}
	parentQueue, ok := request.Metadata.Tags[constants.AppTagNamespaceParentQueue]
	if !ok {
		t.Fatalf("parent queue tag is not updated from the namespace")
	}
	assert.Equal(t, parentQueue, "root.test")
}

func TestFindYKConfigMap(t *testing.T) {
	goodYKConfigmap := v1.ConfigMap{
		ObjectMeta: apis.ObjectMeta{
			Name:   constants.DefaultConfigMapName,
			Labels: map[string]string{"app": "yunikorn", "label2": "value2"},
		},
		Data: map[string]string{"queues.yaml": "OldData"},
	}
	randomConfigMap := v1.ConfigMap{
		ObjectMeta: apis.ObjectMeta{
			Name: "configMap",
		},
	}
	testCases := []struct {
		name          string
		expectedError bool
		configMaps    []*v1.ConfigMap
	}{
		{"Nil configmaps", true, nil},
		{"Empty configmaps", true, []*v1.ConfigMap{}},
		{"Yunikorn configmap found", false, []*v1.ConfigMap{&goodYKConfigmap, &randomConfigMap}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configMap, err := findYKConfigMap(tc.configMaps)
			if tc.expectedError {
				assert.Assert(t, err != nil, "Error is expected")
			} else {
				assert.Assert(t, configMap.Name == constants.DefaultConfigMapName, "Returned configmap is wrong")
				assert.Assert(t, len(configMap.Data) == 1, "Returned configmap has unexpected data")
				assert.Assert(t, configMap.Name == constants.DefaultConfigMapName, "Returned configmap is wrong")
				assert.Assert(t, configMap.Data["queues.yaml"] == "OldData", "Old configmap value is wrong")
			}
		})
	}
}

func TestSaveConfigmap(t *testing.T) {
	// YK configmap not found
	context := initContextForTest()
	newConf := si.UpdateConfigurationRequest{
		Configs: "newConfig",
	}
	resp := context.SaveConfigmap(&newConf)
	assert.Equal(t, false, resp.Success, "Successful update expected")
	assert.Assert(t, strings.Contains(resp.Reason, "not found"), "Unexpected reason returned")

	// successful update
	configMaps, err := context.apiProvider.GetAPIs().ConfigMapInformer.Lister().List(nil)
	assert.NilError(t, err, "No error expected")
	for _, c := range configMaps {
		_, err := context.apiProvider.GetAPIs().KubeClient.GetClientSet().CoreV1().ConfigMaps(c.Namespace).Create(c)
		assert.NilError(t, err, "No error expected")
	}
	resp = context.SaveConfigmap(&newConf)
	assert.Equal(t, true, resp.Success, "Successful update expected")

	//hot-refresh enabled
	context.apiProvider.GetAPIs().Conf.EnableConfigHotRefresh = true
	resp = context.SaveConfigmap(&newConf)
	assert.Equal(t, false, resp.Success, "Failure is expected")
	assert.Assert(t, strings.Contains(resp.Reason, "hot-refresh is enabled"), "Unexpected reason")
}
