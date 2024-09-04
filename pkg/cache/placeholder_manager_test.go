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
	"fmt"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

const (
	pmAppID           = "app01"
	queue             = "root.default"
	namespace         = "test"
	priorityClassName = "test-priority-class"
)

func TestNewPlaceholderManager(t *testing.T) {
	// the shim does not startup if it cannot get a kubeclient, no need to check for nil
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	assert.Equal(t, mgr.running.Load(), false, "new manager should not run")
	if mgr.orphanPods == nil && len(mgr.orphanPods) != 0 {
		t.Fatal("orphanPods map should be initialised and empty")
	}
	if mgr.stopChan == nil {
		t.Fatal("stop channel should be initialised")
	}
}

func TestCreateAppPlaceholders(t *testing.T) {
	app := createAppWIthTaskGroupForTest()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	createdPods := createAndCheckPlaceholderCreate(mockedAPIProvider, app, t)
	for _, pod := range createdPods {
		assert.Assert(t, len(pod.OwnerReferences) == 0, "By default the pod should not have owner reference set")
	}

	// simulate placeholder creation failures
	// failed to create one placeholder
	var failed string
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		if failed == "" && strings.HasPrefix(pod.Name, "tg-app01-test-group-2-") {
			failed = pod.Name
			return nil, fmt.Errorf("failed to create pod %s", pod.Name)
		}
		return pod, nil
	})
	err := placeholderMgr.createAppPlaceholders(app)
	assert.Error(t, err, fmt.Sprintf("failed to create pod %s", failed))
}

func TestCreateAppPlaceholdersWithExistingPods(t *testing.T) {
	createdPods := make(map[string]*v1.Pod)
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods[pod.Name] = pod
		return pod, nil
	})

	placeholderMgr = NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	app := createAppWIthTaskGroupAndPodsForTest()
	err := placeholderMgr.createAppPlaceholders(app)
	assert.NilError(t, err)
	assert.Equal(t, 27, len(createdPods))
	for _, pod := range createdPods {
		assert.Equal(t, pod.Spec.PriorityClassName, "test-priority-class", "Pod should have PriorityClassName of test-priority-class")
	}
}

func createAndCheckPlaceholderCreate(mockedAPIProvider *client.MockedAPIProvider, app *Application, t *testing.T) map[string]*v1.Pod {
	createdPods := make(map[string]*v1.Pod)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods[pod.Name] = pod
		return pod, nil
	})
	placeholderMgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())

	err := placeholderMgr.createAppPlaceholders(app)
	assert.NilError(t, err, "create app placeholders should be successful")
	assert.Equal(t, len(createdPods), 30)
	for _, pod := range createdPods {
		assert.Equal(t, "", pod.Spec.PriorityClassName, "PriorityClassName should be empty")
	}
	return createdPods
}

func TestCreateAppPlaceholdersWithOwnReference(t *testing.T) {
	app := createAppWIthTaskGroupForTest()
	controller := true
	ownRef := apis.OwnerReference{
		Name:       "JobId",
		UID:        "JobUid",
		Controller: &controller,
	}
	app.setPlaceholderOwnerReferences([]apis.OwnerReference{ownRef})
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	pods := createAndCheckPlaceholderCreate(mockedAPIProvider, app, t)
	for _, pod := range pods {
		assert.Assert(t, len(pod.OwnerReferences) == 1, "The pod should have exactly one owner reference set")
		assert.Equal(t, pod.OwnerReferences[0].Name, ownRef.Name, "The owner reference name does not match")
		assert.Equal(t, pod.OwnerReferences[0].UID, ownRef.UID, "The owner reference UID does not match")
	}
}

func createAppWIthTaskGroupForTest() *Application {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(pmAppID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("1024M"),
			},
		},
		{
			Name:      "test-group-2",
			MinMember: 20,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("1000m"),
				"memory": resource.MustParse("2048M"),
			},
		},
	})
	return app
}

func createAppWIthTaskGroupAndPodsForTest() *Application {
	app := createAppWIthTaskGroupForTest()
	mockedContext := initContextForTest()
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "tg-app01-test-group-1-0",
			UID:  "UID-01",
		},
		Spec: v1.PodSpec{
			PriorityClassName: priorityClassName,
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "tg-app01-test-group-1-1",
			UID:  "UID-02",
		},
	}
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "tg-app01-test-group-2-0",
			UID:  "UID-03",
		},
	}

	taskID1 := "task1-01"
	task1 := NewTask(taskID1, app, mockedContext, pod1)
	task1.placeholder = true
	task1.pod = pod1
	task1.originator = true
	task1.setTaskGroupName("test-group-1")
	app.taskMap[taskID1] = task1
	app.setOriginatingTask(task1)

	taskID2 := "task1-02"
	task2 := NewTask(taskID2, app, mockedContext, pod2)
	task2.placeholder = true
	task2.pod = pod2
	task2.setTaskGroupName("test-group-1")
	app.taskMap[taskID2] = task2

	taskID3 := "task2-01"
	task3 := NewTask(taskID3, app, mockedContext, pod3)
	task3.placeholder = true
	task3.pod = pod3
	task3.setTaskGroupName("test-group-2")
	app.taskMap[taskID3] = task3

	return app
}

func TestCleanUp(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(pmAppID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	mockedContext.applications[pmAppID] = app
	res := app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 0)

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-02",
			UID:  "UID-02",
		},
	}
	pod3 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-03",
			UID:  "UID-03",
		},
	}
	pod4 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-04",
			UID:  "UID-04",
		},
	}
	taskID1 := "task01"
	task1 := NewTask(taskID1, app, mockedContext, pod1)
	task1.placeholder = true
	app.taskMap[taskID1] = task1
	taskID2 := "task02"
	task2 := NewTask(taskID2, app, mockedContext, pod2)
	task2.placeholder = true
	app.taskMap[taskID2] = task2
	taskID3 := "task03"
	task3 := NewTask(taskID3, app, mockedContext, pod3)
	task3.placeholder = false
	app.taskMap[taskID3] = task3
	taskID4 := "task04"
	task4 := NewTask(taskID4, app, mockedContext, pod4)
	task4.placeholder = true
	app.taskMap[taskID4] = task4
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 4)

	deletePod := make([]string, 0)
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockDeleteFn(func(pod *v1.Pod) error {
		if pod.Name == "pod-04" {
			return fmt.Errorf("error")
		}
		deletePod = append(deletePod, pod.Name)
		return nil
	})
	placeholderMgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	placeholderMgr.cleanUp(app)

	// check both pod-01 and pod-02 in DeletePod list and pod-03 isn't contain
	assert.Assert(t, is.Contains(deletePod, "pod-01"))
	assert.Assert(t, is.Contains(deletePod, "pod-02"))
	exist := false
	for _, item := range deletePod {
		if item == "pod-03" {
			exist = true
		}
	}
	assert.Equal(t, exist, false)
	assert.Equal(t, len(placeholderMgr.orphanPods), 1)
}

func TestCleanOrphanPlaceholders(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockDeleteFn(func(pod *v1.Pod) error {
		if pod.Name == "pod-02" {
			return fmt.Errorf("error")
		}
		return nil
	})
	placeholderMgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-02",
			UID:  "UID-02",
		},
	}
	placeholderMgr.orphanPods["task01"] = pod1
	placeholderMgr.orphanPods["task02"] = pod2
	assert.Equal(t, len(placeholderMgr.orphanPods), 2)
	placeholderMgr.cleanOrphanPlaceholders()
	assert.Equal(t, len(placeholderMgr.orphanPods), 1)
}

func TestPlaceholderManagerStartStop(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	assert.Equal(t, mgr.running.Load(), false, "new manager should not run")
	// start clean up goroutine
	mgr.Start()
	assert.Equal(t, mgr.isRunning(), true, "manager should be running after start")

	// starting a second time should do nothing
	mgr.Start()
	assert.Equal(t, mgr.isRunning(), true, "sending start 2nd time should not do anything")

	// this is a blocking call
	mgr.Stop()
	// allow time for processing as the call can return faster than the flag is set
	time.Sleep(5 * time.Millisecond)
	// check manager has stopped
	assert.Equal(t, mgr.isRunning(), false, "placeholder manager has not stopped")
	// this should not block anymore, flag should be set
	mgr.Stop()
	assert.Equal(t, mgr.isRunning(), false, "stopping already stopped manager failed")

	// make sure stop doesn't do anything on a non running manager
	mgr = NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	assert.Equal(t, mgr.running.Load(), false, "new manager should not run")
	mgr.Stop()
	assert.Equal(t, mgr.isRunning(), false, "stopping new manager: nothing should happen")
	mgr.Start()
	assert.Equal(t, mgr.isRunning(), true, "manager should be running after start (stop start sequence)")
	// lets stop it again now things should stop correctly
	mgr.Stop()
	// allow time for processing as the call can return faster than the flag is set
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, mgr.isRunning(), false, "placeholder manager has not stopped")
}

func TestPlaceholderManagerCleanup(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-02",
			UID:  "UID-02",
		},
	}
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.setCleanupTime(100 * time.Millisecond)
	defer mgr.setCleanupTime(5 * time.Second)
	mgr.Start()
	assert.Equal(t, mgr.isRunning(), true, "manager should be running after start")
	mgr.orphanPods["task01"] = pod1
	mgr.orphanPods["task02"] = pod2
	assert.Equal(t, mgr.getOrphanPodsLength(), 2)
	<-time.After(100 * time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, mgr.getOrphanPodsLength(), 0)
	mgr.Stop()
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, mgr.isRunning(), false, "placeholder manager has stopped")
}
