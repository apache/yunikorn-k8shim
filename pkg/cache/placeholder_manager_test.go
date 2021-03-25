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
	"testing"
	"time"

	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
)

const (
	appID     = "app01"
	queue     = "root.default"
	namespace = "test"
)

func TestNewPlaceholderManager(t *testing.T) {
	// the shim does not startup if it cannot get a kubeclient, no need to check for nil
	mockedAPIProvider := client.NewMockedAPIProvider()
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
	mockedAPIProvider := client.NewMockedAPIProvider()
	createdPods := createAndCheckPlaceholderCreate(mockedAPIProvider, app, t)
	for _, pod := range createdPods {
		assert.Assert(t, len(pod.OwnerReferences) == 0, "By default the pod should not have owner reference set")
	}

	// simulate placeholder creation failures
	// failed to create one placeholder
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		if pod.Name == "tg-test-group-2-app01-15" {
			return nil, fmt.Errorf("failed to create pod %s", pod.Name)
		}
		return pod, nil
	})
	err := placeholderMgr.createAppPlaceholders(app)
	assert.Error(t, err, "failed to create pod tg-test-group-2-app01-15")
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
	app.setOwnReferences([]apis.OwnerReference{ownRef})
	mockedAPIProvider := client.NewMockedAPIProvider()
	pods := createAndCheckPlaceholderCreate(mockedAPIProvider, app, t)
	for _, pod := range pods {
		assert.Assert(t, len(pod.OwnerReferences) == 1, "The pod should have exactly one owner reference set")
		assert.Assert(t, *pod.OwnerReferences[0].Controller == false, "The owner reference should not be a controller")
		assert.Equal(t, pod.OwnerReferences[0].Name, ownRef.Name, "The owner reference name does not match")
		assert.Equal(t, pod.OwnerReferences[0].UID, ownRef.UID, "The owner reference UID does not match")
	}
}

func createAppWIthTaskGroupForTest() *Application {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]v1alpha1.TaskGroup{
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

func TestCleanUp(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	mockedContext.applications[appID] = app
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
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 3)

	deletePod := make([]string, 0)
	mockedAPIProvider := client.NewMockedAPIProvider()
	mockedAPIProvider.MockDeleteFn(func(pod *v1.Pod) error {
		deletePod = append(deletePod, pod.Name)
		return nil
	})
	placeholderMgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	placeholderMgr.cleanUp(app)

	// check both pod-01 and pod-02 in deletePod list and pod-03 isn't contain
	assert.Assert(t, is.Contains(deletePod, "pod-01"))
	assert.Assert(t, is.Contains(deletePod, "pod-02"))
	exist := false
	for _, item := range deletePod {
		if item == "pod-03" {
			exist = true
		}
	}
	assert.Equal(t, exist, false)
	assert.Equal(t, len(placeholderMgr.orphanPods), 0)
}

func TestCleanOrphanPlaceholders(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider()
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
	placeholderMgr.orphanPods["task01"] = pod1
	assert.Equal(t, len(placeholderMgr.orphanPods), 1)
	placeholderMgr.cleanOrphanPlaceholders()
	assert.Equal(t, len(placeholderMgr.orphanPods), 0)
}

func TestPlaceholderManagerStartStop(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider()
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
