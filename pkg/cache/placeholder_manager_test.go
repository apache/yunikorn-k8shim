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
	"sync"
	"sync/atomic"
	"testing"

	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
)

func TestCreateAppPlaceholders(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
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

	createdPods := make(map[string]*v1.Pod)
	mockedAPIProvider := client.NewMockedAPIProvider()
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods[pod.Name] = pod
		return pod, nil
	})
	placeholderMgr := &PlaceholderManager{
		clients: mockedAPIProvider.GetAPIs(),
		RWMutex: sync.RWMutex{},
	}

	err := placeholderMgr.createAppPlaceholders(app)
	assert.NilError(t, err, "create app placeholders should be successful")
	assert.Equal(t, len(createdPods), 30)

	// simulate placeholder creation failures
	// failed to create one placeholder
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		if pod.Name == "tg-test-group-2-app01-15" {
			return nil, fmt.Errorf("failed to create pod %s", pod.Name)
		}
		return pod, nil
	})
	err = placeholderMgr.createAppPlaceholders(app)
	assert.Error(t, err, "failed to create pod tg-test-group-2-app01-15")
}

func TestCleanUp(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
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
	placeholderMgr := &PlaceholderManager{
		clients:   mockedAPIProvider.GetAPIs(),
		orphanPod: make(map[string]*v1.Pod),
		RWMutex:   sync.RWMutex{},
	}
	placeholderMgr.CleanUp(app)

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
	assert.Equal(t, len(placeholderMgr.orphanPod), 0)
}

func TestCleanOrphanPlaceholders(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider()
	placeholderMgr := &PlaceholderManager{
		clients:   mockedAPIProvider.GetAPIs(),
		orphanPod: make(map[string]*v1.Pod),
		RWMutex:   sync.RWMutex{},
	}
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
	placeholderMgr.orphanPod["task01"] = pod1
	assert.Equal(t, len(placeholderMgr.orphanPod), 1)
	placeholderMgr.cleanOrphanPlaceholders()
	assert.Equal(t, len(placeholderMgr.orphanPod), 0)
}

func TestPlaceholderManagerStartStop(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider()
	placeholderMgr := &PlaceholderManager{
		clients:   mockedAPIProvider.GetAPIs(),
		orphanPod: make(map[string]*v1.Pod),
		running:   atomic.Value{},
		RWMutex:   sync.RWMutex{},
	}
	placeholderMgr.setRunning(false)
	// start clean up goroutine
	placeholderMgr.Start()
	assert.Equal(t, placeholderMgr.isRunning(), true)

	placeholderMgr.Stop()
	// check orphan pod map is empty
	assert.Equal(t, placeholderMgr.isRunning(), false)
}
