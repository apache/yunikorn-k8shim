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
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

type concurrentKubeClient struct {
	client.KubeClient
	createFn func(ctx context.Context, pod *v1.Pod) (*v1.Pod, error)
	deleteFn func(ctx context.Context, pod *v1.Pod) error
}

func (c *concurrentKubeClient) Create(ctx context.Context, pod *v1.Pod) (*v1.Pod, error) {
	return c.createFn(ctx, pod)
}

func (c *concurrentKubeClient) Delete(ctx context.Context, pod *v1.Pod) error {
	return c.deleteFn(ctx, pod)
}

func assertPlaceholderManagerStopped(t *testing.T, mgr *PlaceholderManager, stopped bool) {
	t.Helper()
	select {
	case <-mgr.doneChan:
		assert.Assert(t, stopped, "placeholder manager stopped unexpectedly")
	default:
		assert.Assert(t, !stopped, "placeholder manager has not stopped")
	}
}

func TestNewPlaceholderManager(t *testing.T) {
	// the shim does not startup if it cannot get a kubeclient, no need to check for nil
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	assert.Equal(t, mgr.started.Load(), false, "new manager should not be started")
	assertPlaceholderManagerStopped(t, mgr, false)
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

func TestPlaceholderCreationAndCleanupAreSerialized(t *testing.T) {
	app := createAppWIthTaskGroupAndPodsForTest()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	createStarted := make(chan struct{})
	continueCreate := make(chan struct{})
	deleteStarted := make(chan struct{})
	var createOnce sync.Once
	var deleteOnce sync.Once
	kubeClient := &concurrentKubeClient{
		KubeClient: mockedAPIProvider.GetAPIs().KubeClient,
		createFn: func(_ context.Context, pod *v1.Pod) (*v1.Pod, error) {
			createOnce.Do(func() {
				close(createStarted)
				<-continueCreate
			})
			return pod, nil
		},
		deleteFn: func(_ context.Context, _ *v1.Pod) error {
			deleteOnce.Do(func() {
				close(deleteStarted)
			})
			return nil
		},
	}
	mockedAPIProvider.GetAPIs().KubeClient = kubeClient
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())

	createDone := make(chan struct{})
	go func() {
		defer close(createDone)
		assert.NilError(t, mgr.createAppPlaceholders(app))
	}()
	select {
	case <-createStarted:
	case <-time.After(time.Second):
		t.Fatal("placeholder creation did not start")
	}

	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		mgr.cleanUp(app)
	}()
	select {
	case <-deleteStarted:
		close(continueCreate)
		<-createDone
		<-cleanupDone
		t.Fatal("placeholder cleanup started before creation completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(continueCreate)
	select {
	case <-createDone:
	case <-time.After(time.Second):
		t.Fatal("placeholder creation did not complete")
	}
	select {
	case <-deleteStarted:
	case <-time.After(time.Second):
		t.Fatal("placeholder cleanup did not start after creation completed")
	}
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("placeholder cleanup did not complete")
	}
}

func TestStopCancelsApplicationCleanup(t *testing.T) {
	app := createAppWIthTaskGroupAndPodsForTest()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	deleteStarted := make(chan struct{})
	deleteCanceled := make(chan struct{})
	continueDelete := make(chan struct{})
	var deleteOnce sync.Once
	mockedAPIProvider.MockDeleteWithContextFn(func(ctx context.Context, _ *v1.Pod) error {
		deleteOnce.Do(func() {
			close(deleteStarted)
			<-ctx.Done()
			close(deleteCanceled)
			<-continueDelete
		})
		return ctx.Err()
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()

	app.handleCompleteApplicationEvent()
	select {
	case <-deleteStarted:
	case <-time.After(time.Second):
		t.Fatal("placeholder cleanup did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-deleteCanceled:
	case <-time.After(time.Second):
		t.Fatal("application cleanup was not canceled during manager shutdown")
	}
	select {
	case <-stopDone:
		close(continueDelete)
		t.Fatal("manager stopped before application cleanup completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(continueDelete)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after application cleanup completed")
	}
}

func TestApplicationCleanupRejectedOutsideManagerLifecycle(t *testing.T) {
	app := createAppWIthTaskGroupAndPodsForTest()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	var deletes atomic.Int32
	mockedAPIProvider.MockDeleteWithContextFn(func(_ context.Context, _ *v1.Pod) error {
		deletes.Add(1)
		return nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())

	app.handleCompleteApplicationEvent()
	assert.Equal(t, deletes.Load(), int32(0), "cleanup should not start before the manager starts")

	mgr.Start()
	mgr.Stop()
	app.handleCompleteApplicationEvent()
	assert.Equal(t, deletes.Load(), int32(0), "cleanup should not start after the manager stops")
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
	assert.Equal(t, mgr.started.Load(), false, "new manager should not be started")
	// start clean up goroutine
	mgr.Start()
	assert.Equal(t, mgr.started.Load(), true, "manager should be started")
	assertPlaceholderManagerStopped(t, mgr, false)

	// starting a second time should do nothing
	mgr.Start()
	assertPlaceholderManagerStopped(t, mgr, false)

	// Stop waits until the cleanup goroutine has exited.
	mgr.Stop()
	assertPlaceholderManagerStopped(t, mgr, true)
	// Repeated stops are safe and return immediately.
	mgr.Stop()
	assertPlaceholderManagerStopped(t, mgr, true)
	// A stopped manager is one-shot and cannot be restarted.
	mgr.Start()
	assertPlaceholderManagerStopped(t, mgr, true)

	// make sure stop doesn't do anything before the manager starts
	mgr = NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	assert.Equal(t, mgr.started.Load(), false, "new manager should not be started")
	mgr.Stop()
	assertPlaceholderManagerStopped(t, mgr, false)
	mgr.Start()
	assert.Equal(t, mgr.started.Load(), true, "manager should start after a pre-start Stop")
	assertPlaceholderManagerStopped(t, mgr, false)
	// lets stop it again now things should stop correctly
	mgr.Stop()
	assertPlaceholderManagerStopped(t, mgr, true)
}

func TestPlaceholderManagerConcurrentStop(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()

	const callers = 32
	start := make(chan struct{})
	var stopped sync.WaitGroup
	stopped.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer stopped.Done()
			<-start
			mgr.Stop()
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() {
		stopped.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent PlaceholderManager.Stop calls blocked")
	}
	assertPlaceholderManagerStopped(t, mgr, true)
}

func TestPlaceholderManagerConcurrentStartCreatesOneCleanupLoop(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	var deletes atomic.Int32
	mockedAPIProvider.MockDeleteFn(func(_ *v1.Pod) error {
		deletes.Add(1)
		return nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.setCleanupTime(time.Millisecond)

	const callers = 64
	start := make(chan struct{})
	var started sync.WaitGroup
	started.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer started.Done()
			<-start
			mgr.Start()
		}()
	}
	close(start)
	started.Wait()
	mgr.Stop()

	mgr.Lock()
	mgr.orphanPods["task01"] = &v1.Pod{ObjectMeta: apis.ObjectMeta{Name: "pod-01"}}
	mgr.Unlock()
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, deletes.Load(), int32(0), "an extra cleanup loop remained after Stop")
}

func TestPlaceholderManagerStopCancelsBlockedCleanup(t *testing.T) {
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	deleteStarted := make(chan struct{})
	mockedAPIProvider.MockDeleteWithContextFn(func(ctx context.Context, _ *v1.Pod) error {
		close(deleteStarted)
		<-ctx.Done()
		return ctx.Err()
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.setCleanupTime(time.Millisecond)
	mgr.orphanPods["task01"] = &v1.Pod{ObjectMeta: apis.ObjectMeta{Name: "pod-01"}}
	mgr.Start()

	select {
	case <-deleteStarted:
	case <-time.After(time.Second):
		t.Fatal("orphan cleanup did not start")
	}

	stopped := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the blocked orphan deletion")
	}
	assertPlaceholderManagerStopped(t, mgr, true)
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
	mgr.Start()
	assertPlaceholderManagerStopped(t, mgr, false)
	mgr.Lock()
	mgr.orphanPods["task01"] = pod1
	mgr.orphanPods["task02"] = pod2
	mgr.Unlock()
	assert.Equal(t, mgr.getOrphanPodsLength(), 2)
	<-time.After(100 * time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, mgr.getOrphanPodsLength(), 0)
	mgr.Stop()
	assertPlaceholderManagerStopped(t, mgr, true)
}

func TestPlaceholderManagerStopWaitsForCreation(t *testing.T) {
	app := createAppWIthTaskGroupAndPodsForTest()
	provider := client.NewMockedAPIProvider(false)
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	var once sync.Once
	provider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		once.Do(func() {
			close(createStarted)
			<-releaseCreate
		})
		return pod, nil
	})
	mgr := NewPlaceholderManager(provider.GetAPIs())
	mgr.Start()
	createDone := make(chan struct{})
	go func() {
		defer close(createDone)
		assert.ErrorIs(t, mgr.createAppPlaceholders(app), context.Canceled)
	}()
	defer func() {
		close(releaseCreate)
		<-createDone
		mgr.Stop()
	}()
	select {
	case <-createStarted:
	case <-time.After(time.Second):
		t.Fatal("creation did not start")
	}
	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-mgr.lifecycleCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the lifecycle context")
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned while placeholder creation was still active")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPlaceholderManagerOperationsAfterStop(t *testing.T) {
	app := createAppWIthTaskGroupAndPodsForTest()
	provider := client.NewMockedAPIProvider(false)
	var creates, deletes atomic.Int32
	provider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		creates.Add(1)
		return pod, nil
	})
	provider.MockDeleteFn(func(_ *v1.Pod) error {
		deletes.Add(1)
		return nil
	})
	mgr := NewPlaceholderManager(provider.GetAPIs())
	mgr.Start()
	mgr.Stop()
	assert.ErrorIs(t, mgr.createAppPlaceholders(app), context.Canceled)
	mgr.cleanUp(app)
	mgr.orphanPods["orphan"] = &v1.Pod{}
	mgr.cleanOrphanPlaceholders()
	assert.Equal(t, creates.Load(), int32(0), "creation must reject work after shutdown")
	assert.Equal(t, deletes.Load(), int32(0), "cleanup must reject work after shutdown")
}

func TestPlaceholderManagerStopCancelsCreationWithQueuedCleanup(t *testing.T) {
	app := createAppWIthTaskGroupAndPodsForTest()
	provider := client.NewMockedAPIProvider(false)
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	var deletes atomic.Int32
	provider.GetAPIs().KubeClient = &concurrentKubeClient{
		KubeClient: provider.GetAPIs().KubeClient,
		createFn: func(ctx context.Context, _ *v1.Pod) (*v1.Pod, error) {
			close(createStarted)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseCreate:
				return nil, fmt.Errorf("test released blocked Create")
			}
		},
		deleteFn: func(_ context.Context, _ *v1.Pod) error {
			deletes.Add(1)
			return nil
		},
	}
	mgr := NewPlaceholderManager(provider.GetAPIs())
	mgr.Start()
	createDone := make(chan error, 1)
	go func() { createDone <- mgr.createAppPlaceholders(app) }()
	defer func() {
		close(releaseCreate)
		mgr.Stop()
	}()
	select {
	case <-createStarted:
	case <-time.After(time.Second):
		t.Fatal("creation did not start")
	}
	cleanupDone := make(chan struct{})
	go func() {
		mgr.cleanUp(app)
		close(cleanupDone)
	}()
	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel creation with pending application cleanup")
	}
	select {
	case err := <-createDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("creation did not return")
	}
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("queued cleanup did not return")
	}
	assert.Equal(t, deletes.Load(), int32(0), "queued cleanup must observe cancellation before Delete")
}
