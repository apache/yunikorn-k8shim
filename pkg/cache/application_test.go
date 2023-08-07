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
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sEvents "k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/general"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type recorderTime struct {
	time int64
	lock *sync.RWMutex
}

func TestNewApplication(t *testing.T) {
	app := NewApplication("app00001", "root.queue", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, app.partition, constants.DefaultPartition)
	assert.Equal(t, len(app.taskMap), 0)
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, app.queue, "root.queue")
	assert.Assert(t, reflect.DeepEqual(app.groups, []string{"dev", "yunikorn"}))
}

func TestSubmitApplication(t *testing.T) {
	app := NewApplication("app00001", "root.abc", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	err := app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Submitted, 10*time.Second)

	// app already submitted
	err = app.handle(NewSubmitApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, ApplicationStates().Submitted, 10*time.Second)
}

func TestRunApplication(t *testing.T) {
	ms := &mockSchedulerAPI{}
	ms.UpdateApplicationFn = func(request *si.ApplicationRequest) error {
		assert.Equal(t, len(request.New), 1)
		assert.Equal(t, request.New[0].ApplicationID, "app00001")
		assert.Equal(t, request.New[0].QueueName, "root.abc")
		return nil
	}

	app := NewApplication("app00001", "root.abc", "testuser", testGroups, map[string]string{}, ms)

	// app must be submitted before being able to run
	err := app.handle(NewRunApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, ApplicationStates().New, 3*time.Second)

	// submit the app
	err = app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Submitted, 3*time.Second)

	// app must be accepted first
	err = app.handle(NewRunApplicationEvent(app.applicationID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app, ApplicationStates().Submitted, 3*time.Second)
}

func TestFailApplication(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// inject the mocked clients to the placeholder manager
	createdPods := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods.add(pod)
		return pod, nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()
	defer mgr.Stop()

	rt := &recorderTime{
		time: int64(0),
		lock: &sync.RWMutex{},
	}
	ms := &mockSchedulerAPI{}
	// set test mode
	conf.GetSchedulerConf().SetTestMode(true)
	// set Recorder to mocked type
	mr := events.NewMockedRecorder()
	mr.OnEventf = func() {
		rt.lock.Lock()
		defer rt.lock.Unlock()
		rt.time++
	}
	events.SetRecorder(mr)
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	appID := "app-test-001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, ms)
	task1 := NewTask("task01", app, context, pod)
	task2 := NewTask("task02", app, context, pod)
	task3 := NewTask("task03", app, context, pod)
	task4 := NewTask("task04", app, context, pod)
	// set task states to new/pending/scheduling/running
	task1.sm.SetState(TaskStates().New)
	task2.sm.SetState(TaskStates().Pending)
	task3.sm.SetState(TaskStates().Scheduling)
	task4.sm.SetState(TaskStates().Allocated)
	app.addTask(task1)
	app.addTask(task2)
	app.addTask(task3)
	app.addTask(task4)
	app.SetState(ApplicationStates().Accepted)
	errMess := "Test Error Message"
	err := app.handle(NewFailApplicationEvent(app.applicationID, errMess))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Failing, 3*time.Second)
	err = app.handle(NewFailApplicationEvent(app.applicationID, errMess))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Failed, 3*time.Second)
	assert.Equal(t, rt.time, int64(6))
	// reset time to 0
	rt.time = 0
	appID2 := "app-test-002"
	app2 := NewApplication(appID2, "root.abc", "testuser", testGroups, map[string]string{}, ms)
	app2.SetState(ApplicationStates().New)
	err = app2.handle(NewFailApplicationEvent(app2.applicationID, errMess))
	if err == nil {
		t.Error("expecting error got 'nil'")
	}
	assertAppState(t, app2, ApplicationStates().New, 3*time.Second)
	app2.SetState(ApplicationStates().Submitted)
	err = app2.handle(NewFailApplicationEvent(app2.applicationID, errMess))
	assert.NilError(t, err)
	assertAppState(t, app2, ApplicationStates().Failing, 3*time.Second)
	err = app2.handle(NewFailApplicationEvent(app2.applicationID, errMess))
	assert.NilError(t, err)
	assertAppState(t, app2, ApplicationStates().Failed, 3*time.Second)
	assert.Equal(t, rt.time, int64(0))
	// Test over, set Recorder back fake type
	events.SetRecorder(k8sEvents.NewFakeRecorder(1024))
}

func TestSetUnallocatedPodsToFailedWhenFailApplication(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// inject the mocked clients to the placeholder manager
	createdPods := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods.add(pod)
		return pod, nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()
	defer mgr.Stop()

	mockClient := mockedAPIProvider.GetAPIs().KubeClient
	context.apiProvider.GetAPIs().KubeClient = mockClient

	ms := &mockSchedulerAPI{}
	// set test mode
	conf.GetSchedulerConf().SetTestMode(true)
	// set Recorder to mocked type
	mr := events.NewMockedRecorder()
	events.SetRecorder(mr)
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod1, err := mockClient.Create(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})
	assert.NilError(t, err)
	pod2, err := mockClient.Create(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})
	assert.NilError(t, err)
	pod3, err := mockClient.Create(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00003",
			UID:  "UID-00003",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})
	assert.NilError(t, err)
	appID := "app-test-001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, ms)
	task1 := NewTask("task01", app, context, pod1)
	task2 := NewTaskPlaceholder("task02", app, context, pod2)
	task3 := NewTask("task03", app, context, pod3)
	task1.sm.SetState(TaskStates().Pending)
	task2.sm.SetState(TaskStates().Scheduling)
	task3.sm.SetState(TaskStates().Scheduling)
	app.addTask(task1)
	app.addTask(task2)
	app.addTask(task3)
	app.SetState(ApplicationStates().Accepted)
	errMess := constants.ApplicationInsufficientResourcesFailure
	err = app.handle(NewFailApplicationEvent(app.applicationID, errMess))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Failing, 3*time.Second)
	err = app.handle(NewFailApplicationEvent(app.applicationID, errMess))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Failed, 3*time.Second)

	// Note that the status of pod 2, a placeholder pod, doesn't matter because it will be cleaned up
	newPod1, err := mockClient.Get(pod1.Namespace, pod1.Name)
	assert.NilError(t, err)
	assert.Equal(t, newPod1.Status.Phase, v1.PodFailed, 3*time.Second)
	assert.Equal(t, newPod1.Status.Reason, constants.ApplicationInsufficientResourcesFailure, 3*time.Second)
	newPod3, err := mockClient.Get(pod3.Namespace, pod3.Name)
	assert.NilError(t, err)
	assert.Equal(t, newPod3.Status.Phase, v1.PodFailed, 3*time.Second)
	assert.Equal(t, newPod3.Status.Reason, constants.ApplicationInsufficientResourcesFailure, 3*time.Second)
	// Test over, set Recorder back fake type
	events.SetRecorder(k8sEvents.NewFakeRecorder(1024))
}

func TestSetUnallocatedPodsToFailedWhenRejectApplication(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// inject the mocked clients to the placeholder manager
	createdPods := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods.add(pod)
		return pod, nil
	})

	mockClient := mockedAPIProvider.GetAPIs().KubeClient
	context.apiProvider.GetAPIs().KubeClient = mockClient
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()
	defer mgr.Stop()

	ms := &mockSchedulerAPI{}
	// set test mode
	conf.GetSchedulerConf().SetTestMode(true)
	// set Recorder to mocked type
	mr := events.NewMockedRecorder()
	events.SetRecorder(mr)
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod1, err := mockClient.Create(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})
	assert.NilError(t, err)
	pod2, err := mockClient.Create(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})
	assert.NilError(t, err)
	appID := "app-test-001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, ms)
	task1 := NewTask("task01", app, context, pod1)
	task2 := NewTask("task02", app, context, pod2)
	task1.sm.SetState(TaskStates().Pending)
	task2.sm.SetState(TaskStates().Pending)
	app.addTask(task1)
	app.addTask(task2)
	app.SetState(ApplicationStates().Submitted)
	context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: app.applicationID,
			QueueName:     app.queue,
			User:          app.user,
			Tags:          app.tags,
		},
	})
	errMess := "app rejected"
	err = app.handle(NewApplicationEvent(app.applicationID, RejectApplication, errMess))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Rejected, 3*time.Second)

	err = app.handle(NewFailApplicationEvent(app.applicationID,
		fmt.Sprintf("%s: %s", constants.ApplicationRejectedFailure, errMess)))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Failed, 3*time.Second)

	newPod1, err := mockClient.Get(pod1.Namespace, pod1.Name)
	assert.NilError(t, err)
	assert.Equal(t, newPod1.Status.Phase, v1.PodFailed, 3*time.Second)
	assert.Equal(t, newPod1.Status.Reason, constants.ApplicationRejectedFailure, 3*time.Second)
	newPod2, err := mockClient.Get(pod2.Namespace, pod2.Name)
	assert.NilError(t, err)
	assert.Equal(t, newPod2.Status.Phase, v1.PodFailed, 3*time.Second)
	assert.Equal(t, newPod2.Status.Reason, constants.ApplicationRejectedFailure, 3*time.Second)
	// Test over, set Recorder back fake type
	events.SetRecorder(k8sEvents.NewFakeRecorder(1024))
}

func TestReleaseAppAllocation(t *testing.T) {
	context := initContextForTest()
	ms := &mockSchedulerAPI{}
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	appID := "app-test-001"
	UUID := "testUUID001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, ms)
	task := NewTask("task01", app, context, pod)
	app.addTask(task)
	task.allocationUUID = UUID
	// app must be running states
	err := app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	// set app states to running, let event can be trigger
	app.SetState(ApplicationStates().Running)
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)
	err = app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	assert.NilError(t, err)
	// after handle release event the states of app must be running
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)
}

func newMockSchedulerAPI() *mockSchedulerAPI {
	return &mockSchedulerAPI{
		registerFn: func(request *si.RegisterResourceManagerRequest, callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, nil
		},
		UpdateAllocationFn: func(request *si.AllocationRequest) error {
			return nil
		},
		UpdateApplicationFn: func(request *si.ApplicationRequest) error {
			return nil
		},
		UpdateNodeFn: func(request *si.NodeRequest) error {
			return nil
		},
		UpdateConfigurationFn: func(request *si.UpdateConfigurationRequest) error {
			return nil
		},
	}
}

type mockSchedulerAPI struct {
	callback   api.ResourceManagerCallback //nolint:structcheck,unused
	registerFn func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	UpdateAllocationFn    func(request *si.AllocationRequest) error
	UpdateApplicationFn   func(request *si.ApplicationRequest) error
	UpdateNodeFn          func(request *si.NodeRequest) error
	UpdateConfigurationFn func(request *si.UpdateConfigurationRequest) error
}

func (ms *mockSchedulerAPI) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
	return ms.registerFn(request, callback)
}

func (ms *mockSchedulerAPI) UpdateAllocation(request *si.AllocationRequest) error {
	return ms.UpdateAllocationFn(request)
}

func (ms *mockSchedulerAPI) UpdateApplication(request *si.ApplicationRequest) error {
	return ms.UpdateApplicationFn(request)
}

func (ms *mockSchedulerAPI) UpdateNode(request *si.NodeRequest) error {
	return ms.UpdateNodeFn(request)
}

func (ms *mockSchedulerAPI) UpdateConfiguration(request *si.UpdateConfigurationRequest) error {
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

func TestGetNonTerminatedTaskAlias(t *testing.T) {
	context := initContextForTest()
	appID := "app00001"
	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	context.addApplication(app)
	// app doesn't have any task
	res := app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 0)

	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "test-00001",
			UID:  "UID-00001",
		},
	}
	pod2 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "test-00002",
			UID:  "UID-00002",
		},
	}
	// set two task to non-terminated states
	taskID1 := "task01"
	task1 := NewTask(taskID1, app, context, pod1)
	app.taskMap[taskID1] = task1
	task1.sm.SetState(TaskStates().Pending)
	taskID2 := "task02"
	task2 := NewTask(taskID2, app, context, pod2)
	app.taskMap[taskID2] = task2
	task2.sm.SetState(TaskStates().Pending)
	// check the tasks both in non-terminated states
	// res should return both task's alias
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 2)
	assert.Assert(t, is.Contains(res, "/test-00001"))
	assert.Assert(t, is.Contains(res, "/test-00002"))

	// set two tasks to terminated states
	task1.sm.SetState(TaskStates().Rejected)
	task2.sm.SetState(TaskStates().Rejected)
	// check the tasks both in terminated states
	// res should retuen empty
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 0)

	// set two tasks to one is terminated, another is non-terminated
	task1.sm.SetState(TaskStates().Rejected)
	task2.sm.SetState(TaskStates().Allocated)
	// check the task, should only return task2's alias
	res = app.getNonTerminatedTaskAlias()
	assert.Equal(t, len(res), 1)
	assert.Equal(t, res[0], "/test-00002")
}

func TestSetTaskGroupsAndSchedulingPolicy(t *testing.T) {
	app := NewApplication("app01", "root.a", "test-user", testGroups, map[string]string{}, newMockSchedulerAPI())
	assert.Equal(t, len(app.getTaskGroups()), 0)

	duration := int64(3000)
	app.setTaskGroups([]interfaces.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
		{
			Name:      "test-group-2",
			MinMember: 20,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("1000m"),
				v1.ResourceMemory.String(): resource.MustParse("1000Mi"),
			},
			NodeSelector: map[string]string{
				"locate": "west",
			},
			Tolerations: []v1.Toleration{
				{
					Key:               "nodeType",
					Operator:          v1.TolerationOpEqual,
					Value:             "infra",
					Effect:            v1.TaintEffectNoSchedule,
					TolerationSeconds: &duration,
				},
			},
		},
	})

	assert.Assert(t, app.getTaskGroups() != nil)
	assert.Equal(t, len(app.getTaskGroups()), 2)

	// sort the slice to give us a stable order
	sort.Slice(app.getTaskGroups(), func(i, j int) bool {
		return strings.Compare(app.getTaskGroups()[i].Name, app.getTaskGroups()[j].Name) < 0
	})

	tg1 := app.getTaskGroups()[0]
	assert.Equal(t, tg1.Name, "test-group-1")
	assert.Equal(t, tg1.MinMember, int32(10))
	assert.Equal(t, tg1.MinResource[v1.ResourceCPU.String()], resource.MustParse("500m"))
	assert.Equal(t, tg1.MinResource[v1.ResourceMemory.String()], resource.MustParse("500Mi"))

	tg2 := app.getTaskGroups()[1]
	assert.Equal(t, tg2.Name, "test-group-2")
	assert.Equal(t, tg2.MinMember, int32(20))
	assert.Equal(t, len(tg2.Tolerations), 1)
	assert.Equal(t, tg2.Tolerations[0].Key, "nodeType")
	assert.Equal(t, tg2.Tolerations[0].Value, "infra")
	assert.Equal(t, tg2.Tolerations[0].Operator, v1.TolerationOpEqual)
	assert.Equal(t, tg2.Tolerations[0].Effect, v1.TaintEffectNoSchedule)
	assert.Equal(t, tg2.Tolerations[0].TolerationSeconds, &duration)

	// TG1: 500Mi, 10 members -> total 5000Mi
	// TG2: 1000Mi, 20 members -> total 20000Mi
	// overall usage 5000Mi + 20000Mi = 25000Mi. This will also be the queue usage so the correct handling
	// CPU is normal as it specifies milli cpu to start with
	// Pods should always be equal to the sum of the MinMember values
	expectedPlaceholderAsk := common.NewResourceBuilder().AddResource("pods", 30).AddResource(siCommon.Memory, 25000*1024*1024).AddResource(siCommon.CPU, 25000).Build()
	actualPlaceholderAsk := app.getPlaceholderAsk()
	assert.DeepEqual(t, actualPlaceholderAsk, expectedPlaceholderAsk, cmpopts.IgnoreUnexported(si.Resource{}, si.Quantity{}))
}

type threadSafePodsMap struct {
	pods map[string]*v1.Pod
	sync.RWMutex
}

func newThreadSafePodsMap() *threadSafePodsMap {
	return &threadSafePodsMap{
		pods: make(map[string]*v1.Pod),
	}
}

func (t *threadSafePodsMap) add(pod *v1.Pod) {
	t.Lock()
	defer t.Unlock()
	t.pods[pod.Name] = pod
}

func (t *threadSafePodsMap) count() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.pods)
}

func TestTryReserve(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// inject the mocked clients to the placeholder manager
	createdPods := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods.add(pod)
		return pod, nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()
	defer mgr.Stop()

	// create a new app
	app := NewApplication("app00001", "root.abc", "test-user",
		testGroups, map[string]string{}, mockedAPIProvider.GetAPIs().SchedulerAPI)
	context.addApplication(app)

	// set taskGroups
	app.setTaskGroups([]interfaces.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
		{
			Name:      "test-group-2",
			MinMember: 20,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("1000m"),
				v1.ResourceMemory.String(): resource.MustParse("1000Mi"),
			},
		},
	})

	// submit the app
	assert.Assert(t, app.sm != nil)
	err := app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Submitted, 3*time.Second)

	// accepted the app
	err = app.handle(NewSimpleApplicationEvent(app.GetApplicationID(), AcceptApplication))
	assert.NilError(t, err)

	// run app schedule
	app.Schedule()

	// since this app has taskGroups defined,
	// once the app is accepted, it is expected to see this app goes to Reserving state
	assertAppState(t, app, ApplicationStates().Reserving, 3*time.Second)

	// under Reserving state, the app will need to acquire all the placeholders it asks for
	err = utils.WaitForCondition(func() bool {
		return createdPods.count() == 30
	}, 100*time.Millisecond, 3*time.Second)
	assert.NilError(t, err, "placeholders are not created")
}

func TestTryReservePostRestart(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// inject the mocked clients to the placeholder manager
	createdPods := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods.add(pod)
		return pod, nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()
	defer mgr.Stop()

	// create a new app
	app := NewApplication("app00001", "root.abc", "test-user",
		testGroups, map[string]string{}, mockedAPIProvider.GetAPIs().SchedulerAPI)
	context.addApplication(app)

	// set taskGroups
	app.setTaskGroups([]interfaces.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
		{
			Name:      "test-group-2",
			MinMember: 20,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("1000m"),
				v1.ResourceMemory.String(): resource.MustParse("1000Mi"),
			},
		},
	})

	// submit the app
	err := app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Submitted, 3*time.Second)

	// accepted the app
	err = app.handle(NewSimpleApplicationEvent(app.GetApplicationID(), AcceptApplication))
	assert.NilError(t, err)

	// simulate some tasks are recovered during the restart
	// create 3 pods, 1 of them is Allocated and the other 2 are New
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	task0 := NewTask("task00", app, context, &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00000",
			UID:  "UID-00000",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})
	task0.allocationUUID = string(task0.pod.UID)
	task0.nodeName = "fake-host"
	task0.sm.SetState(TaskStates().Allocated)

	task1 := NewTask("task01", app, context, &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})

	task2 := NewTask("task02", app, context, &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00002",
			UID:  "UID-00002",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	})

	app.addTask(task0)
	app.addTask(task1)
	app.addTask(task2)

	// there should be 1 Allocated task, i.e task0
	// there should be 2 New tasks, i.e task1 and task2
	assert.Equal(t, len(app.getTasks(TaskStates().Allocated)), 1)
	assert.Equal(t, len(app.getTasks(TaskStates().New)), 2)

	// run app schedule
	app.Schedule()

	// since this app has Allocated tasks, the Reserving state will be skipped
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)

	// verify there will be no placeholders created
	time.Sleep(time.Second)
	assert.Equal(t, createdPods.count(), 0)
}

func TestTriggerAppRecovery(t *testing.T) {
	// Trigger app recovery should be successful if the app is in New state
	mockScheduler := newMockSchedulerAPI()
	var savedAppRequest *si.ApplicationRequest
	mockScheduler.UpdateApplicationFn = func(request *si.ApplicationRequest) error {
		savedAppRequest = request
		return nil
	}

	app := NewApplication("app00001", "root.abc", "test-user",
		testGroups, map[string]string{}, mockScheduler)
	app.placeholderAsk = &si.Resource{
		Resources: map[string]*si.Quantity{
			"memory": {Value: 100},
		},
	}
	app.placeholderTimeoutInSec = 1
	app.tags = map[string]string{
		"testkey": "testvalue",
	}
	app.schedulingStyle = "soft"

	err := app.TriggerAppRecovery()
	assert.NilError(t, err)
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().Recovering)
	assert.Assert(t, savedAppRequest != nil, "update function was not called")
	assert.Equal(t, 1, len(savedAppRequest.New))
	appRequest := savedAppRequest.New[0]
	assert.Assert(t, appRequest.PlaceholderAsk != nil, "PlaceholderAsk is not set")
	assert.Equal(t, appRequest.PlaceholderAsk.Resources["memory"].Value, int64(100))
	assert.Equal(t, "app00001", appRequest.ApplicationID)
	assert.Equal(t, appRequest.QueueName, "root.abc")
	assert.Equal(t, appRequest.PartitionName, "default")
	assert.Equal(t, appRequest.ExecutionTimeoutMilliSeconds, int64(1000))
	assert.Equal(t, appRequest.GangSchedulingStyle, "soft")
	assert.Assert(t, appRequest.Tags != nil, "Tags are not set")
	assert.Equal(t, appRequest.Tags["testkey"], "testvalue")
	assert.Assert(t, appRequest.Ugi != nil, "Ugi is not set")
	assert.Equal(t, appRequest.Ugi.User, "test-user")

	// Trigger app recovery should be failed if the app already leaves New state
	app = NewApplication("app00001", "root.abc", "test-user",
		testGroups, map[string]string{}, newMockSchedulerAPI())
	err = app.handle(NewSubmitApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Submitted, 3*time.Second)
	err = app.TriggerAppRecovery()
	assert.ErrorContains(t, err, "event RecoverApplication inappropriate in current state Submitted")
}

func TestSkipReservationStage(t *testing.T) {
	context := initContextForTest()
	app := NewApplication("app00001", "root.queue", "test-user", testGroups, map[string]string{}, newMockSchedulerAPI())
	app.addTask(NewTask("task0001", app, context, &v1.Pod{}))
	skip := app.skipReservationStage()
	assert.Equal(t, skip, true, "expected to skip reservation because there is no task groups defined")

	// app has task groups defined, and contains 2 tasks, 1 Pending and 1 Allocated
	// expect: skip reservation
	app = NewApplication("app00001", "root.queue", "test-user", testGroups, map[string]string{}, newMockSchedulerAPI())
	task1 := NewTask("task0001", app, context, &v1.Pod{})
	task1.sm.SetState(TaskStates().New)
	task2 := NewTask("task0002", app, context, &v1.Pod{})
	task2.sm.SetState(TaskStates().Allocated)
	app.addTask(task1)
	app.addTask(task2)
	app.setTaskGroups([]interfaces.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		}},
	)
	skip = app.skipReservationStage()
	assert.Equal(t, skip, true, "expected to skip reservation because there is task in Allocated state")

	// app has task groups defined, and contains 2 tasks, both are New
	// expect: do not skip reservation
	app = NewApplication("app00001", "root.queue", "test-user", testGroups, map[string]string{}, newMockSchedulerAPI())
	task1 = NewTask("task0001", app, context, &v1.Pod{})
	task1.sm.SetState(TaskStates().New)
	task2 = NewTask("task0002", app, context, &v1.Pod{})
	task2.sm.SetState(TaskStates().New)
	app.addTask(task1)
	app.addTask(task2)
	app.setTaskGroups([]interfaces.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		}},
	)
	skip = app.skipReservationStage()
	assert.Equal(t, skip, false, "expected not to skip reservation")
}

func TestReleaseAppAllocationInFailingState(t *testing.T) {
	context := initContextForTest()
	ms := &mockSchedulerAPI{}
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	appID := "app-test-001"
	UUID := "testUUID001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, ms)
	task := NewTask("task01", app, context, pod)
	app.addTask(task)
	task.allocationUUID = UUID
	// app must be running states
	err := app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	if err == nil {
		// this should give an error
		t.Error("expecting error got 'nil'")
	}
	// set app states to running, let event can be trigger
	app.SetState(ApplicationStates().Running)
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)
	err = app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	assert.NilError(t, err)
	// after handle release event the states of app must be running
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)
	app.SetState(ApplicationStates().Failing)
	err = app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	assert.NilError(t, err)
	// after handle release event the states of app must be failing
	assertAppState(t, app, ApplicationStates().Failing, 3*time.Second)

	errMess := "Test Error Message"
	err = app.handle(NewFailApplicationEvent(app.applicationID, errMess))
	assert.NilError(t, err)
	// after handle fail event the states of app must be failed
	assertAppState(t, app, ApplicationStates().Failed, 3*time.Second)
}

func TestResumingStateTransitions(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	// inject the mocked clients to the placeholder manager
	createdPods := newThreadSafePodsMap()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.MockCreateFn(func(pod *v1.Pod) (*v1.Pod, error) {
		createdPods.add(pod)
		return pod, nil
	})
	mgr := NewPlaceholderManager(mockedAPIProvider.GetAPIs())
	mgr.Start()
	defer mgr.Stop()

	// create a new app
	app := NewApplication("app00001", "root.abc", "test-user",
		testGroups, map[string]string{}, mockedAPIProvider.GetAPIs().SchedulerAPI)
	task1 := NewTask("task0001", app, context, &v1.Pod{})
	task1.sm.SetState(TaskStates().New)
	task2 := NewTask("task0002", app, context, &v1.Pod{})
	task2.sm.SetState(TaskStates().Allocated)

	// Add tasks
	app.addTask(task1)
	app.addTask(task2)
	UUID := "testUUID001"
	task1.allocationUUID = UUID
	context.addApplication(app)

	// Set app state to "reserving"
	app.SetState(ApplicationStates().Reserving)

	// Fire ResumingApplicationEvent for state change from "reserving" to "resuming"
	err := app.handle(NewResumingApplicationEvent(app.applicationID))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Resuming, 3*time.Second)

	// Set 1st task status alone to "completed"
	event1 := NewSimpleTaskEvent(app.applicationID, task1.taskID, CompleteTask)
	err = task1.handle(event1)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)

	// Still app state is "resuming"
	assertAppState(t, app, ApplicationStates().Resuming, 3*time.Second)

	// Setting 2nd task status also to "completed". Now, app state changes from "resuming" to "running"
	event2 := NewSimpleTaskEvent(app.applicationID, task2.taskID, CompleteTask)
	err = task2.handle(event2)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task2.GetTaskState(), TaskStates().Completed)

	err = app.handle(NewSimpleApplicationEvent(app.applicationID, AppTaskCompleted))
	assert.NilError(t, err)
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)
}

func TestGetPlaceholderTasks(t *testing.T) {
	context := initContextForTest()
	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	task1 := NewTask("task0001", app, context, &v1.Pod{})
	task1.placeholder = true
	task2 := NewTask("task0002", app, context, &v1.Pod{})
	task2.placeholder = true
	task3 := NewTask("task0003", app, context, &v1.Pod{})

	app.addTask(task1)
	app.addTask(task2)
	app.addTask(task3)

	phTasks := app.GetPlaceHolderTasks()
	assert.Equal(t, 2, len(phTasks))
	phTasksMap := map[string]bool{
		phTasks[0].GetTaskID(): true,
		phTasks[1].GetTaskID(): true,
	}

	assert.Assert(t, phTasksMap["task0001"])
	assert.Assert(t, phTasksMap["task0002"])
}

func TestPlaceholderTimeoutEvents(t *testing.T) {
	context := initContextForTest()
	recorder, ok := events.GetRecorder().(*k8sEvents.FakeRecorder)
	if !ok {
		t.Fatal("the EventRecorder is expected to be of type FakeRecorder")
	}

	amprotocol := NewMockedAMProtocol()
	podEvent := general.NewPodEventHandler(amprotocol, false)

	am := general.NewManager(client.NewMockedAPIProvider(false), podEvent)
	pod1 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"queue":         "root.a",
				"applicationId": "app00001",
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.AddPod(&pod1)

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00002",
			Namespace: "default",
			UID:       "UID-POD-00002",
			Labels: map[string]string{
				"queue":         "root.a",
				"applicationId": "app00001",
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	managedApp := amprotocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := managedApp.(*Application)
	if !valid {
		t.Fatal("application is expected to be of type Application")
	}
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	appID := "app00001"
	UUID := "UID-POD-00002"

	context.addApplication(app)
	task1 := context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task02",
			Pod:           pod,
			Placeholder:   true,
		},
	})
	assert.Assert(t, task1 != nil)
	assert.Equal(t, task1.GetTaskID(), "task02")

	_, taskErr := app.GetTask("task02")
	assert.NilError(t, taskErr, "Task should exist")

	task2, task2Err := task1.(*Task)
	if !task2Err {
		// this should give an error
		t.Error("task1 is expected to be of type Task")
	}
	task2.allocationUUID = UUID

	// app must be running states
	err := app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	assert.Error(t, err, "event ReleaseAppAllocation inappropriate in current state New")

	// set app states to running, let event can be trigger
	app.SetState(ApplicationStates().Running)
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)
	err = app.handle(NewReleaseAppAllocationEvent(appID, si.TerminationType_TIMEOUT, UUID))
	assert.NilError(t, err)
	// after handle release event the states of app must be running
	assertAppState(t, app, ApplicationStates().Running, 3*time.Second)

	message := "Placeholder timed out"
	reason := "placeholder has been timed out"
	// check that the event has been published
	err = utils.WaitForCondition(func() bool {
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

func TestApplication_onReservationStateChange(t *testing.T) {
	context := initContextForTest()
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, context.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	app := NewApplication(appID, "root.a", "testuser", testGroups, map[string]string{}, newMockSchedulerAPI())
	context.addApplication(app)

	app.sm.SetState("Accepted")
	app.onReservationStateChange()
	// app goes to running (no taskgroups defined)
	assertAppState(t, app, ApplicationStates().Running, 1*time.Second)

	// set taskGroups
	app.setTaskGroups([]interfaces.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 1,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
		{
			Name:      "test-group-2",
			MinMember: 2,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
	})
	app.sm.SetState("Accepted")
	app.onReservationStateChange()
	// app stays in accepted with taskgroup defined
	assertAppState(t, app, ApplicationStates().Accepted, 1*time.Second)

	task1 := NewTask("task0001", app, context, &v1.Pod{})
	task1.setTaskGroupName("test-group-1")
	task1.placeholder = true
	task2 := NewTask("task0002", app, context, &v1.Pod{})
	task2.setTaskGroupName("test-group-2")
	task2.placeholder = true
	// not placeholder, unknown group
	task3 := NewTask("task0003", app, context, &v1.Pod{})
	task3.setTaskGroupName("unknown")

	app.addTask(task1)
	app.addTask(task2)
	app.addTask(task3)

	// app stays in accepted with taskgroups defined none bound
	app.onReservationStateChange()
	assertAppState(t, app, ApplicationStates().Accepted, 1*time.Second)

	// app stays in accepted with taskgroups defined one unknown taskgroup not a placeholder
	// all tasks bound
	task1.sm.SetState("Bound")
	task2.sm.SetState("Bound")
	task3.sm.SetState("Bound")
	app.onReservationStateChange()
	assertAppState(t, app, ApplicationStates().Accepted, 1*time.Second)

	// app stays in accepted with taskgroups defined one unknown taskgroup
	task3.placeholder = true
	app.onReservationStateChange()
	assertAppState(t, app, ApplicationStates().Accepted, 1*time.Second)

	// app moves to running
	task3.setTaskGroupName("test-group-2")
	app.onReservationStateChange()
	assertAppState(t, app, ApplicationStates().Running, 1*time.Second)
}

func (ctx *Context) addApplication(app *Application) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.applications[app.applicationID] = app
}
