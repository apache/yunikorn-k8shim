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
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

func TestAMSvcAddPod(t *testing.T) {
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, client.NewMockedAPIProvider(false))
	am.podEventHandler.recoveryRunning = false

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.AddPod(&pod)

	managedApp := amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := anyToApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	am.AddPod(&pod1)
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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	am.AddPod(&pod2)
	app02 := amProtocol.GetApplication("app00002")
	assert.Assert(t, app02 != nil)
	app, valid = anyToApplication(app02)
	assert.Equal(t, valid, true)
	assert.Equal(t, len(app.GetNewTasks()), 1)
	assert.Equal(t, app.GetApplicationID(), "app00002")
	assert.Equal(t, app.GetNewTasks()[0].GetTaskPod().Name, "pod00004")
}

func TestAMSvcOriginatorPod(t *testing.T) {
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, client.NewMockedAPIProvider(false))
	am.podEventHandler.recoveryRunning = false

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add pod 2 as owner for pod 1
	owner := apis.OwnerReference{
		APIVersion: "v1",
		UID:        "UID-POD-00002",
		Kind:       "Pod",
	}

	refer := []apis.OwnerReference{
		owner,
	}
	pod.SetOwnerReferences(refer)

	// add a pending pod through the AM service
	am.AddPod(&pod)

	managedApp := amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := anyToApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// add another pod, pod 2 (owner) for same application
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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	am.AddPod(&pod1)
	assert.Equal(t, len(app.GetNewTasks()), 2)
	task, err = app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)

	// app originator task should be pod 1
	// even the pod 2 is the ownerreference for pod 1
	// And pod 1 is first added to the AM service
	assert.Equal(t, app.GetOriginatingTask().GetTaskID(), task.GetTaskID())
}

func TestAMSvcUpdatePodWhenSucceed(t *testing.T) {
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, client.NewMockedAPIProvider(false))
	am.podEventHandler.recoveryRunning = false

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.AddPod(&pod)

	managedApp := amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := anyToApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	am.updatePod(&pod, &newPod)

	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
}

func TestAMSvcUpdatePodWhenFailed(t *testing.T) {
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, client.NewMockedAPIProvider(false))
	am.podEventHandler.recoveryRunning = false

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.AddPod(&pod)

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	am.updatePod(&pod, &newPod)

	managedApp := amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := anyToApplication(managedApp)
	assert.Equal(t, valid, true)
	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
}

func TestAMSvcDeletePod(t *testing.T) {
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, client.NewMockedAPIProvider(false))
	am.podEventHandler.recoveryRunning = false

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// add a pending pod through the AM service
	am.AddPod(&pod)

	managedApp := amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := anyToApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), ApplicationStates().New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// try delete the pod
	am.deletePod(&pod)

	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
}

func anyToApplication(something interface{}) (*Application, bool) {
	if app, valid := something.(*Application); valid {
		return app, true
	}
	return nil, false
}

func TestAMSvcGetExistingAllocation(t *testing.T) {
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, client.NewMockedAPIProvider(false))
	am.podEventHandler.recoveryRunning = true

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
			SchedulerName: constants.SchedulerName,
			NodeName:      "allocated-node",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// verifies the existing allocation is correctly returned
	alloc := am.GetExistingAllocation(pod)
	assert.Equal(t, alloc.ApplicationID, "app00001")
	assert.Equal(t, alloc.AllocationKey, string(pod.UID))
	assert.Equal(t, alloc.UUID, string(pod.UID))
	assert.Equal(t, alloc.NodeID, "allocated-node")
}

type Template struct {
	podName    string
	namespace  string
	label      map[string]string
	annotation map[string]string
	scheduler  bool
	node       bool
}

// nolint: funlen
func TestAMSvcListApplication(t *testing.T) {
	// mock the pod lister for this test
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedPodLister := test.NewPodListerMock()
	mockedAPIProvider.SetPodLister(mockedPodLister)
	appName := []string{"app00001", "app00002", "app00003", "app00004", "app00005"}
	var queue01 = "root.queue01"
	var ns01 = "namespace01"
	var ns02 = "namespace02"
	type testcase struct {
		description    string
		applicationID  string
		input          *v1.Pod
		expectedOutput bool
	}
	podCase := []Template{
		// Application 1
		{
			podName:   "app01pod01",
			namespace: ns01,
			label: map[string]string{
				constants.LabelApplicationID: appName[0],
				constants.LabelQueueName:     queue01,
			},
			annotation: nil,
			scheduler:  true,
			node:       true,
		},
		{
			podName:   "app01pod02",
			namespace: ns01,
			label: map[string]string{
				constants.LabelApplicationID: appName[0],
				constants.LabelQueueName:     queue01,
			},
			annotation: nil,
			scheduler:  true,
			node:       false,
		},
		// Application 2
		{
			podName:   "app02pod01",
			namespace: ns02,
			label: map[string]string{
				constants.SparkLabelAppID: appName[1],
			},
			annotation: nil,
			scheduler:  true,
			node:       true,
		},
		// Application 3
		{
			podName:   "app03pod01",
			namespace: ns01,
			label:     nil,
			annotation: map[string]string{
				constants.AnnotationApplicationID: appName[2],
			},
			scheduler: true,
			node:      true,
		},
		// Application 4
		{
			podName:    "app04pod01",
			namespace:  ns01,
			label:      nil,
			annotation: nil,
			scheduler:  true,
			node:       true,
		},
		// Application 5
		{
			podName:   "app05pod01",
			namespace: ns01,
			label: map[string]string{
				constants.SparkLabelAppID: appName[4],
			},
			annotation: nil,
			scheduler:  false,
			node:       true,
		},
	}
	listAppTestCase := []testcase{
		// Application 1
		{
			description:    "running in queue01 and namespace01, with labels, schedulerName, nodeName",
			applicationID:  appName[0],
			input:          podCase[0].InjectPod(),
			expectedOutput: true,
		},
		{
			description:    "running in queue01 and namespace01, with labels, schedulerName",
			applicationID:  appName[0],
			input:          podCase[1].InjectPod(),
			expectedOutput: true,
		},
		// Application 2
		{
			description:    "running in default queue and namespace02, with spark labels, schedulerName, and nodeName",
			applicationID:  appName[1],
			input:          podCase[2].InjectPod(),
			expectedOutput: true,
		},
		// Application 3
		{
			description:    "running in default queue and namespace01, with annotation, schedulerName, and nodeName",
			applicationID:  appName[2],
			input:          podCase[3].InjectPod(),
			expectedOutput: true,
		},
		// Application 4
		{
			description:    "running in default queue and namespace01, without label and annotation",
			applicationID:  "yunikorn-namespace01-autogen",
			input:          podCase[4].InjectPod(),
			expectedOutput: true,
		},
		// Application 5
		{
			description:    "running in default queue and namespace01, with label and nodeName",
			applicationID:  appName[4],
			input:          podCase[5].InjectPod(),
			expectedOutput: false,
		},
	}
	expectOutput := make(map[string]bool)
	descriptionMap := make(map[string]string)
	for index := range listAppTestCase {
		mockedPodLister.AddPod(listAppTestCase[index].input)
		expectOutput[listAppTestCase[index].applicationID] = listAppTestCase[index].expectedOutput
		descriptionMap[listAppTestCase[index].applicationID] = listAppTestCase[index].description
	}
	// init the app manager and run listApp
	amProtocol := NewMockedAMProtocol()
	am := NewAMService(amProtocol, mockedAPIProvider)
	am.podEventHandler.recoveryRunning = true

	pods, err := am.ListPods()
	assert.NilError(t, err)
	assert.Equal(t, len(pods), 4)
	for _, pod := range pods {
		name := utils.GetApplicationIDFromPod(pod)
		expected := expectOutput[name]
		description := descriptionMap[name]
		assert.Assert(t, expected, description)
	}
}

func (temp Template) InjectPod() *v1.Pod {
	tempPod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      temp.podName,
			Namespace: temp.namespace,
		},
	}
	if temp.label != nil {
		tempPod.Labels = temp.label
	}
	if temp.annotation != nil {
		tempPod.Annotations = temp.annotation
	}
	if temp.scheduler {
		tempPod.Spec.SchedulerName = constants.SchedulerName
	}
	if temp.node {
		tempPod.Spec.NodeName = "some-node"
	}
	return tempPod
}
