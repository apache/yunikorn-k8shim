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

package general

import (
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
)

const taskGroupInfo = `
[
	{
		"name": "test-group-1",
		"minMember": 3,
		"minResource": {
			"cpu": 2,
			"memory": "1Gi"
		}
	}
]`

func TestGetAppMetadata(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))

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
			Annotations: map[string]string{
				constants.AnnotationTaskGroups:            taskGroupInfo,
				constants.AnnotationSchedulingPolicyParam: "gangSchedulingStyle=Soft",
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok := am.getAppMetadata(&pod, false)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00001")
	assert.Equal(t, app.QueueName, "root.a")
	assert.Equal(t, app.User, constants.DefaultUser)
	assert.DeepEqual(t, app.Tags, map[string]string{"namespace": "default"})
	assert.Equal(t, app.TaskGroups[0].Name, "test-group-1")
	assert.Equal(t, app.TaskGroups[0].MinMember, int32(3))
	assert.Equal(t, app.TaskGroups[0].MinResource["cpu"], resource.MustParse("2"))
	assert.Equal(t, app.TaskGroups[0].MinResource["memory"], resource.MustParse("1Gi"))
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Soft")

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
				"applicationId":            "app00002",
				"queue":                    "root.b",
				"yunikorn.apache.org/user": "testuser",
				"disableStateAware":        "true",
			},
			Annotations: map[string]string{
				constants.AnnotationSchedulingPolicyParam: "gangSchedulingStyle=Hard",
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod, false)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00002")
	assert.Equal(t, app.QueueName, "root.b")
	assert.Equal(t, app.User, constants.DefaultUser)
	assert.DeepEqual(t, app.Tags, map[string]string{
		"application.stateaware.disable": "true",
		"namespace":                      "app-namespace-01",
	})
	assert.DeepEqual(t, len(app.TaskGroups), 0)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Hard")

	pod = v1.Pod{
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
			Annotations: map[string]string{
				constants.AnnotationTaskGroups: taskGroupInfo,
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod, false)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Soft")

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
				"applicationId":            "app00002",
				"queue":                    "root.b",
				"yunikorn.apache.org/user": "testuser",
			},
			Annotations: map[string]string{
				constants.AnnotationSchedulingPolicyParam: "gangSchedulingStyle=Hard=Soft",
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod, false)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Soft")

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
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod, false)
	assert.Equal(t, ok, false)
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
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod, false)
	assert.Equal(t, ok, false)
}

func TestGetTaskMetadata(t *testing.T) {
	am := NewManager(&cache.MockedAMProtocol{}, client.NewMockedAPIProvider(false))

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
			Annotations: map[string]string{
				constants.AnnotationTaskGroupName: "test-group-01",
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	task, ok := am.getTaskMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, task.ApplicationID, "app00001")
	assert.Equal(t, task.TaskID, "UID-POD-00001")
	assert.Equal(t, task.TaskGroupName, "test-group-01")
	pod.Annotations = map[string]string{}
	task, ok = am.getTaskMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, task.TaskGroupName, "")

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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	task, ok = am.getTaskMetadata(&pod)
	assert.Equal(t, ok, false)
}

func TestAddPod(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))

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
	am.addPod(&pod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
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
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	am.addPod(&pod2)
	app02 := am.amProtocol.GetApplication("app00002")
	assert.Assert(t, app02 != nil)
	app, valid = toApplication(app02)
	assert.Equal(t, valid, true)
	assert.Equal(t, len(app.GetNewTasks()), 1)
	assert.Equal(t, app.GetApplicationID(), "app00002")
	assert.Equal(t, app.GetNewTasks()[0].GetTaskPod().Name, "pod00004")
}

func TestUpdatePodWhenSucceed(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))

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
	am.addPod(&pod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

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
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
}

func TestUpdatePodWhenFailed(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))

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
	am.addPod(&pod)

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

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
}

func TestDeletePod(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))

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
	am.addPod(&pod)

	managedApp := am.amProtocol.GetApplication("app00001")
	assert.Assert(t, managedApp != nil)
	app, valid := toApplication(managedApp)
	assert.Equal(t, valid, true)
	assert.Equal(t, app.GetApplicationID(), "app00001")
	assert.Equal(t, app.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, app.GetQueue(), "root.a")
	assert.Equal(t, len(app.GetNewTasks()), 1)

	task, err := app.GetTask("UID-POD-00001")
	assert.Assert(t, err == nil)
	assert.Equal(t, task.GetTaskState(), events.States().Task.New)

	// try delete the pod
	am.deletePod(&pod)

	// this is to verify NotifyTaskComplete is called
	assert.Equal(t, task.GetTaskState(), events.States().Task.Completed)
}

func toApplication(something interface{}) (*cache.Application, bool) {
	if app, valid := something.(*cache.Application); valid {
		return app, true
	}
	return nil, false
}

func TestGetExistingAllocation(t *testing.T) {
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))

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
	assert.Equal(t, alloc.QueueName, "root.a")
	assert.Equal(t, alloc.AllocationKey, string(pod.UID))
	assert.Equal(t, alloc.UUID, string(pod.UID))
	assert.Equal(t, alloc.NodeID, "allocated-node")
}

func TestGetOwnerReferences(t *testing.T) {
	ownerRef := apis.OwnerReference{
		APIVersion: apis.SchemeGroupVersion.String(),
		Name:       "owner ref",
	}
	podWithOwnerRef := &v1.Pod{
		ObjectMeta: apis.ObjectMeta{
			OwnerReferences: []apis.OwnerReference{ownerRef},
		},
	}
	podWithNoOwnerRef := &v1.Pod{
		ObjectMeta: apis.ObjectMeta{
			Name: "pod",
			UID:  "uid",
		},
	}
	returnedOwnerRefs := getOwnerReferences(podWithOwnerRef)
	assert.Assert(t, len(returnedOwnerRefs) == 1, "Only one owner reference is expected")
	assert.DeepEqual(t, ownerRef, returnedOwnerRefs[0])

	returnedOwnerRefs = getOwnerReferences(podWithNoOwnerRef)
	assert.Assert(t, len(returnedOwnerRefs) == 1, "Only one owner reference is expected")
	assert.Equal(t, returnedOwnerRefs[0].Name, podWithNoOwnerRef.Name, "Unexpected owner reference name")
	assert.Equal(t, returnedOwnerRefs[0].UID, podWithNoOwnerRef.UID, "Unexpected owner reference UID")
	assert.Equal(t, returnedOwnerRefs[0].Kind, "Pod", "Unexpected owner reference Kind")
	assert.Equal(t, returnedOwnerRefs[0].APIVersion, v1.SchemeGroupVersion.String(), "Unexpected owner reference Kind")
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
func TestListApplication(t *testing.T) {
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
			applicationID:  appName[3],
			input:          podCase[4].InjectPod(),
			expectedOutput: false,
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
	am := NewManager(cache.NewMockedAMProtocol(), mockedAPIProvider)
	apps, err := am.ListApplications()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 3)
	for name := range apps {
		_, exist := apps[name]
		assert.Equal(t, exist, expectOutput[name], descriptionMap[name])
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
