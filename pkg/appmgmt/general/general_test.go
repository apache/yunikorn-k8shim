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

	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
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
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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

	app, ok := am.getAppMetadata(&pod)
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

	app, ok = am.getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00002")
	assert.Equal(t, app.QueueName, "root.b")
	assert.Equal(t, app.User, constants.DefaultUser)
	assert.DeepEqual(t, app.Tags, map[string]string{"namespace": "app-namespace-01"})
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

	app, ok = am.getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Hard")

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
				constants.AnnotationSchedulingPolicyParam: "gangSchedulingStyle=Soft=Hard",
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok = am.getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Hard")

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

	app, ok = am.getAppMetadata(&pod)
	assert.Equal(t, ok, false)
}

func TestGetTaskMetadata(t *testing.T) {
	am := NewManager(&cache.MockedAMProtocol{}, client.NewMockedAPIProvider())

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
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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
	am := NewManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())

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

// nolint: funlen
func TestListApplication(t *testing.T) {
	var app01, app02, app03, app04, app05, app06 = "app00001",
		"app00002", "app00003", "app00004", "app00005", "app00006"
	var queue01, queue02 = "root.queue01", "root.queue02"
	var ns01, ns02 = "namespace01", "namespace02"

	// mock the pod lister for this test
	mockedAPIProvider := client.NewMockedAPIProvider()
	mockedPodLister := test.NewPodListerMock()
	mockedAPIProvider.SetPodLister(mockedPodLister)

	// app01 pods, running in namespace01 and queue01
	// all pods are having applicationID and queue name specified
	// 2 pods have assigned nodes, 1 pod is pending for scheduling
	// allocated pod
	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app01pod00001",
			Namespace: ns01,
			Labels: map[string]string{
				constants.LabelApplicationID: app01,
				constants.LabelQueueName:     queue01,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "allocated-node",
		},
	})

	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app01pod00002",
			Namespace: ns01,
			Labels: map[string]string{
				constants.LabelApplicationID: app01,
				constants.LabelQueueName:     queue01,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "allocated-node",
		},
	})

	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app01pod00003",
			Namespace: ns01,
			Labels: map[string]string{
				constants.LabelApplicationID: app01,
				constants.LabelQueueName:     queue01,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
	})

	// app02 pods, running in queue02 and namespace02
	// 2 pods are having applicationID and queue name specified
	// both 2 pods are pending
	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app02pod0001",
			Namespace: ns02,
			Labels: map[string]string{
				constants.LabelApplicationID: app02,
				constants.LabelQueueName:     queue02,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
	})

	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app02pod0002",
			Namespace: ns02,
			Labels: map[string]string{
				constants.LabelApplicationID: app02,
				constants.LabelQueueName:     queue02,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
	})

	// app03 pods, running in queue02 and namespace02
	// 2 pods do not have label applicationID specified, but have spark-app-selector
	// both 2 pods are allocated
	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app03pod0001",
			Namespace: ns01,
			Labels: map[string]string{
				constants.SparkLabelAppID: app03,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "some-node",
		},
	})

	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app03pod0002",
			Namespace: ns02,
			Labels: map[string]string{
				constants.SparkLabelAppID: app03,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "some-node",
		},
	})

	// app04 pods, running in queue01 and namespace01
	// app04 has 2 pods which only has annotation yunikorn.apache.org/app-id specified
	// both 2 pods are allocated
	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app04pod0001",
			Namespace: ns01,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: app04,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "some-node",
		},
	})

	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app04pod0002",
			Namespace: ns01,
			Annotations: map[string]string{
				constants.AnnotationApplicationID: app04,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "some-node",
		},
	})

	// app05 pods, running in queue01 and namespace01
	// app05 pod has no label or annotation specified
	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app05pod0001",
			Namespace: ns01,
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			NodeName:      "some-node",
		},
	})

	// app06 pods, running in queue01 and namespace01
	// pod has spark-app-selector set and it is allocated but not scheduled by yunikorn
	mockedPodLister.AddPod(&v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "app06pod0001",
			Namespace: ns01,
			Labels: map[string]string{
				constants.SparkLabelAppID: app06,
			},
		},
		Spec: v1.PodSpec{
			NodeName: "some-node",
		},
	})

	// init the app manager and run listApp
	am := NewManager(cache.NewMockedAMProtocol(), mockedAPIProvider)
	apps, err := am.ListApplications()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 3)
	_, exist := apps[app01]
	assert.Equal(t, exist, true,
		"app01 should be included in the list because "+
			"it has applicationID and queue namespace specified in the"+
			"queue and it has 2 pods allocated.")

	_, exist = apps[app02]
	assert.Equal(t, exist, false,
		"app02 should be excluded from the list because"+
			" it has no allocated pods found.")

	_, exist = apps[app03]
	assert.Equal(t, exist, true,
		"app03 should be included in the list because"+
			" it has 2 pods allocated and both pods have "+
			"spark-app-selector set.")

	_, exist = apps[app04]
	assert.Equal(t, exist, true,
		"app04 should be included in the list because"+
			" it has 2 pods allocated and both pods have "+
			"annotation yunikorn.apache.org/app-id specified set.")

	_, exist = apps[app05]
	assert.Equal(t, exist, false,
		"app05 should be excluded in the list because"+
			" pods have no appID set in annotation/label and "+
			"spark-app-selector doesn't exist either")

	_, exist = apps[app06]
	assert.Equal(t, exist, false,
		"app06 should be excluded in the list because"+
			" pods have spark-app-selector but the schedulerName "+
			"is not yunikorn, which is not scheduled by yunikorn.")
}
