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
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
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

func TestGetTaskMetadata(t *testing.T) {
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

	task, ok := getTaskMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, task.ApplicationID, "app00001")
	assert.Equal(t, task.TaskID, "UID-POD-00001")
	assert.Equal(t, task.TaskGroupName, "test-group-01")
	pod.Annotations = map[string]string{}
	task, ok = getTaskMetadata(&pod)
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

	task, ok = getTaskMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, task.ApplicationID, "yunikorn-default-autogen")
	assert.Equal(t, task.TaskID, "UID-POD-00001")
	assert.Equal(t, task.TaskGroupName, "")

	// case: empty pod
	task, ok = getTaskMetadata(&v1.Pod{})
	assert.Equal(t, ok, false)
	assert.DeepEqual(t, task, TaskMetadata{})
}

func TestGetAppMetadata(t *testing.T) { //nolint:funlen
	defer utils.SetPluginMode(false)
	defer func() { conf.GetSchedulerConf().GenerateUniqueAppIds = false }()
	utils.SetPluginMode(false)
	conf.GetSchedulerConf().GenerateUniqueAppIds = false

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
		Spec: v1.PodSpec{
			SchedulerName:    constants.SchedulerName,
			ImagePullSecrets: []v1.LocalObjectReference{{Name: "secret1"}, {Name: "secret2"}},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	app, ok := getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00001")
	assert.Equal(t, app.QueueName, "root.a")
	assert.Equal(t, app.User, constants.DefaultUser)
	assert.Equal(t, app.Tags["namespace"], "default")
	assert.Equal(t, app.Tags[constants.AnnotationSchedulingPolicyParam], "gangSchedulingStyle=Soft")
	assert.Equal(t, app.Tags[constants.AppTagImagePullSecrets], "secret1,secret2")
	assert.Equal(t, app.Tags[common.AppTagCreateForce], "false")
	assert.Assert(t, app.Tags[constants.AnnotationTaskGroups] != "")
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
				"applicationId":                   "app00002",
				"queue":                           "root.b",
				constants.DomainYuniKorn + "user": "testuser",
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

	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app00002")
	assert.Equal(t, app.QueueName, "root.b")
	assert.Equal(t, app.User, constants.DefaultUser)
	assert.Equal(t, app.Tags["namespace"], "app-namespace-01")
	assert.Equal(t, app.Tags[common.AppTagCreateForce], "false")
	assert.Equal(t, len(app.TaskGroups), 0)
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

	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Soft")
	assert.Equal(t, app.Tags[common.AppTagCreateForce], "false")

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
				"applicationId":                   "app00002",
				"queue":                           "root.b",
				constants.DomainYuniKorn + "user": "testuser",
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

	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Soft")
	assert.Equal(t, app.Tags[common.AppTagCreateForce], constants.False)

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
			NodeName:      Host1,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.Tags[common.AppTagCreateForce], constants.True)

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

	utils.SetPluginMode(false)
	app, ok = getAppMetadata(&pod)
	conf.GetSchedulerConf().GenerateUniqueAppIds = true
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "yunikorn-app-namespace-01-autogen")

	utils.SetPluginMode(false)
	conf.GetSchedulerConf().GenerateUniqueAppIds = true
	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.ApplicationID, "app-namespace-01-UID-POD-00001")

	utils.SetPluginMode(true)
	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, false)

	// case: invalid annotation task groups
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
				constants.AnnotationTaskGroups: "{ name:\"\" }",
			},
		},
		Spec:   v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{Phase: v1.PodPending},
	}
	app, ok = getAppMetadata(&pod)
	assert.Equal(t, ok, true)
	assert.Equal(t, app.SchedulingPolicyParameters.GetGangSchedulingStyle(), "Soft")
	assert.Equal(t, app.Tags[common.AppTagCreateForce], "false")
	assert.Equal(t, len(app.TaskGroups), 0)
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

	returnedOwnerRefs := getOwnerReference(podWithOwnerRef)
	assert.Assert(t, len(returnedOwnerRefs) == 1, "Only one owner reference is expected")
	assert.Equal(t, returnedOwnerRefs[0].Name, podWithOwnerRef.Name, "Unexpected owner reference name")
	assert.Equal(t, returnedOwnerRefs[0].UID, podWithOwnerRef.UID, "Unexpected owner reference UID")
	assert.Equal(t, returnedOwnerRefs[0].Kind, "Pod", "Unexpected owner reference Kind")
	assert.Equal(t, returnedOwnerRefs[0].APIVersion, v1.SchemeGroupVersion.String(), "Unexpected owner reference Kind")

	returnedOwnerRefs = getOwnerReference(podWithNoOwnerRef)
	assert.Assert(t, len(returnedOwnerRefs) == 1, "Only one owner reference is expected")
	assert.Equal(t, returnedOwnerRefs[0].Name, podWithNoOwnerRef.Name, "Unexpected owner reference name")
	assert.Equal(t, returnedOwnerRefs[0].UID, podWithNoOwnerRef.UID, "Unexpected owner reference UID")
	assert.Equal(t, returnedOwnerRefs[0].Kind, "Pod", "Unexpected owner reference Kind")
	assert.Equal(t, returnedOwnerRefs[0].APIVersion, v1.SchemeGroupVersion.String(), "Unexpected owner reference Kind")
}
