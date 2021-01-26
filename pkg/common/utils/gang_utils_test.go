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

package utils

import (
	"math"
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
)

func TestFindAppTaskGroup(t *testing.T) {
	taskGroups := []*v1alpha1.TaskGroup{
		{
			Name:      "test-group-0",
			MinMember: 1,
			MinResource: map[string]resource.Quantity{
				"cpu": resource.MustParse("500m"),
			},
		},
		{
			Name:      "test-group-1",
			MinMember: 2,
			MinResource: map[string]resource.Quantity{
				"cpu": resource.MustParse("500m"),
			},
		},
	}

	tg, err := FindAppTaskGroup(taskGroups, "")
	assert.NilError(t, err)
	assert.Assert(t, tg == nil)

	tg, err = FindAppTaskGroup(nil, "")
	assert.NilError(t, err)
	assert.Assert(t, tg == nil)

	tg, err = FindAppTaskGroup(nil, "test-group-0")
	assert.NilError(t, err)
	assert.Assert(t, tg == nil)

	tg, err = FindAppTaskGroup(taskGroups, "test-group-3")
	assert.Error(t, err, "taskGroup test-group-3 is not defined in the application")
	assert.Assert(t, tg == nil)

	tg, err = FindAppTaskGroup(taskGroups, "test-group-1")
	assert.NilError(t, err)
	assert.Equal(t, tg.Name, "test-group-1")
	assert.Equal(t, tg.MinMember, int32(2))
}

func TestGeneratePlaceholderName(t *testing.T) {
	name := GeneratePlaceholderName("my-group", "app0001", 100)
	assert.Equal(t, name, "tg-my-group-app0001-100")

	name = GeneratePlaceholderName("my-group",
		"app00000000000000000000000000000000000000000001", 100)
	assert.Equal(t, name, "tg-my-group-app0000000000000000000000000-100")
	assert.Assert(t, len(name) < 63)

	name = GeneratePlaceholderName("a-very-long-task-group-name------------------------------------------",
		"a-very-long-app-ID-----------------------------------------------------------------", 100)
	assert.Equal(t, name, "tg-a-very-long-task-gro-a-very-long-app-ID-----------100")
	assert.Assert(t, len(name) < 63)

	name = GeneratePlaceholderName("a-very-long-task-group-name------------------------------------------",
		"a-very-long-app-ID-----------------------------------------------------------------", math.MaxInt32)
	assert.Equal(t, name, "tg-a-very-long-task-gro-a-very-long-app-ID-----------2147483647")
	assert.Assert(t, len(name) == 63)
}

func TestGetTaskGroupFromPodSpec(t *testing.T) {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
			Annotations: map[string]string{
				constants.AnnotationTaskGroupName: "test-task-group",
			},
		},
	}

	assert.Equal(t, GetTaskGroupFromPodSpec(pod), "test-task-group")

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}

	assert.Equal(t, GetTaskGroupFromPodSpec(pod), "")
}

func TestTaskGroupInstanceCountMap(t *testing.T) {
	counts := NewTaskGroupInstanceCountMap()
	assert.Equal(t, counts.Size(), 0)
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g1"), int32(0))
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g2"), int32(0))

	counts = NewTaskGroupInstanceCountMap()
	counts.AddOne("g1")
	counts.AddOne("g1")
	counts.AddOne("g1")
	counts.AddOne("g2")
	counts.AddOne("g2")
	assert.Equal(t, counts.Size(), 2)
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g1"), int32(3))
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g2"), int32(2))

	counts.DeleteOne("g1")
	counts.DeleteOne("g2")
	assert.Equal(t, counts.Size(), 2)
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g1"), int32(2))
	assert.Equal(t, counts.GetTaskGroupInstanceCount("g2"), int32(1))

	counts1 := NewTaskGroupInstanceCountMap()
	counts2 := NewTaskGroupInstanceCountMap()
	assert.Equal(t, counts1.Equals(counts2), true)
	counts1.Add("g1", 101)
	counts2.Add("g1", 101)
	assert.Equal(t, counts1.Equals(counts2), true)
	counts1.Add("g1", 100)
	counts2.Add("g1", 101)
	assert.Equal(t, counts1.Equals(counts2), false)

	counts1 = NewTaskGroupInstanceCountMap()
	counts2 = NewTaskGroupInstanceCountMap()
	counts1.AddOne("g1")
	counts1.AddOne("g2")
	counts1.AddOne("g3")
	counts1.AddOne("g4")
	counts1.AddOne("g5")
	counts2.AddOne("g5")
	counts2.AddOne("g4")
	counts2.AddOne("g3")
	counts2.AddOne("g2")
	counts2.AddOne("g1")
	assert.Equal(t, counts1.Equals(counts2), true)

	counts1 = NewTaskGroupInstanceCountMap()
	counts2 = NewTaskGroupInstanceCountMap()
	counts1.AddOne("g1")
	counts1.AddOne("g2")
	counts2.AddOne("g1")
	assert.Equal(t, counts1.Equals(counts2), false)

	counts1 = NewTaskGroupInstanceCountMap()
	counts2 = NewTaskGroupInstanceCountMap()
	counts1.AddOne("g1")
	counts2.AddOne("g2")
	counts2.AddOne("g1")
	assert.Equal(t, counts1.Equals(counts2), false)

	var nilOne *TaskGroupInstanceCountMap
	var nilTwo *TaskGroupInstanceCountMap
	assert.Equal(t, nilOne.Equals(nilTwo), true)

	empty := NewTaskGroupInstanceCountMap()
	assert.Equal(t, nilOne.Equals(empty), false)
}

// nolint: funlen
func TestGetTaskGroupFromAnnotation(t *testing.T) {
	// correct json
	testGroup := `
	[
		{
			"name": "test-group-1",
			"minMember": 10,
			"minResource": {
				"cpu": 1,
				"memory": "2Gi"
			},
			"nodeSelector": {
				"test": "testnode",
				"locate": "west"
			},
			"tolerations": [
				{
					"key": "key",
					"operator": "Equal",
					"value": "value",
					"effect": "NoSchedule"
				}
			]
		},
		{
			"name": "test-group-2",
			"minMember": 5,
			"minResource": {
				"cpu": 2,
				"memory": "4Gi"
			}
		}
	]`
	testGroup2 := `
	[
		{
			"name": "test-group-3",
			"minMember": 3,
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// Error json
	testGroupErr := `
	[
		{
			"name": "test-group-err-1",
			"minMember": "ERR",
			"minResource": {
				"cpu": "ERR",
				"memory": "ERR"
			},
		}
	]`
	// without name
	testGroupErr2 := `
	[
		{
			"minMember": 3,
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// without minMember
	testGroupErr3 := `
	[
		{
			"name": "test-group-err-2",
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// withot minResource
	testGroupErr4 := `
	[
		{
			"name": "test-group-err-4",
			"minMember": 3,
		}
	]`
	// negative minMember
	testGroupErr5 := `
	[
		{
			"name": "test-group-err-5",
			"minMember": -100,
		}
	]`
	// Insert task group info to pod annotation
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-err",
			Namespace: "test",
			UID:       "test-pod-UID-err",
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	// Empty case
	taskGroupEmpty, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupEmpty == nil)
	assert.Assert(t, err == nil)
	// Error case
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr}
	taskGroupErr, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr2}
	taskGroupErr2, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr2 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr3}
	taskGroupErr3, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr3 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr4}
	taskGroupErr4, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr4 == nil)
	assert.Assert(t, err != nil)
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr5}
	taskGroupErr5, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr5 == nil)
	assert.Assert(t, err != nil)
	// Correct case
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroup}
	taskGroups, err := GetTaskGroupsFromAnnotation(pod)
	assert.NilError(t, err)
	// Group value check
	assert.Equal(t, taskGroups[0].Name, "test-group-1")
	assert.Equal(t, taskGroups[0].MinMember, int32(10))
	assert.Equal(t, taskGroups[0].MinResource["cpu"], resource.MustParse("1"))
	assert.Equal(t, taskGroups[0].MinResource["memory"], resource.MustParse("2Gi"))
	assert.Equal(t, taskGroups[1].Name, "test-group-2")
	assert.Equal(t, taskGroups[1].MinMember, int32(5))
	assert.Equal(t, taskGroups[1].MinResource["cpu"], resource.MustParse("2"))
	assert.Equal(t, taskGroups[1].MinResource["memory"], resource.MustParse("4Gi"))
	// NodeSelector check
	assert.Equal(t, taskGroups[0].NodeSelector["test"], "testnode")
	assert.Equal(t, taskGroups[0].NodeSelector["locate"], "west")
	// Toleration check
	var tolerations []v1.Toleration
	toleration := v1.Toleration{
		Key:      "key",
		Operator: "Equal",
		Value:    "value",
		Effect:   "NoSchedule",
	}
	tolerations = append(tolerations, toleration)
	assert.DeepEqual(t, taskGroups[0].Tolerations, tolerations)

	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroup2}
	taskGroups2, err := GetTaskGroupsFromAnnotation(pod)
	assert.NilError(t, err)
	assert.Equal(t, taskGroups2[0].Name, "test-group-3")
	assert.Equal(t, taskGroups2[0].MinMember, int32(3))
	assert.Equal(t, taskGroups2[0].MinResource["cpu"], resource.MustParse("2"))
	assert.Equal(t, taskGroups2[0].MinResource["memory"], resource.MustParse("1Gi"))
}
