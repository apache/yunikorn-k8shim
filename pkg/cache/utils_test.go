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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

const (
	appID            = "app01"
	app2ID           = "app02"
	taskAllocationID = "ALLOCATIONID01"
)

//nolint:funlen
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
			"name": "test-group-err-3",
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
		}
	]`
	// without minResource
	testGroupErr4 := `
	[
		{
			"name": "test-group-err-4",
			"minMember": 3
		}
	]`
	// negative minMember without minResource
	testGroupErr5 := `
	[
		{
			"name": "test-group-err-5",
			"minMember": -100
		}
	]`
	// negative minMember with minResource
	testGroupErr6 := `
	[
		{
			"name": "test-group-err-6",
			"minMember": -100,
			"minResource": {
				"cpu": 2,
				"memory": "1Gi"
			}
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
	pod.Annotations = map[string]string{constants.AnnotationTaskGroups: testGroupErr6}
	taskGroupErr6, err := GetTaskGroupsFromAnnotation(pod)
	assert.Assert(t, taskGroupErr6 == nil)
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
