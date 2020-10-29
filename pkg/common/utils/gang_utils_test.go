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
}
