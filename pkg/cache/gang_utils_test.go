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
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

func TestFindAppTaskGroup(t *testing.T) {
	taskGroups := []*TaskGroup{
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
	name := GeneratePlaceholderName("my-group", "app0001")
	prefix := name[0 : len(name)-11]
	nonce := name[len(name)-10:]
	assert.Equal(t, prefix, "tg-app0001-my-group")
	assert.Equal(t, name, fmt.Sprintf("%s-%s", prefix, nonce))
	assert.Equal(t, len(name), 30)

	name = GeneratePlaceholderName("my-group",
		"app00000000000000000000000000000000000000000001")
	prefix = name[0 : len(name)-11]
	nonce = name[len(name)-10:]
	assert.Equal(t, prefix, "tg-app0000000000000000000000000-my-group")
	assert.Equal(t, name, fmt.Sprintf("%s-%s", prefix, nonce))
	assert.Equal(t, len(name), 51)

	name = GeneratePlaceholderName("a-very-long-task-group-name------------------------------------------",
		"a-very-long-app-ID-----------------------------------------------------------------")
	prefix = name[0 : len(name)-11]
	nonce = name[len(name)-10:]
	assert.Equal(t, prefix, "tg-a-very-long-app-ID-----------a-very-long-task-gro")
	assert.Equal(t, name, fmt.Sprintf("%s-%s", prefix, nonce))
	assert.Equal(t, len(name), 63)
}

func TestGetSchedulingPolicyParams(t *testing.T) {
	tests := []struct {
		key, timeoutParam string
		want              int64
		expectedStyle     string
	}{
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown placeholderTimeoutInSeconds=50=25 gangSchedulingStyle=Hard=Soft", int64(0), "Soft"},
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown placeholderTimeoutInSeconds=50 gangSchedulingStyle=Hard=Soft", int64(50), "Soft"},
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown placeholderTimeoutInSeconds=oneSecond gangSchedulingStyle=Soft", int64(0), "Soft"},
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown", int64(0), "Soft"},
		{"policyParamUndefined", "unknownPara=unkown placeholderTimeoutInSeconds=50", int64(0), "Soft"},
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown placeholderTimeoutInSeconds=50  gangSchedulingStyle=Hard", int64(50), "Hard"},
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown gangSchedulingStyle=Soft", int64(0), "Soft"},
		{constants.AnnotationSchedulingPolicyParam, "unknownPara=unkown gangSchedulingStyle=abc", int64(0), "Soft"},
		{constants.AnnotationSchedulingPolicyParam, "placeholderTimeoutInSeconds gangSchedulingStyle", int64(0), "Soft"},
	}

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

	for testID, tt := range tests {
		t.Run(tt.timeoutParam, func(t *testing.T) {
			pod.Annotations = map[string]string{tt.key: tt.timeoutParam}
			schedulingPolicyParams := GetSchedulingPolicyParam(pod)
			if schedulingPolicyParams.GetPlaceholderTimeout() != tt.want {
				t.Errorf("%d:got %d,want %d", testID, schedulingPolicyParams.GetPlaceholderTimeout(), tt.want)
			}
			if schedulingPolicyParams.GetGangSchedulingStyle() != tt.expectedStyle {
				t.Errorf("%d:got %s,want %s", testID, schedulingPolicyParams.GetGangSchedulingStyle(), tt.expectedStyle)
			}
		})
	}
}

func Test_GetPlaceholderResourceRequest(t *testing.T) {
	tests := []struct {
		name   string
		resMap map[string]resource.Quantity
		want   v1.ResourceList
	}{
		{"nil", nil, v1.ResourceList{}},
		{"empty", map[string]resource.Quantity{}, v1.ResourceList{}},
		{"base", map[string]resource.Quantity{"pods": resource.MustParse("1")}, v1.ResourceList{"pods": resource.MustParse("1")}},
		{"hugepages", map[string]resource.Quantity{"hugepages-huge": resource.MustParse("2")}, v1.ResourceList{"hugepages-huge": resource.MustParse("2")}},
		{"mixed", map[string]resource.Quantity{"pods": resource.MustParse("4"), "nvidia.com/gpu": resource.MustParse("5")}, v1.ResourceList{"pods": resource.MustParse("4"), "nvidia.com/gpu": resource.MustParse("5")}},
		{"empty key", map[string]resource.Quantity{"": resource.MustParse("1")}, v1.ResourceList{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPlaceholderResourceRequests(tt.resMap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPlaceholderResourceRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
