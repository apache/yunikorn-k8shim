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
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestConvert2Pod(t *testing.T) {
	pod, err := Convert2Pod(&v1.Node{})
	assert.Assert(t, err != nil)
	assert.Assert(t, pod == nil)

	pod, err = Convert2Pod(&v1.Pod{})
	assert.NilError(t, err)
	assert.Assert(t, pod != nil)
}

func TestIsAssignedPod(t *testing.T) {
	assigned := IsAssignedPod(&v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "some-node",
		},
	})
	assert.Equal(t, assigned, true)

	assigned = IsAssignedPod(&v1.Pod{
		Spec: v1.PodSpec{},
	})
	assert.Equal(t, assigned, false)

	assigned = IsAssignedPod(&v1.Pod{})
	assert.Equal(t, assigned, false)
}

func TestGetNamespaceQuotaFromAnnotation(t *testing.T) {
	testCases := []struct {
		namespace        *v1.Namespace
		expectedResource *si.Resource
	}{
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu": "1",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.memory": "128M",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.Memory, 128*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "error",
					constants.DomainYuniKorn + "namespace.max.memory": "128M",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "1",
					constants.DomainYuniKorn + "namespace.max.memory": "error",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "error",
					constants.DomainYuniKorn + "namespace.max.memory": "error",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "1",
					constants.DomainYuniKorn + "namespace.max.memory": "64M",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			Build()},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("namespace: %v", tc.namespace), func(t *testing.T) {
			res := GetNamespaceQuotaFromAnnotation(tc.namespace)
			assert.Assert(t, common.Equals(res, tc.expectedResource))
		})
	}
}

func TestGetNamespaceQuotaFromAnnotationUsingNewAnnotations(t *testing.T) {
	testCases := []struct {
		namespace        *v1.Namespace
		expectedResource *si.Resource
	}{
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.quota": "{\"cpu\": \"5\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 5000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.quota": "{\"memory\": \"256M\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.Memory, 256*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.quota": "{\"cpu\": \"1\", \"memory\": \"64M\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.quota": "{\"cpu\": \"1\", \"memory\": \"64M\", \"nvidia.com/gpu\": \"1\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			AddResource("nvidia.com/gpu", 1).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.quota": "{\"cpu\": \"error\", \"memory\": \"error\"}",
				},
			},
		}, nil},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("namespace: %v", tc.namespace), func(t *testing.T) {
			res := GetNamespaceQuotaFromAnnotation(tc.namespace)
			assert.Assert(t, common.Equals(res, tc.expectedResource))
		})
	}
}

func TestGetNamespaceGuaranteedFromAnnotation(t *testing.T) {
	testCases := []struct {
		namespace        *v1.Namespace
		expectedResource *si.Resource
	}{
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceGuaranteed: "{\"cpu\": \"5\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 5000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceGuaranteed: "{\"memory\": \"256M\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.Memory, 256*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceGuaranteed: "{\"cpu\": \"1\", \"memory\": \"64M\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceGuaranteed: "{\"cpu\": \"1\", \"memory\": \"64M\", \"nvidia.com/gpu\": \"1\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			AddResource("nvidia.com/gpu", 1).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceGuaranteed: "{\"cpu\": \"error\", \"memory\": \"error\"}",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceGuaranteed: "error",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
			},
		}, nil},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("namespace: %v", tc.namespace), func(t *testing.T) {
			res := GetNamespaceGuaranteedFromAnnotation(tc.namespace)
			assert.Assert(t, common.Equals(res, tc.expectedResource))
		})
	}
}

func TestGetNamespaceQuotaFromAnnotationUsingNewAndOldAnnotations(t *testing.T) {
	testCases := []struct {
		namespace        *v1.Namespace
		expectedResource *si.Resource
	}{
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu": "1",
					constants.DomainYuniKorn + "namespace.quota":   "{\"cpu\": \"5\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 5000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.memory": "128M",
					constants.DomainYuniKorn + "namespace.quota":      "{\"memory\": \"256M\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.Memory, 256*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "5",
					constants.DomainYuniKorn + "namespace.max.memory": "32M",
					constants.DomainYuniKorn + "namespace.quota":      "{\"cpu\": \"1\", \"memory\": \"64M\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "1",
					constants.DomainYuniKorn + "namespace.max.memory": "64M",
					constants.DomainYuniKorn + "namespace.quota":      "{\"cpu\": \"1\", \"memory\": \"64M\", \"nvidia.com/gpu\": \"1\"}",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(siCommon.CPU, 1000).
			AddResource(siCommon.Memory, 64*1000*1000).
			AddResource("nvidia.com/gpu", 1).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.max.cpu":    "1",
					constants.DomainYuniKorn + "namespace.max.memory": "64M",
					constants.DomainYuniKorn + "namespace.quota":      "{\"cpu\": \"error\", \"memory\": \"error\"}",
				},
			},
		}, nil},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("namespace: %v", tc.namespace), func(t *testing.T) {
			res := GetNamespaceQuotaFromAnnotation(tc.namespace)
			assert.Assert(t, common.Equals(res, tc.expectedResource))
		})
	}
}

// nolint: funlen
func TestPodUnderCondition(t *testing.T) {
	// pod has no condition set
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test",
			UID:       "test-pod-UID",
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	condition := &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  "some-reason",
		Message: "some-message",
	}

	assert.Equal(t, PodUnderCondition(pod, condition), false)

	// pod has condition set and condition not changed
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test",
			UID:       "test-pod-UID",
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:    v1.PodScheduled,
					Status:  v1.ConditionFalse,
					Reason:  "some-reason",
					Message: "some-message",
				},
			},
		},
	}

	condition = &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  "some-reason",
		Message: "some-message",
	}

	assert.Equal(t, PodUnderCondition(pod, condition), true)

	// pod has condition set and condition has changed
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test",
			UID:       "test-pod-UID",
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:    v1.PodScheduled,
					Status:  v1.ConditionFalse,
					Reason:  "some-reason",
					Message: "some-message",
				},
			},
		},
	}

	condition = &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  "some-other-reason",
		Message: "some-message",
	}

	assert.Equal(t, PodUnderCondition(pod, condition), false)

	// pod has multiple condition set, one condition has changed
	time0 := time.Now()
	time1 := time0.Add(100 * time.Second)

	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "test",
			UID:       "test-pod-UID",
		},
		Spec: v1.PodSpec{},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				{
					Type:               v1.PodScheduled,
					Status:             v1.ConditionFalse,
					Reason:             "some-reason",
					Message:            "some-message",
					LastTransitionTime: metav1.Time{Time: time0},
				},
				{
					Type:               v1.ContainersReady,
					Status:             v1.ConditionFalse,
					Reason:             "some-reason",
					Message:            "some-message",
					LastTransitionTime: metav1.Time{Time: time1},
				},
			},
		},
	}

	condition = &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionTrue,
		Reason:  "scheduled",
		Message: "",
	}

	assert.Equal(t, PodUnderCondition(pod, condition), false)
}

func TestGetApplicationIDFromPod(t *testing.T) {
	defer SetPluginMode(false)
	defer func() { conf.GetSchedulerConf().GenerateUniqueAppIds = false }()

	appIDInLabel := "labelAppID"
	appIDInAnnotation := "annotationAppID"
	appIDInSelector := "selectorAppID"
	sparkIDInAnnotation := "sparkAnnotationAppID"
	testCases := []struct {
		name                    string
		pod                     *v1.Pod
		expectedAppID           string
		expectedAppIDPluginMode string
		generateUniqueAppIds    bool
	}{
		{"AppID defined in label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInLabel, appIDInLabel, false},
		{"No AppID defined", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "testns",
				UID:       "podUid",
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, "yunikorn-testns-autogen", "", false},
		{"No AppID defined but generateUnique", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "testns",
				UID:       "podUid",
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, "testns-podUid", "", true},
		{"Non-yunikorn schedulerName", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: "default"},
		}, "", "", false},
		{"AppID defined in annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInAnnotation, appIDInAnnotation, false},
		{"AppID defined but ignore-application set", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.AnnotationApplicationID:     appIDInAnnotation,
					constants.AnnotationIgnoreApplication: "true",
				},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInAnnotation, "", false},
		{"AppID defined and ignore-application invalid", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					constants.AnnotationApplicationID:     appIDInAnnotation,
					constants.AnnotationIgnoreApplication: "invalid",
				},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInAnnotation, appIDInAnnotation, false},
		{"AppID defined in label and annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
				Labels:      map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInAnnotation, appIDInAnnotation, false},

		{"Spark AppID defined in spark app selector", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.SparkLabelAppID: appIDInSelector},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInSelector, appIDInSelector, false},
		{"Spark AppID defined in spark app selector and annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{constants.SparkLabelAppID: appIDInSelector},
				Annotations: map[string]string{constants.AnnotationApplicationID: sparkIDInAnnotation},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, sparkIDInAnnotation, sparkIDInAnnotation, false},
		{"Spark AppID defined in spark app selector and annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{constants.SparkLabelAppID: appIDInSelector, constants.LabelApplicationID: appIDInLabel},
				Annotations: map[string]string{constants.AnnotationApplicationID: sparkIDInAnnotation},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, sparkIDInAnnotation, sparkIDInAnnotation, false},
		{"No AppID defined", &v1.Pod{}, "", "", false},
		{"Spark AppID defined in spark app selector and label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.SparkLabelAppID: appIDInSelector, constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInLabel, appIDInLabel, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf.GetSchedulerConf().GenerateUniqueAppIds = tc.generateUniqueAppIds
			SetPluginMode(false)
			appID := GetApplicationIDFromPod(tc.pod)
			assert.Equal(t, appID, tc.expectedAppID, "Wrong appID (standard mode)")
			SetPluginMode(true)
			appID2 := GetApplicationIDFromPod(tc.pod)
			assert.Equal(t, appID2, tc.expectedAppIDPluginMode, "Wrong appID (plugin mode)")
		})
	}
}

func TestGenerateApplicationID(t *testing.T) {
	assert.Equal(t, "yunikorn-this-is-a-namespace-autogen",
		GenerateApplicationID("this-is-a-namespace", false, "pod-uid"))

	assert.Equal(t, "this-is-a-namespace-pod-uid",
		GenerateApplicationID("this-is-a-namespace", true, "pod-uid"))

	assert.Equal(t, "yunikorn-short-autogen",
		GenerateApplicationID("short", false, "pod-uid"))

	assert.Equal(t, "short-pod-uid",
		GenerateApplicationID("short", true, "pod-uid"))

	assert.Equal(t, "yunikorn-longlonglonglonglonglonglonglonglonglonglonglonglonglo",
		GenerateApplicationID(strings.Repeat("long", 100), false, "pod-uid"))

	assert.Equal(t, "longlonglonglonglonglonglo-pod-uid",
		GenerateApplicationID(strings.Repeat("long", 100), true, "pod-uid"))
}

func TestMergeMaps(t *testing.T) {
	testCases := []struct {
		name     string
		input1   map[string]string
		input2   map[string]string
		expected map[string]string
	}{
		{
			name:     "Both inputs nil",
			input1:   nil,
			input2:   nil,
			expected: nil,
		},
		{
			name:   "First input nil, Second input not nil",
			input1: nil,
			input2: map[string]string{"a": "b"},
			expected: map[string]string{
				"a": "b",
			},
		},
		{
			name:   "First input not nil, Second input nil",
			input1: map[string]string{"a": "b"},
			input2: nil,
			expected: map[string]string{
				"a": "b",
			},
		},
		{
			name: "Merge with existing key",
			input1: map[string]string{
				"a": "a1",
			},
			input2: map[string]string{
				"a": "a2",
			},
			expected: map[string]string{
				"a": "a2",
			},
		},
		{
			name: "Merge with additional keys",
			input1: map[string]string{
				"a": "a1",
				"b": "b1",
				"c": "c1",
			},
			input2: map[string]string{
				"a": "a2",
				"b": "b2",
				"d": "d2",
			},
			expected: map[string]string{
				"a": "a2",
				"b": "b2",
				"c": "c1",
				"d": "d2",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := MergeMaps(tc.input1, tc.input2)
			assert.DeepEqual(t, result, tc.expected)
		})
	}
}

func TestGetUserFromPodLabel(t *testing.T) {
	userInLabel := "testuser"
	userNotInLabel := constants.DefaultUser
	customUserKeyLabel := "test"
	testCases := []struct {
		name         string
		userLabelKey string
		pod          *v1.Pod
		expectedUser string
	}{
		{"User defined in label with default key", constants.DefaultUserLabel, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.DefaultUserLabel: userInLabel},
			},
		}, userInLabel},
		{"The length of UserKeyLabel value is 0", constants.DefaultUserLabel, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{customUserKeyLabel: ""},
			},
		}, userNotInLabel},
		{"User not defined in label", constants.DefaultUserLabel, &v1.Pod{}, userNotInLabel},
		{"UserKeyLabel is empty and the user definded in the pod labels with default key", "", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.DefaultUserLabel: userInLabel},
			},
		}, userInLabel},
		{"UserKeyLabel is empty and the user isn't defined in the pod labels", "", &v1.Pod{}, userNotInLabel},
		{"UserKeyLabel is changed and the user definded in the pod labels", customUserKeyLabel, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{customUserKeyLabel: userInLabel},
			},
		}, userInLabel},
		{"UserKeyLabel is changed and the user isn't defined in the pod labels", customUserKeyLabel, &v1.Pod{}, userNotInLabel},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := conf.GetSchedulerConf()
			// The default UserLabelKey could be set with the custom UserLabelKey.
			if tc.userLabelKey != constants.DefaultUserLabel {
				conf.UserLabelKey = tc.userLabelKey
			}

			userID, _ := GetUserFromPod(tc.pod)
			assert.Equal(t, userID, tc.expectedUser)
			// The order of test cases is allowed to impact other test case.
			conf.UserLabelKey = constants.DefaultUserLabel
		})
	}
}

func TestGetUserFromPodAnnotation(t *testing.T) {
	const userAndGroups = "{\"user\":\"test\",\"groups\":[\"devops\",\"test\"]}"
	const emptyUserAndGroups = "{\"user\":\"\",\"groups\":[\"devops\",\"test\"]}"

	testCases := []struct {
		name           string
		pod            *v1.Pod
		expectedUser   string
		expectedGroups []string
	}{
		{"User/groups properly defined", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					userInfoKey: userAndGroups},
			},
		}, "test", []string{"devops", "test"}},
		{"Empty username", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					userInfoKey: emptyUserAndGroups},
			},
		}, "nobody", []string{"devops", "test"}},
		{"Invalid JSON", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					userInfoKey: "xyzxyz"},
			},
		}, "nobody", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			userID, groups := GetUserFromPod(tc.pod)
			assert.DeepEqual(t, userID, tc.expectedUser)
			assert.DeepEqual(t, groups, tc.expectedGroups)
		})
	}
}

func TestGetQueueNameFromPod(t *testing.T) {
	queueInLabel := "sandboxLabel"
	queueInAnnotation := "sandboxAnnotation"
	testCases := []struct {
		name          string
		pod           *v1.Pod
		expectedQueue string
	}{
		{
			name: "With queue label",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{constants.LabelQueueName: queueInLabel},
				},
			},
			expectedQueue: queueInLabel,
		},
		{
			name: "With queue annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{constants.AnnotationQueueName: queueInAnnotation},
				},
			},
			expectedQueue: queueInAnnotation,
		},
		{
			name: "With queue label and annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{constants.LabelQueueName: queueInLabel},
					Annotations: map[string]string{constants.AnnotationQueueName: queueInAnnotation},
				},
			},
			expectedQueue: queueInLabel,
		},
		{
			name: "Without queue label and annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
			},
			expectedQueue: constants.ApplicationDefaultQueue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queue := GetQueueNameFromPod(tc.pod)
			assert.Equal(t, queue, tc.expectedQueue)
		})
	}
}

func TestPodAlreadyBound(t *testing.T) {
	const fakeNodeID = "fake-node"
	testCases := []struct {
		description       string
		pod               *v1.Pod
		expectedBoundFlag bool
	}{
		{"New pod pending for scheduling",
			&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.LabelApplicationID: "app1"}},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      "",
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				}}, false},
		{"Succeed pod",
			&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.LabelApplicationID: "app1"}},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				}}, false},
		{"Failed pod",
			&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.LabelApplicationID: "app1"}},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
				}}, false},
		{"Non YK pod",
			&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.LabelApplicationID: "app1"}},
				Spec: v1.PodSpec{
					SchedulerName: "default-scheduler",
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				}}, false},
		{"Assigned pod and Running",
			&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.LabelApplicationID: "app1"}},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				}}, true},
		{"Assigned pod but Pending",
			&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.LabelApplicationID: "app1"}},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				}}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			bound := PodAlreadyBound(tc.pod)
			assert.Equal(t, bound, tc.expectedBoundFlag, tc.description)
		})
	}
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

func TestGetPlaceholderFlagFromPodSpec(t *testing.T) {
	testCases := []struct {
		description             string
		pod                     *v1.Pod
		expectedPlaceholderFlag bool
	}{

		{"Setting by annotation", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
				Annotations: map[string]string{
					constants.AnnotationPlaceholderFlag: "true",
				},
			},
		}, true},
		{"Setting by label", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
				Labels: map[string]string{
					constants.LabelPlaceholderFlag: "true",
				},
			},
		}, true},
		{"Setting both annotation and label, annotation has higher priority", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
				Annotations: map[string]string{
					constants.AnnotationPlaceholderFlag: "true",
					constants.LabelPlaceholderFlag:      "false",
				},
			},
		}, true},
		{"No setting both annotation and label", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
			},
		}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			placeHolderFlag := GetPlaceholderFlagFromPodSpec(tc.pod)
			assert.Equal(t, placeHolderFlag, tc.expectedPlaceholderFlag)
		})
	}
}

func TestGetCoreSchedulerConfigFromConfigMapNil(t *testing.T) {
	assert.Equal(t, "", GetCoreSchedulerConfigFromConfigMap(nil))
}

func TestGetCoreSchedulerConfigFromConfigMapEmpty(t *testing.T) {
	cm := map[string]string{}
	assert.Equal(t, "", GetCoreSchedulerConfigFromConfigMap(cm))
}

func TestGetCoreSchedulerConfigFromConfigMap(t *testing.T) {
	cm := map[string]string{
		"queues.yaml": "test",
	}
	assert.Equal(t, "test", GetCoreSchedulerConfigFromConfigMap(cm))
}

func TestGzipCompressedConfigMap(t *testing.T) {
	var b bytes.Buffer
	gzWriter := gzip.NewWriter(&b)
	if _, err := gzWriter.Write([]byte(configs.DefaultSchedulerConfig)); err != nil {
		t.Fatal("expected nil, got error while compressing test schedulerConfig")
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatal("expected nil, got error")
	}
	encodedConfigString := make([]byte, base64.StdEncoding.EncodedLen(len(b.Bytes())))
	base64.StdEncoding.Encode(encodedConfigString, b.Bytes())
	confMap := conf.FlattenConfigMaps([]*v1.ConfigMap{
		{Data: map[string]string{}},
		{Data: map[string]string{conf.CMSvcClusterID: "new"}, BinaryData: map[string][]byte{"queues.yaml.gz": encodedConfigString}},
	})
	config := GetCoreSchedulerConfigFromConfigMap(confMap)
	assert.Equal(t, configs.DefaultSchedulerConfig, config)
}

func TestGetExtraConfigFromConfigMapNil(t *testing.T) {
	res := GetExtraConfigFromConfigMap(nil)
	assert.Equal(t, 0, len(res))
}

func TestGetExtraConfigFromConfigMapEmpty(t *testing.T) {
	cm := map[string]string{}
	res := GetExtraConfigFromConfigMap(cm)
	assert.Equal(t, 0, len(res))
}

func TestGetExtraConfigFromConfigMapQueuesYaml(t *testing.T) {
	cm := map[string]string{
		"queues.yaml": "test",
	}
	res := GetExtraConfigFromConfigMap(cm)
	assert.Equal(t, 0, len(res))
}

func TestGetExtraConfigFromConfigMap(t *testing.T) {
	cm := map[string]string{
		"key": "value",
	}
	res := GetExtraConfigFromConfigMap(cm)
	assert.Equal(t, 1, len(res))
	value, ok := res["key"]
	assert.Assert(t, ok, "key not found")
	assert.Equal(t, "value", value, "wrong value")
}

func TestConvert2PriorityClass(t *testing.T) {
	assert.Assert(t, Convert2PriorityClass(nil) == nil)
	assert.Assert(t, Convert2PriorityClass("foo") == nil)

	preemptLower := v1.PreemptLowerPriority
	pc := schedulingv1.PriorityClass{
		ObjectMeta:       metav1.ObjectMeta{},
		Value:            0,
		GlobalDefault:    false,
		Description:      "",
		PreemptionPolicy: &preemptLower,
	}

	assert.Assert(t, Convert2PriorityClass(pc) == nil)
	result := Convert2PriorityClass(&pc)
	assert.Assert(t, result != nil)
	assert.Equal(t, result.PreemptionPolicy, &preemptLower)
}
