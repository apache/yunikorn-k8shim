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
	"errors"
	"fmt"
	"reflect"
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

func TestConvert2ConfigMap(t *testing.T) {
	configMap := &v1.ConfigMap{}
	result := Convert2ConfigMap(configMap)
	assert.Equal(t, result != nil, true)
	assert.Equal(t, reflect.DeepEqual(result, configMap), true)

	obj := struct{}{}
	result = Convert2ConfigMap(obj)
	assert.Equal(t, result == nil, true)

	pod := &v1.Pod{}
	result = Convert2ConfigMap(pod)
	assert.Equal(t, result == nil, true)
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
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.DomainYuniKorn + "namespace.quota": "expecting JSON object",
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

func TestGetNamespaceMaxAppsFromAnnotation(t *testing.T) {
	testCases := []struct {
		namespace      *v1.Namespace
		expectedMaxApp string
	}{
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
			},
		}, ""},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceMaxApps: "5",
				},
			},
		}, "5"},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceMaxApps: "-5",
				},
			},
		}, ""},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					constants.NamespaceMaxApps: "error",
				},
			},
		}, ""},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("namespace: %v", tc.namespace), func(t *testing.T) {
			maxApp := GetNamespaceMaxAppsFromAnnotation(tc.namespace)
			assert.Equal(t, maxApp, tc.expectedMaxApp)
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

// nolint: funlen
func TestGetApplicationIDFromPod(t *testing.T) {
	defer SetPluginMode(false)
	defer func() { conf.GetSchedulerConf().GenerateUniqueAppIds = false }()

	appIDInCanonicalLabel := "CanonicalLabelAppID"
	appIDInAnnotation := "annotationAppID"
	appIDInLabel := "labelAppID"
	appIDInSelector := "sparkLabelAppID"
	testCases := []struct {
		name                    string
		pod                     *v1.Pod
		expectedAppID           string
		expectedAppIDPluginMode string
		generateUniqueAppIds    bool
	}{
		{"AppID defined in canonical label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.CanonicalLabelApplicationID: appIDInCanonicalLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInCanonicalLabel, appIDInCanonicalLabel, false},
		{"AppID defined in annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInAnnotation, appIDInAnnotation, false},
		{"AppID defined in legacy label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInLabel, appIDInLabel, false},
		{"AppID defined in spark app selector label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.SparkLabelAppID: appIDInSelector},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInSelector, appIDInSelector, false},
		{"AppID defined in canonical label and annotation, canonical label win", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
				Labels:      map[string]string{constants.CanonicalLabelApplicationID: appIDInCanonicalLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInCanonicalLabel, appIDInCanonicalLabel, false},
		{"AppID defined in annotation and legacy label, annotation win", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
				Labels:      map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInAnnotation, appIDInAnnotation, false},
		{"Spark AppID defined in legacy label and spark app selector, legacy label win", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					constants.LabelApplicationID: appIDInLabel,
					constants.SparkLabelAppID:    appIDInSelector,
				},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, appIDInLabel, appIDInLabel, false},
		{"No AppID defined", &v1.Pod{}, "", "", false},
		{"No AppID defined but not generateUnique", &v1.Pod{
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
		{"Unique autogen token found with generateUnique in canonical AppId label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "testns",
				UID:       "podUid",
				Labels:    map[string]string{constants.CanonicalLabelApplicationID: "testns-uniqueautogen"},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, "testns-podUid", "testns-podUid", true},
		{"Unique autogen token found with generateUnique in legacy AppId labels", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "testns",
				UID:       "podUid",
				Labels:    map[string]string{constants.LabelApplicationID: "testns-uniqueautogen"},
			},
			Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		}, "testns-podUid", "testns-podUid", true},
		{"Non-yunikorn schedulerName with canonical AppId label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.CanonicalLabelApplicationID: appIDInCanonicalLabel},
			},
			Spec: v1.PodSpec{SchedulerName: "default"},
		}, "", "", false},
		{"Non-yunikorn schedulerName with legacy AppId label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
			Spec: v1.PodSpec{SchedulerName: "default"},
		}, "", "", false},
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf.GetSchedulerConf().GenerateUniqueAppIds = tc.generateUniqueAppIds
			SetPluginMode(false)
			assert.Equal(t, IsPluginMode(), false)
			appID := GetApplicationIDFromPod(tc.pod)
			assert.Equal(t, appID, tc.expectedAppID, "Wrong appID (standard mode)")
			SetPluginMode(true)
			assert.Equal(t, IsPluginMode(), true)
			appID2 := GetApplicationIDFromPod(tc.pod)
			assert.Equal(t, appID2, tc.expectedAppIDPluginMode, "Wrong appID (plugin mode)")
		})
	}
}

func TestCheckAppIdInPod(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *v1.Pod
		expected error
	}{
		{
			name: "consistent app ID",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: "app-123",
						constants.SparkLabelAppID:             "app-123",
						constants.LabelApplicationID:          "app-123",
					},
					Annotations: map[string]string{
						constants.AnnotationApplicationID: "app-123",
					},
				},
			},
			expected: nil,
		},
		{
			name: "inconsistent app ID in labels",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: "app-123",
						constants.SparkLabelAppID:             "app-456",
					},
				},
			},
			expected: errors.New("label spark-app-selector: \"app-456\" doesn't match label yunikorn.apache.org/app-id: \"app-123\""),
		},
		{
			name: "inconsistent app ID between label and annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: "app-123",
					},
					Annotations: map[string]string{
						constants.AnnotationApplicationID: "app-456",
					},
				},
			},
			expected: errors.New("annotation yunikorn.apache.org/app-id: \"app-456\" doesn't match label yunikorn.apache.org/app-id: \"app-123\""),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckAppIdInPod(tc.pod)
			if tc.expected != nil {
				assert.ErrorContains(t, err, tc.expected.Error())
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestCheckQueueNameInPod(t *testing.T) {
	testCases := []struct {
		name     string
		pod      *v1.Pod
		expected error
	}{
		{
			name: "consistent queue name",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.CanonicalLabelQueueName: "root.a",
						constants.LabelQueueName:          "root.a",
					},
					Annotations: map[string]string{
						constants.AnnotationQueueName: "root.a",
					},
				},
			},
			expected: nil,
		},
		{
			name: "inconsistent app ID in labels",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.CanonicalLabelQueueName: "root.a",
						constants.LabelQueueName:          "root.b",
					},
				},
			},
			expected: errors.New("label queue: \"root.b\" doesn't match label yunikorn.apache.org/queue: \"root.a\""),
		},
		{
			name: "inconsistent app ID between label and annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.CanonicalLabelQueueName: "root.a",
					},
					Annotations: map[string]string{
						constants.AnnotationQueueName: "root.b",
					},
				},
			},
			expected: errors.New("annotation yunikorn.apache.org/queue: \"root.b\" doesn't match label yunikorn.apache.org/queue: \"root.a\""),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckQueueNameInPod(tc.pod)
			if tc.expected != nil {
				assert.ErrorContains(t, err, tc.expected.Error())
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestValidatePodLabelAnnotation(t *testing.T) {
	labelKeys := []string{"labelKey1", "labelKey2"}
	annotationKeys := []string{"annotationKey1", "annotationKey2"}

	testCases := []struct {
		name     string
		pod      *v1.Pod
		expected error
	}{
		{
			name:     "empty pod",
			pod:      &v1.Pod{},
			expected: nil,
		},
		{
			name: "pod with all values are consistent",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"labelKey1": "value1",
						"labelKey2": "value1",
					},
					Annotations: map[string]string{
						"annotationKey1": "value1",
						"annotationKey2": "value1",
					},
				},
			},
			expected: nil,
		},
		{
			name: "pod with inconsistent value in labels",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"labelKey1": "value1",
						"labelKey2": "value2",
					},
				},
			},
			expected: errors.New("label labelKey2: \"value2\" doesn't match label labelKey1: \"value1\""),
		},
		{
			name: "pod with inconsistent value between label and annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"labelKey1": "value1",
					},
					Annotations: map[string]string{
						"annotationKey1": "value2",
					},
				},
			},
			expected: errors.New("annotation annotationKey1: \"value2\" doesn't match label labelKey1: \"value1\""),
		},
		{
			name: "pod with inconsistent value in annotations",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"annotationKey1": "value1",
						"annotationKey2": "value2",
					},
				},
			},
			expected: errors.New("annotation annotationKey2: \"value2\" doesn't match annotation annotationKey1: \"value1\""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePodLabelAnnotation(tc.pod, labelKeys, annotationKeys)
			if tc.expected != nil {
				assert.ErrorContains(t, err, tc.expected.Error())
			} else {
				assert.NilError(t, err)
			}
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

	assert.Equal(t, "namespace-uniqueautogen",
		GenerateApplicationID("namespace", true, ""))
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
	queueInCanonicalLabel := "sandboxCanonicalLabel"
	queueInLabel := "sandboxLabel"
	queueInAnnotation := "sandboxAnnotation"
	testCases := []struct {
		name          string
		pod           *v1.Pod
		expectedQueue string
	}{
		{
			name: "Queue defined in canonical label",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{constants.CanonicalLabelQueueName: queueInCanonicalLabel},
				},
			},
			expectedQueue: queueInCanonicalLabel,
		},
		{
			name: "Queue defined in annotation",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{constants.AnnotationQueueName: queueInAnnotation},
				},
			},
			expectedQueue: queueInAnnotation,
		},
		{
			name: "Queue defined in legacy label",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{constants.LabelQueueName: queueInLabel},
				},
			},
			expectedQueue: queueInLabel,
		},
		{
			name: "Queue defined in canonical label and annotation, canonical label win",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{constants.CanonicalLabelQueueName: queueInCanonicalLabel},
					Annotations: map[string]string{constants.AnnotationQueueName: queueInAnnotation},
				},
			},
			expectedQueue: queueInCanonicalLabel,
		},
		{
			name: "Queue defined in annotation and legacy label, annotation win",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{constants.LabelQueueName: queueInLabel},
					Annotations: map[string]string{constants.AnnotationQueueName: queueInAnnotation},
				},
			},
			expectedQueue: queueInAnnotation,
		},
		{
			name: "No queue defined",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
			},
			expectedQueue: "",
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

func TestIsPodRunning(t *testing.T) {
	pod := &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	assert.Equal(t, IsPodRunning(pod), true)

	pod = &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}
	assert.Equal(t, IsPodRunning(pod), false)
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
		{"Setting by deprecated annotation", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
				Annotations: map[string]string{
					constants.OldAnnotationPlaceholderFlag: "true", // nolint:staticcheck
				},
			},
		}, true},
		{"Setting by deprecated label", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
				Labels: map[string]string{
					constants.OldLabelPlaceholderFlag: "true", // nolint:staticcheck
				},
			},
		}, true},
		{"Set new placeholder annotation and old placeholder label/annotation together, new annotation has higher priority", &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-01",
				UID:  "UID-01",
				Labels: map[string]string{
					constants.OldLabelPlaceholderFlag: "false", // nolint:staticcheck
				},
				Annotations: map[string]string{
					constants.AnnotationPlaceholderFlag:    "true",
					constants.OldAnnotationPlaceholderFlag: "false", // nolint:staticcheck
				},
			},
		}, true},
		{"Pod without placeholder annotation", &v1.Pod{
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

func TestGetCoreSchedulerConfigFromConfigMap(t *testing.T) {
	// case: mapping
	cm := map[string]string{
		"queues.yaml": "test",
	}
	assert.Equal(t, "test", GetCoreSchedulerConfigFromConfigMap(cm))

	// case: not mapping
	cm = map[string]string{
		"unknow.yaml": "test",
	}
	assert.Equal(t, "", GetCoreSchedulerConfigFromConfigMap(cm))

	// case: nil
	assert.Equal(t, "", GetCoreSchedulerConfigFromConfigMap(nil))

	// case: empty
	cm = map[string]string{}
	assert.Equal(t, "", GetCoreSchedulerConfigFromConfigMap(cm))
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
	confMap := conf.FlattenConfigMaps([]*v1.ConfigMap{
		{Data: map[string]string{}},
		{Data: map[string]string{conf.CMSvcClusterID: "new"}, BinaryData: map[string][]byte{"queues.yaml.gz": b.Bytes()}},
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

func TestWaitForCondition(t *testing.T) {
	target := false
	eval := func() bool {
		return target
	}
	tests := []struct {
		input    bool
		interval time.Duration
		timeout  time.Duration
		output   error
	}{
		{true, time.Duration(1) * time.Second, time.Duration(2) * time.Second, nil},
		{false, time.Duration(1) * time.Second, time.Duration(2) * time.Second, ErrorTimeout},
		{true, time.Duration(3) * time.Second, time.Duration(2) * time.Second, nil},
		{false, time.Duration(3) * time.Second, time.Duration(2) * time.Second, ErrorTimeout},
	}
	for _, test := range tests {
		target = test.input
		get := WaitForCondition(eval, test.timeout, test.interval)
		if test.output == nil {
			assert.NilError(t, get)
		} else {
			assert.Equal(t, get.Error(), test.output.Error())
		}
	}
}
