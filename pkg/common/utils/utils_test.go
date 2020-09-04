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
	"fmt"
	"testing"
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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
					"yunikorn.apache.org/namespace.max.cpu": "1",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(constants.CPU, 1000).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					"yunikorn.apache.org/namespace.max.memory": "128M",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(constants.Memory, 128).
			Build()},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					"yunikorn.apache.org/namespace.max.cpu":    "error",
					"yunikorn.apache.org/namespace.max.memory": "128M",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					"yunikorn.apache.org/namespace.max.cpu":    "1",
					"yunikorn.apache.org/namespace.max.memory": "error",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					"yunikorn.apache.org/namespace.max.cpu":    "error",
					"yunikorn.apache.org/namespace.max.memory": "error",
				},
			},
		}, nil},
		{&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "test",
				Annotations: map[string]string{
					"yunikorn.apache.org/namespace.max.cpu":    "1",
					"yunikorn.apache.org/namespace.max.memory": "64M",
				},
			},
		}, common.NewResourceBuilder().
			AddResource(constants.CPU, 1000).
			AddResource(constants.Memory, 64).
			Build()},
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
	appIDInLabel := "labelAppID"
	appIDInAnnotation := "annotationAppID"
	appIDInSelector := "selectorAppID"
	sparkIDInAnnotation := "sparkAnnotationAppID"
	testCases := []struct {
		name          string
		pod           *v1.Pod
		expectedError bool
		expectedAppID string
	}{
		{"AppID defined in label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
		}, false, appIDInLabel},
		{"AppID defined in annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
			},
		}, false, appIDInAnnotation},
		{"AppID defined in label and annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{constants.AnnotationApplicationID: appIDInAnnotation},
				Labels:      map[string]string{constants.LabelApplicationID: appIDInLabel},
			},
		}, false, appIDInAnnotation},

		{"Spark AppID defined in spark app selector", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.SparkLabelAppID: appIDInSelector},
			},
		}, false, appIDInSelector},
		{"Spark AppID defined in spark app selector and annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{constants.SparkLabelAppID: appIDInSelector},
				Annotations: map[string]string{constants.AnnotationApplicationID: sparkIDInAnnotation},
			},
		}, false, sparkIDInAnnotation},
		{"Spark AppID defined in spark app selector and annotation", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{constants.SparkLabelAppID: appIDInSelector, constants.LabelApplicationID: appIDInLabel},
				Annotations: map[string]string{constants.AnnotationApplicationID: sparkIDInAnnotation},
			},
		}, false, sparkIDInAnnotation},
		{"No AppID defined", &v1.Pod{}, true, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			appID, err := GetApplicationIDFromPod(tc.pod)
			if tc.expectedError {
				assert.Assert(t, err != nil, "An error is expected")
			} else {
				assert.NilError(t, err, "No error is expected")
			}
			assert.DeepEqual(t, appID, tc.expectedAppID)
		})
	}
}
