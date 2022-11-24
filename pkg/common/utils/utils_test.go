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
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
					"yunikorn.apache.org/namespace.max.cpu": "1",
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
					"yunikorn.apache.org/namespace.max.memory": "128M",
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
					"yunikorn.apache.org/namespace.quota": "{\"cpu\": \"5\"}",
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
					"yunikorn.apache.org/namespace.quota": "{\"memory\": \"256M\"}",
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
					"yunikorn.apache.org/namespace.quota": "{\"cpu\": \"1\", \"memory\": \"64M\"}",
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
					"yunikorn.apache.org/namespace.quota": "{\"cpu\": \"1\", \"memory\": \"64M\", \"nvidia.com/gpu\": \"1\"}",
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
					"yunikorn.apache.org/namespace.quota": "{\"cpu\": \"error\", \"memory\": \"error\"}",
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
					"yunikorn.apache.org/namespace.max.cpu": "1",
					"yunikorn.apache.org/namespace.quota":   "{\"cpu\": \"5\"}",
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
					"yunikorn.apache.org/namespace.max.memory": "128M",
					"yunikorn.apache.org/namespace.quota":      "{\"memory\": \"256M\"}",
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
					"yunikorn.apache.org/namespace.max.cpu":    "5",
					"yunikorn.apache.org/namespace.max.memory": "32M",
					"yunikorn.apache.org/namespace.quota":      "{\"cpu\": \"1\", \"memory\": \"64M\"}",
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
					"yunikorn.apache.org/namespace.max.cpu":    "1",
					"yunikorn.apache.org/namespace.max.memory": "64M",
					"yunikorn.apache.org/namespace.quota":      "{\"cpu\": \"1\", \"memory\": \"64M\", \"nvidia.com/gpu\": \"1\"}",
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
					"yunikorn.apache.org/namespace.max.cpu":    "1",
					"yunikorn.apache.org/namespace.max.memory": "64M",
					"yunikorn.apache.org/namespace.quota":      "{\"cpu\": \"error\", \"memory\": \"error\"}",
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
		{"Spark AppID defined in spark app selector and label", &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{constants.SparkLabelAppID: appIDInSelector, constants.LabelApplicationID: appIDInLabel},
			},
		}, false, appIDInLabel},
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

func TestMergeMaps(t *testing.T) {
	result := MergeMaps(nil, nil)
	assert.Assert(t, result == nil)

	result = MergeMaps(nil, map[string]string{"a": "b"})
	assert.Assert(t, result != nil)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["a"], "b")

	result = MergeMaps(map[string]string{"a": "b"}, nil)
	assert.Assert(t, result != nil)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["a"], "b")

	result = MergeMaps(map[string]string{"a": "a1"}, map[string]string{"a": "a2"})
	assert.Assert(t, result != nil)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["a"], "a2")

	result = MergeMaps(map[string]string{
		"a": "a1",
		"b": "b1",
		"c": "c1",
	}, map[string]string{
		"a": "a2",
		"b": "b2",
		"d": "d2",
	})
	assert.Assert(t, result != nil)
	assert.Equal(t, len(result), 4)
	assert.Equal(t, result["a"], "a2")
	assert.Equal(t, result["b"], "b2")
	assert.Equal(t, result["c"], "c1")
	assert.Equal(t, result["d"], "d2")
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
			assert.DeepEqual(t, userID, tc.expectedUser)
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

func TestNeedRecovery(t *testing.T) {
	const fakeNodeID = "fake-node"
	testCases := []struct {
		description          string
		pod                  *v1.Pod
		expectedRecoveryFlag bool
	}{
		{"New pod pending for scheduling",
			&v1.Pod{
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      "",
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				}}, false},
		{"Succeed pod",
			&v1.Pod{
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodSucceeded,
				}}, false},
		{"Failed pod",
			&v1.Pod{
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
				}}, false},
		{"Non YK pod",
			&v1.Pod{
				Spec: v1.PodSpec{
					SchedulerName: "default-scheduler",
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				}}, false},
		{"Assigned pod and Running",
			&v1.Pod{
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
					NodeName:      fakeNodeID,
				},
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				}}, true},
		{"Assigned pod but Pending",
			&v1.Pod{
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
			recovery := NeedRecovery(tc.pod)
			assert.Equal(t, recovery, tc.expectedRecoveryFlag, tc.description)
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

// nolint: funlen
func TestUpdatePodLabelForAdmissionController(t *testing.T) {
	// verify when appId/queue are not given,
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random": "random",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	if result := UpdatePodLabelForAdmissionController(pod, "default"); result != nil {
		assert.Equal(t, len(result), 4)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// verify if applicationId is given in the labels,
	// we won't modify it
	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random":        "random",
				"applicationId": "app-0001",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	if result := UpdatePodLabelForAdmissionController(pod, "default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["applicationId"], "app-0001")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// verify if queue is given in the labels,
	// we won't modify it
	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random": "random",
				"queue":  "root.abc",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	if result := UpdatePodLabelForAdmissionController(pod, "default"); result != nil {
		assert.Equal(t, len(result), 4)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["queue"], "root.abc")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionControllert is not as expected")
	}

	// namespace might be empty
	// labels might be empty
	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	if result := UpdatePodLabelForAdmissionController(pod, "default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// pod name might be empty, it can comes from generatedName
	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "some-pod-",
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	if result := UpdatePodLabelForAdmissionController(pod, "default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       v1.PodSpec{},
		Status:     v1.PodStatus{},
	}
	if result := UpdatePodLabelForAdmissionController(pod, "default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
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
