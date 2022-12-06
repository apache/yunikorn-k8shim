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

package metadata

import (
	"encoding/json"
	"testing"

	"gotest.tools/assert"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1Beta "k8s.io/api/batch/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"

	"github.com/apache/yunikorn-k8shim/pkg/admission/common"
	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
)

const (
	userName       = "test"
	testAnnotation = "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}"
)

var (
	groups     = []string{"system:authenticated", "devs"}
	annotation = map[string]string{
		"key": "yunikorn",
	}
	label = map[string]string{
		"key": "yunikorn",
	}
)

type TestCase struct {
	obj  interface{}
	kind string
	path string
}

func TestValidateAnnotation(t *testing.T) {
	ah := getAnnotationHandler()

	err := ah.IsAnnotationValid("xyzxyz")
	assert.ErrorContains(t, err, "invalid character 'x'")

	err = ah.IsAnnotationValid(testAnnotation)
	assert.NilError(t, err)
}

func TestBypassControllers(t *testing.T) {
	ah := getAnnotationHandler()
	allowed := ah.IsAnnotationAllowed("system:serviceaccount:kube-system:job-controller", groups)
	assert.Assert(t, allowed)
}

func TestExternalUsers(t *testing.T) {
	ah := getAnnotationHandlerWithOverrides(map[string]string{
		conf.AMAccessControlExternalUsers: "^yunikorn$",
	})
	allowed := ah.IsAnnotationAllowed("yunikorn", groups)
	assert.Assert(t, allowed)
}

func TestExternalGroups(t *testing.T) {
	ah := getAnnotationHandlerWithOverrides(map[string]string{
		conf.AMAccessControlExternalGroups: "^devs$",
	})
	allowed := ah.IsAnnotationAllowed(userName, groups)
	assert.Assert(t, allowed)
}

func TestExternalAuthenticationDenied(t *testing.T) {
	ah := getAnnotationHandler()
	allowed := ah.IsAnnotationAllowed("yunikorn", groups)
	assert.Assert(t, !allowed)
}

func TestGetAnnotationFromRequest(t *testing.T) {
	tests := getTestCases()
	ah := getAnnotationHandler()

	for _, testCase := range tests {
		t.Run("TestGetAnnotationFromRequest#"+testCase.kind, func(t *testing.T) {
			req := getAdmissionRequest(t, testCase.obj, testCase.kind)
			annotations, supported, err := ah.GetAnnotationsFromRequestKind(req)
			assert.Assert(t, supported)
			assert.NilError(t, err)
			assert.Assert(t, annotations != nil)
			assert.Equal(t, annotations["key"], "yunikorn")
		})
	}
}

func TestGetAnnotationFromRequestFails(t *testing.T) {
	tests := getTestCases()
	ah := getAnnotationHandler()

	for _, testCase := range tests {
		t.Run("TestGetAnnotationFromRequestFails#"+testCase.kind, func(t *testing.T) {
			req := getAdmissionRequest(t, nil, testCase.kind)
			req.Object.Raw = []byte{0, 1, 2, 3, 4}
			annotations, supported, err := ah.GetAnnotationsFromRequestKind(req)
			assert.Assert(t, supported)
			assert.ErrorContains(t, err, "invalid character")
			assert.Assert(t, annotations == nil)
		})
	}
}

func TestGetAnnotationFromUnknownObject(t *testing.T) {
	ah := getAnnotationHandler()
	req := &admissionv1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "Unknown",
		},
	}
	annotations, supported, err := ah.GetAnnotationsFromRequestKind(req)
	assert.Check(t, annotations == nil)
	assert.Check(t, !supported)
	assert.NilError(t, err)
}

func TestGetAnnotationFromInvalidObject(t *testing.T) {
	req := getAdmissionRequest(t, nil, "Deployment")
	req.Object.Raw = []byte{0, 1, 2, 3, 4}
	ah := getAnnotationHandler()
	annotations, supported, err := ah.GetAnnotationsFromRequestKind(req)
	assert.Check(t, annotations == nil)
	assert.Check(t, supported)
	assert.ErrorContains(t, err, "invalid character")
}

func TestGetPatchForWorkload(t *testing.T) {
	tests := getTestCases()
	ah := getAnnotationHandler()

	for _, testCase := range tests {
		t.Run("TestGetPatchForWorkload#"+testCase.kind, func(t *testing.T) {
			req := getAdmissionRequest(t, testCase.obj, testCase.kind)
			patch, err := ah.GetPatchForWorkload(req, "yunikorn", []string{"users", "dev"})
			assert.NilError(t, err)
			assert.Equal(t, 1, len(patch))
			patchOp := patch[0]
			assert.Equal(t, patchOp.Op, "add")
			assert.Equal(t, patchOp.Path, testCase.path)
			verifyUserGroupAnnotation(t, patchOp.Value)
		})
	}
}

func TestGetPatchForPod(t *testing.T) {
	ah := getAnnotationHandler()
	patchOp, err := ah.GetPatchForPod(annotation, "yunikorn", []string{"users", "dev"})
	assert.NilError(t, err)
	assert.Assert(t, patchOp != nil)
	assert.Equal(t, patchOp.Op, "add")
	assert.Equal(t, patchOp.Path, "/metadata/annotations")
	verifyUserGroupAnnotation(t, patchOp.Value)
}

func verifyUserGroupAnnotation(t *testing.T, patchValue interface{}) {
	value, ok := patchValue.(map[string]string)
	assert.Assert(t, ok, "type assertion failed")
	assert.Equal(t, len(value), 2)
	assert.Assert(t, value[common.UserInfoAnnotation] != "")
	assert.Equal(t, value["key"], "yunikorn")
	jsonValue := value[common.UserInfoAnnotation]
	var userGroup si.UserGroupInformation
	err := json.Unmarshal([]byte(jsonValue), &userGroup)
	assert.NilError(t, err)
	assert.Equal(t, userGroup.User, "yunikorn")
	assert.Equal(t, len(userGroup.Groups), 2)
	assert.Equal(t, userGroup.Groups[0], "users")
	assert.Equal(t, userGroup.Groups[1], "dev")
}

func getTestCases() []TestCase {
	tests := []TestCase{
		{
			obj: appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
							Labels:      label,
						},
					},
				},
			},
			kind: ReplicaSet,
			path: defaultPodAnnotationsPath,
		},
		{
			obj: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
							Labels:      label,
						},
					},
				},
			},
			kind: StatefulSet,
			path: defaultPodAnnotationsPath,
		},
		{
			obj: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
							Labels:      label,
						},
					},
				},
			},
			kind: Deployment,
			path: defaultPodAnnotationsPath,
		},
		{
			obj: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
							Labels:      label,
						},
					},
				},
			},
			kind: DaemonSet,
			path: defaultPodAnnotationsPath,
		},
		{
			obj: &batchv1.Job{
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
							Labels:      label,
						},
					},
				},
			},
			kind: Job,
			path: defaultPodAnnotationsPath,
		},
		{
			obj: &batchv1Beta.CronJob{
				Spec: batchv1Beta.CronJobSpec{
					JobTemplate: batchv1Beta.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Annotations: annotation,
									Labels:      label,
								},
							},
						},
					},
				},
			},
			kind: CronJob,
			path: cronJobPodAnnotationsPath,
		},
	}
	return tests
}

func getAdmissionRequest(t *testing.T, obj interface{}, kind string) *admissionv1.AdmissionRequest {
	raw, err := json.Marshal(obj)
	assert.NilError(t, err, "Could not marshal object")
	req := &admissionv1.AdmissionRequest{
		Object: runtime.RawExtension{
			Raw: raw,
		},
		Kind: metav1.GroupVersionKind{
			Kind: kind,
		},
	}

	return req
}

func getAnnotationHandler() *UserGroupAnnotationHandler {
	return getAnnotationHandlerWithOverrides(map[string]string{})
}

func getAnnotationHandlerWithOverrides(overrides map[string]string) *UserGroupAnnotationHandler {
	return &UserGroupAnnotationHandler{
		conf: conf.NewAdmissionControllerConf([]*v1.ConfigMap{{
			Data: map[string]string{
				conf.AMAccessControlSystemUsers:    "^system:serviceaccount:kube-system:",
				conf.AMAccessControlExternalUsers:  "",
				conf.AMAccessControlExternalGroups: "",
			},
		}, {
			Data: overrides,
		}}),
	}
}
