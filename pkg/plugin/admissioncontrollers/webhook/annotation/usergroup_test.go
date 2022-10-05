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

package annotation

import (
	"encoding/json"
	"regexp"
	"testing"

	"gotest.tools/assert"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1Beta "k8s.io/api/batch/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
)

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
	ah := getAnnotationHandler()
	extUsersRegexps := make([]*regexp.Regexp, 1)
	extUsersRegexps[0] = regexp.MustCompile("yunikorn")
	ah.ExternalUsers = extUsersRegexps

	allowed := ah.IsAnnotationAllowed("yunikorn", groups)
	assert.Assert(t, allowed)
}

func TestExternalGroups(t *testing.T) {
	ah := getAnnotationHandler()
	extGroupsRegexps := make([]*regexp.Regexp, 1)
	extGroupsRegexps[0] = regexp.MustCompile("devs")
	ah.ExternalGroups = extGroupsRegexps

	allowed := ah.IsAnnotationAllowed(userName, groups)
	assert.Assert(t, allowed)
}

func TestExternalAuthenticationDenied(t *testing.T) {
	ah := getAnnotationHandler()
	allowed := ah.IsAnnotationAllowed("yunikorn", groups)
	assert.Assert(t, !allowed)
}

func TestGetAnnotationFromRequest(t *testing.T) {
	type TestCase struct {
		obj  interface{}
		kind string
	}

	tests := []TestCase{
		{
			obj: appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
						},
					},
				},
			},
			kind: ReplicaSet,
		},
		{
			obj: &appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
						},
					},
				},
			},
			kind: StatefulSet,
		},
		{
			obj: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
						},
					},
				},
			},
			kind: Deployment,
		},
		{
			obj: &appsv1.DaemonSet{
				Spec: appsv1.DaemonSetSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
						},
					},
				},
			},
			kind: DaemonSet,
		},
		{
			obj: &batchv1.Job{
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: annotation,
						},
					},
				},
			},
			kind: Job,
		},
		{
			obj: &batchv1Beta.CronJob{
				Spec: batchv1Beta.CronJobSpec{
					JobTemplate: batchv1Beta.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: v1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Annotations: annotation,
								},
							},
						},
					},
				},
			},
			kind: CronJob,
		},
	}
	ah := getAnnotationHandler()

	for _, testCase := range tests {
		t.Run("TestGetAnnotationFromRequest#"+testCase.kind, func(t *testing.T) {
			req := getAdmissionRequest(t, testCase.obj)
			annotations, supported, err := ah.GetAnnotationsFromRequestKind(testCase.kind, req)
			assert.Assert(t, supported)
			assert.NilError(t, err)
			assert.Assert(t, annotations != nil)
			assert.Equal(t, annotations["key"], "yunikorn")
		})
	}
}

func TestGetAnnotationFromRequestFails(t *testing.T) {
	tests := []string{Deployment, DaemonSet, StatefulSet, ReplicaSet, Job, CronJob}

	ah := getAnnotationHandler()
	for _, testCase := range tests {
		t.Run("TestGetAnnotationFromRequestFails#"+testCase, func(t *testing.T) {
			req := getAdmissionRequest(t, nil)
			req.Object.Raw = []byte{0, 1, 2, 3, 4}
			annotations, supported, err := ah.GetAnnotationsFromRequestKind(testCase, req)
			assert.Assert(t, supported)
			assert.ErrorContains(t, err, "invalid character")
			assert.Assert(t, annotations == nil)
		})
	}
}

func TestGetAnnotationFromUnknownObject(t *testing.T) {
	ah := getAnnotationHandler()
	annotations, supported, err := ah.GetAnnotationsFromRequestKind("Unknown", nil)
	assert.Check(t, annotations == nil)
	assert.Check(t, !supported)
	assert.NilError(t, err)
}

func TestGetAnnotationFromInvalidObject(t *testing.T) {
	req := getAdmissionRequest(t, nil)
	req.Object.Raw = []byte{0, 1, 2, 3, 4}
	ah := getAnnotationHandler()
	annotations, supported, err := ah.GetAnnotationsFromRequestKind("Deployment", req)
	assert.Check(t, annotations == nil)
	assert.Check(t, supported)
	assert.ErrorContains(t, err, "invalid character")
}

func getAdmissionRequest(t *testing.T, obj interface{}) *admissionv1.AdmissionRequest {
	raw, err := json.Marshal(obj)
	assert.NilError(t, err, "Could not marshal object")
	req := &admissionv1.AdmissionRequest{
		Object: runtime.RawExtension{
			Raw: raw,
		},
	}

	return req
}

func getAnnotationHandler() *UserGroupAnnotationHandler {
	sysUsersRegexps := make([]*regexp.Regexp, 1)
	sysUsersRegexps[0] = regexp.MustCompile("system:serviceaccount:kube-system:*")
	extUsersRegexps := make([]*regexp.Regexp, 0)
	extGroupsRegexps := make([]*regexp.Regexp, 0)

	ugh := UserGroupAnnotationHandler{
		BypassControllers: true,
		SystemUsers:       sysUsersRegexps,
		ExternalGroups:    extUsersRegexps,
		ExternalUsers:     extGroupsRegexps,
	}

	return &ugh
}
