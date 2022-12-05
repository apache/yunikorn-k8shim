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
	"testing"

	"gotest.tools/v3/assert"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetLabelFromWorkload(t *testing.T) {
	tests := getTestCases()
	le := &LabelExtractor{}

	for _, testCase := range tests {
		t.Run("TestGetLabelsFromWorkload#"+testCase.kind, func(t *testing.T) {
			req := getAdmissionRequest(t, testCase.obj, testCase.kind)
			labels, supported, err := le.GetLabelsFromWorkload(req)
			assert.Assert(t, supported)
			assert.NilError(t, err)
			assert.Assert(t, labels != nil)
			assert.Equal(t, labels["key"], "yunikorn")
		})
	}
}

func TestGetLabelFromWorkloadFails(t *testing.T) {
	tests := getTestCases()
	le := &LabelExtractor{}

	for _, testCase := range tests {
		t.Run("TestGetLabelFromWorkloadFails#"+testCase.kind, func(t *testing.T) {
			req := getAdmissionRequest(t, nil, testCase.kind)
			req.Object.Raw = []byte{0, 1, 2, 3, 4}
			labels, supported, err := le.GetLabelsFromWorkload(req)
			assert.Assert(t, supported)
			assert.ErrorContains(t, err, "invalid character")
			assert.Assert(t, labels == nil)
		})
	}
}

func TestGetLabelFromUnknownObject(t *testing.T) {
	le := &LabelExtractor{}
	req := &admissionv1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Kind: "Unknown",
		},
	}
	labels, supported, err := le.GetLabelsFromWorkload(req)
	assert.Check(t, labels == nil)
	assert.Check(t, !supported)
	assert.NilError(t, err)
}

func TestGetLabelFromInvalidObject(t *testing.T) {
	req := getAdmissionRequest(t, nil, "Deployment")
	req.Object.Raw = []byte{0, 1, 2, 3, 4}
	le := &LabelExtractor{}
	labels, supported, err := le.GetLabelsFromWorkload(req)
	assert.Check(t, labels == nil)
	assert.Check(t, supported)
	assert.ErrorContains(t, err, "invalid character")
}
