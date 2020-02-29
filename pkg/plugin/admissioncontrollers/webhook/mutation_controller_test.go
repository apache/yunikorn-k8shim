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

package main

import (
	"strings"
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUpdateLabels(t *testing.T) {
	// verify when appId/queue are not given,
	// we patch it correctly
	var patch []patchOperation

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

	patch = updateLabels(pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], "default_a-test-pod"), true)
	} else {
		t.Fatal("patch info is not expected")
	}

	// verify if applicationId is given in the labels,
	// we won't modify it
	patch = make([]patchOperation, 0)

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
				"applicationId": "app-0001",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels(pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["applicationId"], "app-0001")
	} else {
		t.Fatal("patch info is not expected")
	}

	// verify if queue is given in the labels,
	// we won't modify it
	patch = make([]patchOperation, 0)

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
				"queue": "root.abc",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels(pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.abc")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], "default_a-test-pod"), true)
	} else {
		t.Fatal("patch info is not expected")
	}

	// namespace might be empty
	// labels might be empty
	patch = make([]patchOperation, 0)

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

	patch = updateLabels(pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], "default_a-test-pod"), true)
	} else {
		t.Fatal("patch info is not expected")
	}

	// pod name might be empty, it can comes from generatedName
	patch = make([]patchOperation, 0)

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

	patch = updateLabels(pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], "default_some-pod-"), true)
	} else {
		t.Fatal("patch info is not expected")
	}
}

func TestUpdateSchedulerName(t *testing.T) {
	var patch []patchOperation
	patch = updateSchedulerName(patch)
	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/spec/schedulerName")
	if name, ok := patch[0].Value.(string); ok {
		assert.Equal(t, name, "yunikorn")
	} else {
		t.Fatal("patch info is not expected")
	}
}

