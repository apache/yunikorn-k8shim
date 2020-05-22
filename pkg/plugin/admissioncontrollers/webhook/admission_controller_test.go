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
	"fmt"
	"strings"
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
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

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
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
				"random":        "random",
				"applicationId": "app-0001",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["applicationId"], "app-0001")
	} else {
		t.Fatal("patch info content is not as expected")
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
				"queue":  "root.abc",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.abc")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
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

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
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

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
	}

	// pod name and generate name could be both empty
	patch = make([]patchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       v1.PodSpec{},
		Status:     v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
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
		t.Fatal("patch info content is not as expected")
	}
}

func TestValidateConfigMap(t *testing.T) {
	configName := fmt.Sprintf("%s.yaml", conf.DefaultPolicyGroup)
	controller := &admissionController{
		configName: configName,
	}
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: common.DefaultConfigMapName,
		},
		Data: make(map[string]string),
	}
	// specified config "queues.yaml" not found
	err := controller.validateConfigMap(configmap)
	assert.Assert(t, err != nil, "expecting error when specified config is not found")
	assert.Equal(t, err.Error(), "required config 'queues.yaml' not found in this configmap")
	// skip further validations which depends on the webservice of yunikorn-core
}

func TestGenerateAppID(t *testing.T) {
	appID := generateAppID("")
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-default", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 24)

	appID = generateAppID("this-is-a-namespace")
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-this-is-a-namespace", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 36)

	appID = generateAppID("short")
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-short", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 22)

	appID = generateAppID(strings.Repeat("long", 100))
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-long", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 63)
}
