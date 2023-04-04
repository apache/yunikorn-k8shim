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

package admission

import (
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

func createConfig() *conf.AdmissionControllerConf {
	return conf.NewAdmissionControllerConf([]*v1.ConfigMap{nil, nil})
}

func createConfigWithOverrides(overrides map[string]string) *conf.AdmissionControllerConf {
	return conf.NewAdmissionControllerConf([]*v1.ConfigMap{nil, {Data: overrides}})
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

	if result := updatePodLabel(pod, "default"); result != nil {
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

	if result := updatePodLabel(pod, "default"); result != nil {
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
	if result := updatePodLabel(pod, "default"); result != nil {
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
	if result := updatePodLabel(pod, "default"); result != nil {
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
	if result := updatePodLabel(pod, "default"); result != nil {
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
	if result := updatePodLabel(pod, "default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}
}
