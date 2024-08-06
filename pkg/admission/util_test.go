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
	"reflect"
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

func createMinimalTestingPod() *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       v1.PodSpec{},
		Status:     v1.PodStatus{},
	}
}

func createTestingPodWithMeta() *v1.Pod {
	pod := createMinimalTestingPod()

	pod.ObjectMeta =
		metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random": "random",
			},
			Annotations: map[string]string{},
		}

	return pod
}

func createTestingPodWithLabels(appId string, queue string) *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels[constants.CanonicalLabelApplicationID] = appId
	pod.ObjectMeta.Labels[constants.CanonicalLabelQueueName] = queue

	return pod
}

func createTestingPodWithGenerateName() *v1.Pod {
	pod := createMinimalTestingPod()
	pod.ObjectMeta.GenerateName = "some-pod-"

	return pod
}

func createTestingPodWithAnnotations(appId string, queue string) *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Annotations[constants.AnnotationApplicationID] = appId
	pod.ObjectMeta.Annotations[constants.AnnotationQueueName] = queue

	return pod
}

func createTestingPodNoNamespaceAndLabels() *v1.Pod {
	pod := createMinimalTestingPod()
	pod.ObjectMeta =
		metav1.ObjectMeta{
			Name:            "a-test-pod",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
		}
	return pod
}

func TestUpdatePodLabelForAdmissionController(t *testing.T) {
	dummyAppId := "app-0001"
	dummyQueueName := "root.abc"

	// verify when appId/queue are not given,
	// we generate new appId/queue labels
	pod := createTestingPodWithMeta()
	if result := updatePodLabel(pod, "default", false); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, strings.HasPrefix(result[constants.CanonicalLabelApplicationID], constants.AutoGenAppPrefix), true)
		assert.Equal(t, strings.HasPrefix(result[constants.LabelApplicationID], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// verify if appId/queue is given in the canonical labels
	// we won't modify the value and will add it to non-canonical label for backward compatibility
	pod = createTestingPodWithLabels(dummyAppId, dummyQueueName)
	if result := updatePodLabel(pod, "default", false); result != nil {
		assert.Equal(t, len(result), 5)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result[constants.CanonicalLabelApplicationID], dummyAppId)
		assert.Equal(t, result[constants.LabelApplicationID], dummyAppId)
		assert.Equal(t, result[constants.CanonicalLabelQueueName], dummyQueueName)
		assert.Equal(t, result[constants.LabelQueueName], dummyQueueName)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// verify if applicationId and queue is given in the annotations,
	// we won't generate new labels
	pod = createTestingPodWithAnnotations(dummyAppId, dummyQueueName)
	if result := updatePodLabel(pod, "default", false); result != nil {
		t.Log(result)
		assert.Equal(t, len(result), 1)
		assert.Equal(t, result["random"], "random")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// namespace might be empty
	// labels might be empty
	pod = createTestingPodNoNamespaceAndLabels()

	if result := updatePodLabel(pod, "default", false); result != nil {
		assert.Equal(t, len(result), 2)
		assert.Equal(t, strings.HasPrefix(result[constants.CanonicalLabelApplicationID], constants.AutoGenAppPrefix), true)
		assert.Equal(t, strings.HasPrefix(result[constants.LabelApplicationID], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// pod name might be empty, it can comes from generatedName
	pod = createTestingPodWithGenerateName()
	if result := updatePodLabel(pod, "default", false); result != nil {
		assert.Equal(t, len(result), 2)
		assert.Equal(t, strings.HasPrefix(result[constants.CanonicalLabelApplicationID], constants.AutoGenAppPrefix), true)
		assert.Equal(t, strings.HasPrefix(result[constants.LabelApplicationID], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	pod = createMinimalTestingPod()
	if result := updatePodLabel(pod, "default", false); result != nil {
		assert.Equal(t, len(result), 2)
		assert.Equal(t, strings.HasPrefix(result[constants.CanonicalLabelApplicationID], constants.AutoGenAppPrefix), true)
		assert.Equal(t, strings.HasPrefix(result[constants.LabelApplicationID], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}
}

func TestDefaultQueueName(t *testing.T) {
	defaultConf := createConfig()
	pod := createTestingPodWithMeta()
	if result := updatePodLabel(pod, defaultConf.GetNamespace(), defaultConf.GetGenerateUniqueAppIds()); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result[constants.CanonicalLabelApplicationID], "yunikorn-default-autogen")
		assert.Equal(t, result[constants.LabelApplicationID], "yunikorn-default-autogen")
		assert.Equal(t, result[constants.CanonicalLabelQueueName], "")
		assert.Equal(t, result[constants.LabelQueueName], "")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}
}

func TestConvert2Namespace(t *testing.T) {
	namespace := &v1.Namespace{}
	result := convert2Namespace(namespace)
	assert.Check(t, result != nil)
	assert.Check(t, reflect.DeepEqual(result, namespace))

	obj := struct{}{}
	result = convert2Namespace(obj)
	assert.Check(t, result == nil)

	pod := &v1.Pod{}
	result = convert2Namespace(pod)
	assert.Check(t, result == nil)
}
