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
		}

	return pod
}

func createTestingPodWithAppId() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels["applicationId"] = "app-0001"

	return pod
}

func createTestingPodWithGenerateName() *v1.Pod {
	pod := createMinimalTestingPod()
	pod.ObjectMeta.GenerateName = "some-pod-"

	return pod
}

func createTestingPodWithQueue() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels["queue"] = "root.abc"

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
	// verify when appId/queue are not given,
	pod := createTestingPodWithMeta()

	if result := updatePodLabel(pod, "default", false, "root.default"); result != nil {
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
	pod = createTestingPodWithAppId()

	if result := updatePodLabel(pod, "default", false, "root.default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["applicationId"], "app-0001")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// verify if queue is given in the labels,
	// we won't modify it
	pod = createTestingPodWithQueue()
	if result := updatePodLabel(pod, "default", false, "root.default"); result != nil {
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
	pod = createTestingPodNoNamespaceAndLabels()

	if result := updatePodLabel(pod, "default", false, "root.default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	// pod name might be empty, it can comes from generatedName
	pod = createTestingPodWithGenerateName()
	if result := updatePodLabel(pod, "default", false, "root.default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	pod = createMinimalTestingPod()
	if result := updatePodLabel(pod, "default", false, "root.default"); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["queue"], "root.default")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, strings.HasPrefix(result["applicationId"], constants.AutoGenAppPrefix), true)
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}
}

func TestDefaultQueueName(t *testing.T) {
	defaultConf := createConfig()
	pod := createTestingPodWithMeta()
	if result := updatePodLabel(pod, defaultConf.GetNamespace(), defaultConf.GetGenerateUniqueAppIds(), defaultConf.GetDefaultQueueName()); result != nil {
		assert.Equal(t, len(result), 4)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["applicationId"], "yunikorn-default-autogen")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, result["queue"], "root.default")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	queueNameEmptyConf := createConfigWithOverrides(map[string]string{
		conf.AMFilteringDefaultQueueName: "",
	})
	if result := updatePodLabel(pod, queueNameEmptyConf.GetNamespace(), queueNameEmptyConf.GetGenerateUniqueAppIds(), queueNameEmptyConf.GetDefaultQueueName()); result != nil {
		assert.Equal(t, len(result), 3)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["applicationId"], "yunikorn-default-autogen")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, result["queue"], "")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	customQueueNameConf := createConfigWithOverrides(map[string]string{
		conf.AMFilteringDefaultQueueName: "yunikorn",
	})
	if result := updatePodLabel(pod, customQueueNameConf.GetNamespace(), customQueueNameConf.GetGenerateUniqueAppIds(), customQueueNameConf.GetDefaultQueueName()); result != nil {
		assert.Equal(t, len(result), 4)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["applicationId"], "yunikorn-default-autogen")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Assert(t, result["queue"] != "yunikorn")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}

	customValidQueueNameConf := createConfigWithOverrides(map[string]string{
		conf.AMFilteringDefaultQueueName: "root.yunikorn",
	})
	if result := updatePodLabel(pod, customValidQueueNameConf.GetNamespace(),
		customValidQueueNameConf.GetGenerateUniqueAppIds(), customValidQueueNameConf.GetDefaultQueueName()); result != nil {
		assert.Equal(t, len(result), 4)
		assert.Equal(t, result["random"], "random")
		assert.Equal(t, result["applicationId"], "yunikorn-default-autogen")
		assert.Equal(t, result["disableStateAware"], "true")
		assert.Equal(t, result["queue"], "root.yunikorn")
	} else {
		t.Fatal("UpdatePodLabelForAdmissionController is not as expected")
	}
}
