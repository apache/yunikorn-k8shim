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

func createTestingPodWithLabelAppId() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels["applicationId"] = "app-0001"

	return pod
}

func createTestingPodWithAnnotationAppId() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Annotations = map[string]string{
		"yunikorn.apache.org/app-id": "app-0001",
	}

	return pod
}

func createTestingPodWithGenerateName() *v1.Pod {
	pod := createMinimalTestingPod()
	pod.ObjectMeta.GenerateName = "some-pod-"

	return pod
}

func createTestingPodWithLabelQueue() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels["queue"] = "root.abc"

	return pod
}

func createTestingPodWithAnnotationQueue() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Annotations = map[string]string{
		"yunikorn.apache.org/queue": "root.abc",
	}

	return pod
}

func createTestingPodWithLabelEnableStateAware() *v1.Pod {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels = map[string]string{
		constants.LabelDisableStateAware: "false",
	}

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

// nolint: funlen
func TestGetNewApplicationInfo(t *testing.T) {
	// verify when appId/queue are not given, will generate it
	pod := createTestingPodWithMeta()
	newLabels, newAnnotations := getNewApplicationInfo(pod, "default", false, "root.default")

	assert.Equal(t, len(newLabels), 3)
	assert.Equal(t, newLabels["queue"], "root.default")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// verify if applicationId is given in the labels,
	// will copy applicationId to annotation and won't change existing value
	pod = createTestingPodWithLabelAppId()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")

	assert.Equal(t, len(newLabels), 1)
	assert.Equal(t, newLabels["queue"], "root.default")
	assert.Equal(t, newLabels["disableStateAware"], "")
	assert.Equal(t, newLabels["applicationId"], "")
	assert.Equal(t, len(newAnnotations), 2)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/app-id"], "app-0001")

	// verify if queue is given in the labels,
	// will copy queue to annotation and won't change existing value
	pod = createTestingPodWithLabelQueue()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")

	assert.Equal(t, len(newLabels), 2)
	assert.Equal(t, newLabels["queue"], "")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.abc")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// verify if applicationId is given in the annotation,
	// will copy to label and won't change existing value
	pod = createTestingPodWithAnnotationAppId()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")
	assert.Equal(t, len(newLabels), 2)
	assert.Equal(t, newLabels["applicationId"], "app-0001")
	assert.Equal(t, newLabels["queue"], "root.default")
	assert.Equal(t, newLabels["disableStateAware"], "")
	assert.Equal(t, len(newAnnotations), 1)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/app-id"], "")

	// verify if queue is given in the annotation
	// will copy to labels and won't change existing value
	pod = createTestingPodWithAnnotationQueue()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")
	assert.Equal(t, len(newLabels), 3)
	assert.Equal(t, newLabels["queue"], "root.abc")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(newAnnotations), 2)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// verify if state aware label set in pod,
	// won't change state aware setting and will copy to annotation
	pod = createTestingPodWithLabelEnableStateAware()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")

	assert.Equal(t, len(newLabels), 2)
	assert.Equal(t, newLabels["queue"], "root.default")
	assert.Equal(t, newLabels["disableStateAware"], "")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "false")
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// verify empty pod info and default queue name is ""
	// won't add queue name to pod
	pod = createTestingPodWithMeta()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "")

	assert.Equal(t, len(newLabels), 2)
	assert.Equal(t, newLabels["queue"], "")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(newAnnotations), 2)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// pod name might be empty, applicationId can come from generatedName
	pod = createTestingPodWithGenerateName()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	pod = createMinimalTestingPod()
	newLabels, newAnnotations = getNewApplicationInfo(pod, "default", false, "root.default")
	assert.Equal(t, len(newLabels), 3)
	assert.Equal(t, newLabels["queue"], "root.default")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(newLabels["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(newAnnotations["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)
}

func TestDefaultQueueName(t *testing.T) {
	defaultConf := createConfig()
	pod := createTestingPodWithMeta()
	newLabels, newAnnotations := getNewApplicationInfo(pod, defaultConf.GetNamespace(), defaultConf.GetGenerateUniqueAppIds(), defaultConf.GetDefaultQueueName())

	assert.Equal(t, len(newLabels), 3)
	assert.Equal(t, newLabels["applicationId"], "yunikorn-default-autogen")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, newLabels["queue"], "root.default")
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/app-id"], "yunikorn-default-autogen")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.default")

	queueNameEmptyConf := createConfigWithOverrides(map[string]string{
		conf.AMFilteringDefaultQueueName: "",
	})
	newLabels, newAnnotations = getNewApplicationInfo(pod, queueNameEmptyConf.GetNamespace(), queueNameEmptyConf.GetGenerateUniqueAppIds(), queueNameEmptyConf.GetDefaultQueueName())
	assert.Equal(t, len(newLabels), 2)
	assert.Equal(t, newLabels["applicationId"], "yunikorn-default-autogen")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, newLabels["queue"], "")
	assert.Equal(t, len(newAnnotations), 2)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/app-id"], "yunikorn-default-autogen")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "")

	customQueueNameConf := createConfigWithOverrides(map[string]string{
		conf.AMFilteringDefaultQueueName: "yunikorn",
	})
	newLabels, newAnnotations = getNewApplicationInfo(pod, customQueueNameConf.GetNamespace(), customQueueNameConf.GetGenerateUniqueAppIds(), customQueueNameConf.GetDefaultQueueName())
	assert.Equal(t, len(newLabels), 3)
	assert.Equal(t, newLabels["applicationId"], "yunikorn-default-autogen")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Assert(t, newLabels["queue"] != "yunikorn")
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/app-id"], "yunikorn-default-autogen")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Assert(t, newAnnotations["yunikorn.apache.org/queue"] != "yunikorn")

	customValidQueueNameConf := createConfigWithOverrides(map[string]string{
		conf.AMFilteringDefaultQueueName: "root.yunikorn",
	})
	newLabels, newAnnotations = getNewApplicationInfo(pod, customValidQueueNameConf.GetNamespace(),
		customValidQueueNameConf.GetGenerateUniqueAppIds(), customValidQueueNameConf.GetDefaultQueueName())
	assert.Equal(t, len(newLabels), 3)
	assert.Equal(t, newLabels["applicationId"], "yunikorn-default-autogen")
	assert.Equal(t, newLabels["disableStateAware"], "true")
	assert.Equal(t, newLabels["queue"], "root.yunikorn")
	assert.Equal(t, len(newAnnotations), 3)
	assert.Equal(t, newAnnotations["yunikorn.apache.org/app-id"], "yunikorn-default-autogen")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, newAnnotations["yunikorn.apache.org/queue"], "root.yunikorn")
}

func TestUpdateLabelIfNotPresentInPod(t *testing.T) {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Labels = map[string]string{
		"exist_key": "exist_value",
	}

	// Verify updating label that exist in pod
	result := make(map[string]string)
	result = updateLabelIfNotPresentInPod(pod, result, "exist_key", "new_value")
	assert.Equal(t, len(result), 0)
	assert.Equal(t, result["exist_key"], "")

	// Verify updating label that not exists in pod
	result = make(map[string]string)
	result = updateLabelIfNotPresentInPod(pod, result, "new_key", "new_value")
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["new_key"], "new_value")

	// Verify if no label in pod
	pod.ObjectMeta.Labels = nil
	result = make(map[string]string)
	result = updateLabelIfNotPresentInPod(pod, result, "new_key", "new_value")
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["new_key"], "new_value")
}

func TestUpdateAnnotationIfNotPresentInPod(t *testing.T) {
	pod := createTestingPodWithMeta()
	pod.ObjectMeta.Annotations = map[string]string{
		"exist_key": "exist_value",
	}

	// Verify updating annotation that exist in pod
	result := make(map[string]string)
	result = updateAnnotationIfNotPresentInPod(pod, result, "exist_key", "new_value")
	assert.Equal(t, len(result), 0)
	assert.Equal(t, result["exist_key"], "")

	// Verify updating annotation that not exists in pod
	result = make(map[string]string)
	result = updateAnnotationIfNotPresentInPod(pod, result, "new_key", "new_value")
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["new_key"], "new_value")

	// Verify if no annotation in pod
	pod.ObjectMeta.Labels = nil
	result = make(map[string]string)
	result = updateAnnotationIfNotPresentInPod(pod, result, "new_key", "new_value")
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result["new_key"], "new_value")
}

func TestGetApplicationIDFromPod(t *testing.T) {
	appName := []string{"app00001", "app00002"}

	// test empty pod
	pod := &v1.Pod{}
	appId, isFromLabel := getApplicationIDFromPod(pod)
	assert.Equal(t, appId, "")
	assert.Equal(t, isFromLabel, false)

	// test annotation take precedence over label
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				constants.LabelApplicationID: appName[0],
			},
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appName[1],
			},
		},
	}
	appId, isFromLabel = getApplicationIDFromPod(pod)
	assert.Equal(t, appId, appName[1])
	assert.Equal(t, isFromLabel, false)

	// test pod with label only
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				constants.LabelApplicationID: appName[0],
			},
		},
	}
	appId, isFromLabel = getApplicationIDFromPod(pod)
	assert.Equal(t, appId, appName[0])
	assert.Equal(t, isFromLabel, true)
}

func TestGetDisableStateAwareFromPod(t *testing.T) {
	// test empty pod
	pod := &v1.Pod{}
	disableStateAware, isFromLabel := getDisableStateAwareFromPod(pod)
	assert.Equal(t, disableStateAware, "")
	assert.Equal(t, isFromLabel, false)

	// test annotation take precedence over label
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				constants.LabelDisableStateAware: "true",
			},
			Annotations: map[string]string{
				constants.AnnotationDisableStateAware: "false",
			},
		},
	}
	disableStateAware, isFromLabel = getDisableStateAwareFromPod(pod)
	assert.Equal(t, disableStateAware, "false")
	assert.Equal(t, isFromLabel, false)

	// test pod with label only
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				constants.LabelDisableStateAware: "true",
			},
		},
	}
	disableStateAware, isFromLabel = getDisableStateAwareFromPod(pod)
	assert.Equal(t, disableStateAware, "true")
	assert.Equal(t, isFromLabel, true)
}
