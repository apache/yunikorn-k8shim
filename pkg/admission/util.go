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
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

func UpdatePodLabelForAdmissionController(pod *v1.Pod, namespace string) map[string]string {
	existingLabels := pod.Labels
	result := make(map[string]string)
	for k, v := range existingLabels {
		result[k] = v
	}

	sparkAppID := utils.GetPodLabelValue(pod, constants.SparkLabelAppID)
	appID := utils.GetPodLabelValue(pod, constants.LabelApplicationID)
	if sparkAppID == "" && appID == "" {
		// if app id not exist, generate one
		// for each namespace, we group unnamed pods to one single app
		// application ID convention: ${AUTO_GEN_PREFIX}-${NAMESPACE}-${AUTO_GEN_SUFFIX}
		generatedID := generateAppID(namespace)
		result[constants.LabelApplicationID] = generatedID

		// if we generate an app ID, disable state-aware scheduling for this app
		if _, ok := existingLabels[constants.LabelDisableStateAware]; !ok {
			result[constants.LabelDisableStateAware] = "true"
		}
	}

	if _, ok := existingLabels[constants.LabelQueueName]; !ok {
		result[constants.LabelQueueName] = constants.DefaultQueue
	}

	return result
}

func UpdatePodAnnotationForAdmissionController(pod *v1.Pod, key string, value string) map[string]string {
	existingAnnotations := pod.Annotations
	result := make(map[string]string)
	for k, v := range existingAnnotations {
		result[k] = v
	}
	result[key] = value
	return result
}
