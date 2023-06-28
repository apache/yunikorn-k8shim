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
	"fmt"
	"reflect"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

func updatePodLabel(pod *v1.Pod, namespace string, generateUniqueAppIds bool, defaultQueueName string) map[string]string {
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
		generatedID := generateAppID(namespace, pod, generateUniqueAppIds)
		result[constants.LabelApplicationID] = generatedID

		// if we generate an app ID, disable state-aware scheduling for this app
		if _, ok := existingLabels[constants.LabelDisableStateAware]; !ok {
			result[constants.LabelDisableStateAware] = "true"
		}
	}

	// if existing label exist, it takes priority over everything else
	if _, ok := existingLabels[constants.LabelQueueName]; !ok {
		// if defaultQueueName is "", skip adding default queue name to the pod labels
		if defaultQueueName != "" {
			// for undefined configuration, am_conf will add 'root.default' to retain existing behavior
			// if a custom name is configured for default queue, it will be used instead of root.default
			result[constants.LabelQueueName] = defaultQueueName
		}
	}

	return result
}

func updatePodAnnotation(pod *v1.Pod, key string, value string) map[string]string {
	existingAnnotations := pod.Annotations
	result := make(map[string]string)
	for k, v := range existingAnnotations {
		result[k] = v
	}
	result[key] = value
	return result
}

func convert2Namespace(obj interface{}) *v1.Namespace {
	if nameSpace, ok := obj.(*v1.Namespace); ok {
		return nameSpace
	}
	log.Log(log.AdmissionUtils).Warn("cannot convert to *v1.Namespace", zap.Stringer("type", reflect.TypeOf(obj)))
	return nil
}

// generate appID based on the namespace value
// if configured to generate unique appID, generate appID as <namespace>-<pod-uid> namespace capped at 26chars
// if not set or configured as false, appID generated as <autogen-prefix>-<namespace>-<autogen-suffix>
func generateAppID(namespace string, pod *v1.Pod, generateUniqueAppIds bool) string {
	podUid := string(pod.UID)

	var generatedID string
	if generateUniqueAppIds {
		generatedID = fmt.Sprintf("%.26s-%s", namespace, string(podUid))
	} else {
		generatedID = fmt.Sprintf("%s-%s-%s", constants.AutoGenAppPrefix, namespace, constants.AutoGenAppSuffix)
	}
	appID := fmt.Sprintf("%.63s", generatedID)
	return appID
}
