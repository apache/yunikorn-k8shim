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

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

func updatePodLabel(pod *v1.Pod, namespace string, generateUniqueAppIds bool) map[string]string {
	result := make(map[string]string)
	for k, v := range pod.Labels {
		result[k] = v
	}

	canonicalAppID := utils.GetPodLabelValue(pod, constants.CanonicalLabelApplicationID)
	sparkAppID := utils.GetPodLabelValue(pod, constants.SparkLabelAppID)
	labelAppID := utils.GetPodLabelValue(pod, constants.LabelApplicationID)
	annotationAppID := utils.GetPodAnnotationValue(pod, constants.AnnotationApplicationID)
	if canonicalAppID == "" && sparkAppID == "" && labelAppID == "" && annotationAppID == "" {
		// if app id not exist, generate one
		// for each namespace, we group unnamed pods to one single app - if GenerateUniqueAppId is not set
		// if GenerateUniqueAppId:
		//		application ID convention: ${NAMESPACE}-${POD_UID}
		// else
		// 		application ID convention: ${AUTO_GEN_PREFIX}-${NAMESPACE}-${AUTO_GEN_SUFFIX}
		generatedID := utils.GenerateApplicationID(namespace, generateUniqueAppIds, string(pod.UID))

		result[constants.CanonicalLabelApplicationID] = generatedID
		// Deprecated: After 1.7.0, admission controller will only add canonical label if application ID was not set
		result[constants.LabelApplicationID] = generatedID
	} else if canonicalAppID != "" {
		// Deprecated: Added in 1.6.0 for backward compatibility, in case the prior shim version can't handle canonical label
		result[constants.LabelApplicationID] = canonicalAppID
	}

	canonicalQueueName := utils.GetPodLabelValue(pod, constants.CanonicalLabelQueueName)
	if canonicalQueueName != "" {
		// Deprecated: Added in 1.6.0 for backward compatibility, in case the prior shim version can't handle canonical label
		result[constants.LabelQueueName] = canonicalQueueName
	}

	return result
}

func updatePodAnnotation(pod *v1.Pod, key string, value string) map[string]string {
	result := make(map[string]string)
	for k, v := range pod.Annotations {
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
