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

package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const userInfoKey = siCommon.DomainYuniKorn + "user.info"
const uniqueAutogenSuffix = "-uniqueautogen"

var pluginMode bool
var (
	// ErrorTimeout returned if waiting for a condition times out
	ErrorTimeout = errors.New("timeout waiting for condition")
)

func SetPluginMode(value bool) {
	pluginMode = value
}

func IsPluginMode() bool {
	return pluginMode
}

func Convert2Pod(obj interface{}) (*v1.Pod, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, fmt.Errorf("cannot convert to *v1.Pod: %v", obj)
	}
	return pod, nil
}

func Convert2ConfigMap(obj interface{}) *v1.ConfigMap {
	if configmap, ok := obj.(*v1.ConfigMap); ok {
		return configmap
	}
	log.Log(log.ShimUtils).Warn("cannot convert to *v1.ConfigMap", zap.Stringer("type", reflect.TypeOf(obj)))
	return nil
}

func Convert2PriorityClass(obj interface{}) *schedulingv1.PriorityClass {
	if priorityClass, ok := obj.(*schedulingv1.PriorityClass); ok {
		return priorityClass
	}
	log.Log(log.ShimUtils).Warn("cannot convert to *schedulingv1.PriorityClass", zap.Stringer("type", reflect.TypeOf(obj)))
	return nil
}

// PodAlreadyBound returns true if a newly initializing Pod is already assigned to a Node
func PodAlreadyBound(pod *v1.Pod) bool {
	// pod already bound needs to satisfy conditions:
	// 1. Pod is scheduled by us
	// 2. pod is already assigned to a node
	// 3. pod is not in terminated state
	if GetApplicationIDFromPod(pod) != "" && IsAssignedPod(pod) && !IsPodTerminated(pod) {
		return true
	}

	return false
}

func IsPodRunning(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodRunning
}

func IsPodTerminated(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded
}

// assignedPod selects pods that are assigned (scheduled and running).
func IsAssignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func GetQueueNameFromPod(pod *v1.Pod) string {
	// Queue name can be defined in multiple places
	// The queue name is determined by the following order
	// 1. Label: constants.CanonicalLabelQueueName
	// 2. Annotation: constants.AnnotationQueueName
	// 3. Label: constants.LabelQueueName
	queueName := ""
	if canonicalLabelQueueName := GetPodLabelValue(pod, constants.CanonicalLabelQueueName); canonicalLabelQueueName != "" {
		queueName = canonicalLabelQueueName
	} else if annotationQueueName := GetPodAnnotationValue(pod, constants.AnnotationQueueName); annotationQueueName != "" {
		queueName = annotationQueueName
	} else if labelQueueName := GetPodLabelValue(pod, constants.LabelQueueName); labelQueueName != "" {
		queueName = labelQueueName
	}

	return queueName
}

// GenerateApplicationID generates an appID based on the namespace value
// if configured to generate unique appID, generate appID as <namespace>-<pod-uid> namespace capped at 26chars
// if not set or configured as false, appID generated as <autogen-prefix>-<namespace>-<autogen-suffix>
func GenerateApplicationID(namespace string, generateUniqueAppIds bool, podUID string) string {
	var generatedID string
	if generateUniqueAppIds {
		if podUID == "" {
			// we must be running in the admission controller and don't yet have a pod UID assigned
			generatedID = fmt.Sprintf("%.26s%s", namespace, uniqueAutogenSuffix)
		} else {
			generatedID = fmt.Sprintf("%.26s-%s", namespace, podUID)
		}
	} else {
		generatedID = fmt.Sprintf("%s-%s-%s", constants.AutoGenAppPrefix, namespace, constants.AutoGenAppSuffix)
	}

	return fmt.Sprintf("%.63s", generatedID)
}

// GetApplicationIDFromPod returns the Application for a Pod. If a Pod is marked as schedulable by YuniKorn but is
// missing an ApplicationID, one will be generated here (if YuniKorn is running in standard mode) or an empty string
// will be returned (if YuniKorn is running in plugin mode).
// If an Application ID is returned, the Pod is managed by YuniKorn. Otherwise, it is managed by an external scheduler.
func GetApplicationIDFromPod(pod *v1.Pod) string {
	// SchedulerName needs to match
	if strings.Compare(pod.Spec.SchedulerName, constants.SchedulerName) != 0 {
		return ""
	}

	// If pod was tagged with ignore-application and plugin mode is active, return
	if pluginMode {
		if value := GetPodAnnotationValue(pod, constants.AnnotationIgnoreApplication); value != "" {
			ignore, err := strconv.ParseBool(value)
			if err != nil {
				log.Log(log.ShimUtils).Warn("Failed to parse annotation "+constants.AnnotationIgnoreApplication, zap.Error(err))
			} else if ignore {
				return ""
			}
		}
	}

	// Application ID can be defined in multiple places
	// The application ID is determined by the following order.
	// 1. Label: constants.CanonicalLabelApplicationID
	// 2. Annotation: constants.AnnotationApplicationID
	// 3. Label: constants.LabelApplicationID
	// 4. Label: constants.SparkLabelAppID

	appID := GetPodLabelValue(pod, constants.CanonicalLabelApplicationID)

	if appID == "" {
		appID = GetPodAnnotationValue(pod, constants.AnnotationApplicationID)
	}

	if appID == "" {
		labelKeys := []string{
			constants.LabelApplicationID,
			constants.SparkLabelAppID,
		}
		for _, label := range labelKeys {
			appID = GetPodLabelValue(pod, label)
			if appID != "" {
				break
			}
		}
	}

	// If plugin mode, interpret missing Application ID as a non-YuniKorn pod
	if pluginMode && appID == "" {
		return ""
	}

	// does appID end with '-uniqueautogen'?
	if strings.HasSuffix(appID, uniqueAutogenSuffix) {
		// replace suffix with pod UID
		appID = fmt.Sprintf("%s-%s", strings.TrimSuffix(appID, uniqueAutogenSuffix), string(pod.UID))
	}

	// if app ID is not empty, return it
	if appID != "" {
		return appID
	}

	// Standard deployment mode, so we need a valid Application ID to proceed. Generate one now.
	return GenerateApplicationID(pod.Namespace, conf.GetSchedulerConf().GenerateUniqueAppIds, string(pod.UID))
}

func CheckAppIdInPod(pod *v1.Pod) error {
	return ValidatePodLabelAnnotation(pod, constants.AppIdLabelKeys, constants.AppIdAnnotationKeys)
}

func CheckQueueNameInPod(pod *v1.Pod) error {
	return ValidatePodLabelAnnotation(pod, constants.QueueLabelKeys, constants.QueueAnnotationKeys)
}

// return true if all non-empty values are same across all provided label/annotation
func ValidatePodLabelAnnotation(pod *v1.Pod, labelKeys []string, annotationKeys []string) error {
	var referenceKey string
	var referenceValue string
	var referenceType string

	checkingType := constants.Label
	for _, key := range labelKeys {
		value := GetPodLabelValue(pod, key)
		if value == "" {
			continue
		}
		if referenceValue == "" {
			referenceKey = key
			referenceValue = value
			referenceType = checkingType
		} else if referenceValue != value {
			return fmt.Errorf("%s %s: \"%s\" doesn't match %s %s: \"%s\"", checkingType, key, value, referenceType, referenceKey, referenceValue)
		}
	}

	checkingType = constants.Annotation
	for _, key := range annotationKeys {
		value := GetPodAnnotationValue(pod, key)
		if value == "" {
			continue
		}
		if referenceValue == "" {
			referenceKey = key
			referenceValue = value
			referenceType = checkingType
		} else if referenceValue != value {
			return fmt.Errorf("%s %s: \"%s\" doesn't match %s %s: \"%s\"", checkingType, key, value, referenceType, referenceKey, referenceValue)
		}
	}

	return nil
}

// compare the existing pod condition with the given one, return true if the pod condition remains not changed.
// return false if pod has no condition set yet, or condition has changed.
func PodUnderCondition(pod *v1.Pod, condition *v1.PodCondition) bool {
	_, current := podv1.GetPodCondition(&pod.Status, condition.Type)
	return current != nil && current.Status == condition.Status && current.Reason == condition.Reason
}

// get namespace guaranteed resource from namespace annotation
func GetNamespaceGuaranteedFromAnnotation(namespaceObj *v1.Namespace) *si.Resource {
	// retrieve guaranteed resource info from annotations
	if guaranteed := GetNameSpaceAnnotationValue(namespaceObj, constants.NamespaceGuaranteed); guaranteed != "" {
		var namespaceGuaranteedMap map[string]string
		err := json.Unmarshal([]byte(guaranteed), &namespaceGuaranteedMap)
		if err != nil {
			log.Log(log.ShimUtils).Warn("Unable to process namespace.guaranteed annotation",
				zap.String("namespace", namespaceObj.Name),
				zap.String("namespace.guaranteed is", guaranteed))
			return nil
		}
		return common.GetResource(namespaceGuaranteedMap)
	}
	return nil
}

// get namespace max apps from namespace annotation
func GetNamespaceMaxAppsFromAnnotation(namespaceObj *v1.Namespace) string {
	if maxApps := GetNameSpaceAnnotationValue(namespaceObj, constants.NamespaceMaxApps); maxApps != "" {
		numMaxApp, err := strconv.Atoi(maxApps)
		if err != nil {
			log.Log(log.ShimUtils).Warn("Unable to process namespace.maxApps annotation",
				zap.String("namespace", namespaceObj.Name),
				zap.String("namespace.maxApps is", maxApps))
			return ""
		}
		if numMaxApp < 0 {
			log.Log(log.ShimUtils).Warn("Invalid value for namespace.maxApps annotation",
				zap.String("namespace", namespaceObj.Name),
				zap.String("namespace.maxApps is", maxApps))
			return ""
		}
		return maxApps
	}
	return ""
}

func GetNamespaceQuotaFromAnnotation(namespaceObj *v1.Namespace) *si.Resource {
	// retrieve resource quota info from annotations
	cpuQuota := GetNameSpaceAnnotationValue(namespaceObj, constants.CPUQuota)
	memQuota := GetNameSpaceAnnotationValue(namespaceObj, constants.MemQuota)
	namespaceQuota := GetNameSpaceAnnotationValue(namespaceObj, constants.NamespaceQuota)

	// order of annotation preference
	// 1. namespace.quota
	// 2. namespace.max.* (Retaining for backwards compatibility. Need to be removed in next major release)
	switch {
	case namespaceQuota != "":
		if cpuQuota != "" || memQuota != "" {
			log.Log(log.ShimUtils).Warn("Using namespace.quota instead of namespace.max.* (deprecated) annotation to set cpu and/or memory for namespace though both are available.",
				zap.String("namespace", namespaceObj.Name))
		}
		var namespaceQuotaMap map[string]string
		err := json.Unmarshal([]byte(namespaceQuota), &namespaceQuotaMap)
		if err != nil {
			log.Log(log.ShimUtils).Warn("Unable to process namespace.quota annotation",
				zap.String("namespace", namespaceObj.Name),
				zap.String("namespace.quota is", namespaceQuota))
			return nil
		}
		return common.GetResource(namespaceQuotaMap)
	case cpuQuota != "" || memQuota != "":
		log.Log(log.ShimUtils).Warn("Please use namespace.quota instead of namespace.max.* (deprecated) annotation. Using deprecated annotation to set cpu and/or memory for namespace. ",
			zap.String("namespace", namespaceObj.Name))
		return common.ParseResource(cpuQuota, memQuota)
	default:
		return nil
	}
}

func WaitForCondition(eval func() bool, interval time.Duration, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if eval() {
			return nil
		}

		if time.Now().After(deadline) {
			return ErrorTimeout
		}

		time.Sleep(interval)
	}
}

// merge two string maps
// if the same key defined in the first and second maps
// the value will be set by the second map
func MergeMaps(first, second map[string]string) map[string]string {
	if first == nil && second == nil {
		return nil
	}
	result := make(map[string]string)
	for k, v := range first {
		result[k] = v
	}
	for k, v := range second {
		result[k] = v
	}
	return result
}

// GetUserFromPod find username from pod annotation or label
func GetUserFromPod(pod *v1.Pod) (string, []string) {
	if pod.Annotations[userInfoKey] != "" {
		userInfoJSON := pod.Annotations[userInfoKey]
		var userGroup si.UserGroupInformation
		err := json.Unmarshal([]byte(userInfoJSON), &userGroup)
		if err != nil {
			log.Log(log.ShimUtils).Error("unable to process user info annotation", zap.Error(err))
			return constants.DefaultUser, nil
		}
		user := userGroup.User
		groups := userGroup.Groups
		if user == "" {
			log.Log(log.ShimUtils).Warn("got empty username, using default")
			user = constants.DefaultUser
		}
		log.Log(log.ShimUtils).Info("found user info from pod annotations",
			zap.String("username", user), zap.Strings("groups", groups))
		return user, groups
	}

	// Label is processed for backwards compatibility
	userLabelKey := conf.GetSchedulerConf().UserLabelKey
	// UserLabelKey should not be empty
	if len(userLabelKey) == 0 {
		userLabelKey = constants.DefaultUserLabel
	}
	// User name to be defined in labels
	if username := GetPodLabelValue(pod, userLabelKey); username != "" && len(username) > 0 {
		log.Log(log.ShimUtils).Info("Found user name from pod labels.",
			zap.String("userLabel", userLabelKey), zap.String("user", username))
		return username, nil
	}
	value := constants.DefaultUser

	log.Log(log.ShimUtils).Debug("Unable to retrieve user name from pod labels. Empty user label",
		zap.String("userLabel", userLabelKey))

	return value, nil
}

// GetCoreSchedulerConfigFromConfigMap resolves a yunikorn configmap into a core scheduler config.
// If the configmap is missing or the policy group doesn't exist, uses a default configuration
func GetCoreSchedulerConfigFromConfigMap(config map[string]string) string {
	// use default config if there isn't one
	if len(config) == 0 {
		return ""
	}
	policyGroup := conf.GetSchedulerConf().PolicyGroup
	if data, ok := config[fmt.Sprintf("%s.yaml", policyGroup)]; ok {
		return data
	}
	return ""
}

// GetExtraConfigFromConfigMap filters the configmap entries, returning those that are not yaml
func GetExtraConfigFromConfigMap(config map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range config {
		if strings.HasSuffix(k, ".yaml") {
			continue
		}
		result[k] = v
	}
	return result
}

func GetPodAnnotationValue(pod *v1.Pod, annotationKey string) string {
	if value, ok := pod.Annotations[annotationKey]; ok {
		return value
	}
	return ""
}

func GetNameSpaceAnnotationValue(namespace *v1.Namespace, annotationKey string) string {
	if value, ok := namespace.Annotations[annotationKey]; ok {
		return value
	}
	return ""
}

func GetPodLabelValue(pod *v1.Pod, labelKey string) string {
	if value, ok := pod.Labels[labelKey]; ok {
		return value
	}
	return ""
}

func GetTaskGroupFromPodSpec(pod *v1.Pod) string {
	return GetPodAnnotationValue(pod, constants.AnnotationTaskGroupName)
}

func GetPlaceholderFlagFromPodSpec(pod *v1.Pod) bool {
	if value := GetPodAnnotationValue(pod, constants.AnnotationPlaceholderFlag); value != "" {
		if v, err := strconv.ParseBool(value); err == nil {
			return v
		}
	}

	// Deprecated: Support for old placeholder flags should be removed in version 1.7.0.
	if value := GetPodAnnotationValue(pod, constants.OldAnnotationPlaceholderFlag); value != "" { // nolint:staticcheck
		if v, err := strconv.ParseBool(value); err == nil {
			log.Log(log.ShimUtils).Warn("Using deprecated placeholder annotation. The support for old placeholder flag will be removed in version 1.7.0",
				zap.String("podName", pod.Name),
				zap.String("annotation", constants.OldAnnotationPlaceholderFlag), // nolint:staticcheck
				zap.String("value", value))
			return v
		}
	}

	// Deprecated: Support for old placeholder flags should be removed in version 1.7.0.
	if value := GetPodLabelValue(pod, constants.OldLabelPlaceholderFlag); value != "" { // nolint:staticcheck
		if v, err := strconv.ParseBool(value); err == nil {
			log.Log(log.ShimUtils).Warn("Using deprecated placeholder label. The support for old placeholder flag will be removed in version 1.7.0",
				zap.String("podName", pod.Name),
				zap.String("label", constants.OldLabelPlaceholderFlag), // nolint:staticcheck
				zap.String("value", value))
			return v
		}
	}

	return false
}
