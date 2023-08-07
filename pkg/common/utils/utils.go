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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const userInfoKey = siCommon.DomainYuniKorn + "user.info"

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

func NeedRecovery(pod *v1.Pod) bool {
	// pod requires recovery needs to satisfy both conditions
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
	queueName := constants.ApplicationDefaultQueue
	if an := GetPodLabelValue(pod, constants.LabelQueueName); an != "" {
		queueName = an
	} else if qu := GetPodAnnotationValue(pod, constants.AnnotationQueueName); qu != "" {
		queueName = qu
	}
	return queueName
}

// GetApplicationIDFromPod returns the applicationID (if present) from a Pod or an empty string if not present.
// If an applicationID is present, the Pod is managed by YuniKorn. Otherwise, it is managed by an external scheduler.
func GetApplicationIDFromPod(pod *v1.Pod) string {
	// SchedulerName needs to match
	if strings.Compare(pod.Spec.SchedulerName, constants.SchedulerName) != 0 {
		return ""
	}
	// if pod was tagged with ignore-application, return
	if value := GetPodAnnotationValue(pod, constants.AnnotationIgnoreApplication); value != "" {
		ignore, err := strconv.ParseBool(value)
		if err != nil {
			log.Log(log.ShimUtils).Warn("Failed to parse annotation "+constants.AnnotationIgnoreApplication, zap.Error(err))
		} else if ignore {
			return ""
		}
	}
	// application ID can be defined in annotations
	if value := GetPodAnnotationValue(pod, constants.AnnotationApplicationID); value != "" {
		return value
	}
	if value := GetPodLabelValue(pod, constants.LabelApplicationID); value != "" {
		return value
	}
	// application ID can be defined in labels
	if value := GetPodLabelValue(pod, constants.SparkLabelAppID); value != "" {
		return value
	}
	// no application ID found, this is not a YuniKorn-managed Pod
	return ""
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
	namespaceGuaranteed := GetNameSpaceAnnotationValue(namespaceObj, constants.NamespaceGuaranteed)
	var namespaceGuaranteedMap map[string]string
	err := json.Unmarshal([]byte(namespaceGuaranteed), &namespaceGuaranteedMap)
	if err != nil {
		log.Log(log.ShimUtils).Warn("Unable to process namespace.guaranteed annotation",
			zap.String("namespace", namespaceObj.Name),
			zap.String("namespace.guaranteed is", namespaceGuaranteed))
		return nil
	}
	return common.GetResource(namespaceGuaranteedMap)
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

type K8sResource struct {
	ResourceName v1.ResourceName
	Value        int64
}

func NewK8sResourceList(resources ...K8sResource) map[v1.ResourceName]resource.Quantity {
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	for _, r := range resources {
		resourceList[r.ResourceName] = *resource.NewQuantity(r.Value, resource.DecimalSI)
	}
	return resourceList
}

func WaitForCondition(eval func() bool, interval time.Duration, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if eval() {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for condition")
		}

		time.Sleep(interval)
	}
}

func PodForTest(podName, memory, cpu string) *v1.Pod {
	containers := make([]v1.Container, 0)
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceMemory] = resource.MustParse(memory)
	c1Resources[v1.ResourceCPU] = resource.MustParse(cpu)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})

	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

func NodeForTest(nodeID, memory, cpu string) *v1.Node {
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = resource.MustParse(memory)
	resourceList[v1.ResourceName("cpu")] = resource.MustParse(cpu)
	return &v1.Node{
		TypeMeta: apis.TypeMeta{
			Kind:       "Node",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      nodeID,
			Namespace: "default",
			UID:       "uid_0001",
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Allocatable: resourceList,
		},
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

	if value := GetPodLabelValue(pod, constants.LabelPlaceholderFlag); value != "" {
		if v, err := strconv.ParseBool(value); err == nil {
			return v
		}
	}
	return false
}

func GetTaskGroupsFromAnnotation(pod *v1.Pod) ([]interfaces.TaskGroup, error) {
	taskGroupInfo := GetPodAnnotationValue(pod, constants.AnnotationTaskGroups)
	if taskGroupInfo == "" {
		return nil, nil
	}

	taskGroups := []interfaces.TaskGroup{}
	err := json.Unmarshal([]byte(taskGroupInfo), &taskGroups)
	if err != nil {
		return nil, err
	}
	// json.Unmarchal won't return error if name or MinMember is empty, but will return error if MinResource is empty or error format.
	for _, taskGroup := range taskGroups {
		if taskGroup.Name == "" {
			return nil, fmt.Errorf("can't get taskGroup Name from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinResource == nil {
			return nil, fmt.Errorf("can't get taskGroup MinResource from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember == int32(0) {
			return nil, fmt.Errorf("can't get taskGroup MinMember from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember < int32(0) {
			return nil, fmt.Errorf("minMember cannot be negative, %s",
				taskGroupInfo)
		}
	}
	return taskGroups, nil
}
