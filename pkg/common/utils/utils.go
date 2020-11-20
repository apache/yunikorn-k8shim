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
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func Convert2Pod(obj interface{}) (*v1.Pod, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, fmt.Errorf("cannot convert to *v1.Pod: %v", obj)
	}
	return pod, nil
}

func NeedRecovery(pod *v1.Pod) (bool, error) {
	if pod.Status.Phase == v1.PodPending {
		return false, nil
	}

	if !GeneralPodFilter(pod) {
		return false, nil
	}

	if pod.Spec.NodeName != "" {
		return true, nil
	}

	return false, fmt.Errorf("unknown pod state %v", pod)
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

func GeneralPodFilter(pod *v1.Pod) bool {
	return strings.Compare(pod.Spec.SchedulerName, constants.SchedulerName) == 0
}

func GetQueueNameFromPod(pod *v1.Pod) string {
	queueName := constants.ApplicationDefaultQueue
	if an, ok := pod.Labels[constants.LabelQueueName]; ok {
		queueName = an
	}
	return queueName
}

func GetApplicationIDFromPod(pod *v1.Pod) (string, error) {
	// application ID can be defined in annotations
	for name, value := range pod.Annotations {
		if name == constants.AnnotationApplicationID {
			return value, nil
		}
	}

	// application ID can be defined in labels
	for name, value := range pod.Labels {
		// application ID can be defined as a label
		if name == constants.LabelApplicationID {
			return value, nil
		}

		// if a pod for spark already provided appID, reuse it
		if name == constants.SparkLabelAppID {
			return value, nil
		}
	}

	return "", fmt.Errorf("unable to retrieve application ID from pod spec, %s",
		pod.Spec.String())
}

// compare the existing pod condition with the given one, return true if the pod condition remains not changed.
// return false if pod has no condition set yet, or condition has changed.
func PodUnderCondition(pod *v1.Pod, condition *v1.PodCondition) bool {
	_, current := podv1.GetPodCondition(&pod.Status, condition.Type)
	return current != nil && current.Status == condition.Status && current.Reason == condition.Reason
}

func GetNamespaceQuotaFromAnnotation(namespaceObj *v1.Namespace) *si.Resource {
	// retrieve resource quota info from annotations
	cpuQuota := namespaceObj.Annotations["yunikorn.apache.org/namespace.max.cpu"]
	memQuota := namespaceObj.Annotations["yunikorn.apache.org/namespace.max.memory"]

	// no quota found
	if cpuQuota == "" && memQuota == "" {
		return nil
	}

	return common.ParseResource(cpuQuota, memQuota)
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
