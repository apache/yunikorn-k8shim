/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"
	"time"
)

func Convert2Pod(obj interface{}) (*v1.Pod, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, fmt.Errorf("cannot convert to *v1.Pod: %v", obj)
	}
	return pod, nil
}

// assignedPod selects pods that are assigned (scheduled and running).
func IsAssignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

func IsSchedulablePod(pod *v1.Pod) bool {
	return strings.Compare(pod.Spec.SchedulerName, conf.GetSchedulerConf().SchedulerName) == 0
}

func GetQueueNameFromPod(pod *v1.Pod) string {
	queueName := common.ApplicationDefaultQueue
	if an, ok := pod.Labels[common.LabelQueueName]; ok {
		queueName = an
	}
	return queueName
}

func GetApplicationIdFromPod(pod *v1.Pod) (string, error) {
	for name, value := range pod.Labels {
		// if a pod for spark already provided appId, reuse it
		if name == common.SparkLabelAppId {
			return value, nil
		}

		// application ID can be defined as a label
		if name == common.LabelApplicationId {
			return value, nil
		}
	}

	// application ID can be defined in annotations too
	for name, value := range pod.Annotations {
		if name == common.LabelApplicationId {
			return value, nil
		}
	}
	return "", fmt.Errorf("unable to retrieve application ID from pod spec, %s",
		pod.Spec.String())
}

type K8sResource struct {
	ResourceName v1.ResourceName
	Value int64
}

func NewK8sResourceList(resources...K8sResource) map[v1.ResourceName]resource.Quantity {
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