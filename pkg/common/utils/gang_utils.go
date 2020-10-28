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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
)

func FindAppTaskGroup(appTaskGroups []*v1alpha1.TaskGroup, groupName string) (*v1alpha1.TaskGroup, error) {
	if groupName == "" {
		// task has no group defined
		return nil, nil
	}

	// app has no taskGroups associated
	if len(appTaskGroups) == 0 {
		return nil, nil
	}

	// task group defined in app, return the corresponding taskGroup
	for _, tg := range appTaskGroups {
		if tg.Name == groupName {
			return tg, nil
		}
	}

	// task group name specified, but could not find a mapping value in app taskGroups
	return nil, fmt.Errorf("taskGroup %s is not defined in the application", groupName)
}

// the placeholder name is the pod name, pod name can not be longer than 63 chars
// taskGroup name and appID will be truncated if they go over 20/28 chars respectively
func GeneratePlaceholderName(taskGroupName, appID string, index int32) string {
	// taskGroup name no longer than 20 chars
	// appID no longer than 35 chars
	// total length no longer than 20 + 28 + 5 + 10 = 63
	shortTaskGroupName := fmt.Sprintf("%.20s", taskGroupName)
	shortAppID := fmt.Sprintf("%.28s", appID)
	return "tg-" + shortTaskGroupName + "-" + shortAppID + fmt.Sprintf("-%d", index)
}

func GetPlaceholderResourceRequest(resources map[string]resource.Quantity) v1.ResourceList {
	resourceReq := v1.ResourceList{}
	for k, v := range resources {
		resourceReq[v1.ResourceName(k)] = v
	}
	return resourceReq
}

func GetTaskGroupFromPodSpec(pod *v1.Pod) string {
	if value, ok := pod.Annotations[constants.AnnotationTaskGroupName]; ok {
		return value
	}
	return ""
}
