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

package general

import (
	"strings"

	v1 "k8s.io/api/core/v1"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

func getTaskMetadata(pod *v1.Pod) (interfaces.TaskMetadata, bool) {
	appID, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger().Debug("unable to get task by given pod", zap.Error(err))
		return interfaces.TaskMetadata{}, false
	}

	placeholder := utils.GetPlaceholderFlagFromPodSpec(pod)

	var taskGroupName string
	if !conf.GetSchedulerConf().DisableGangScheduling {
		taskGroupName = utils.GetTaskGroupFromPodSpec(pod)
	}

	return interfaces.TaskMetadata{
		ApplicationID: appID,
		TaskID:        string(pod.UID),
		Pod:           pod,
		Placeholder:   placeholder,
		TaskGroupName: taskGroupName,
	}, true
}

func getAppMetadata(pod *v1.Pod, recovery bool) (interfaces.ApplicationMetadata, bool) {
	appID, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger().Debug("unable to get application for pod",
			zap.String("namespace", pod.Namespace),
			zap.String("name", pod.Name),
			zap.Error(err))
		return interfaces.ApplicationMetadata{}, false
	}

	// tags will at least have namespace info
	// labels or annotations from the pod can be added when needed
	// user info is retrieved via service account
	tags := map[string]string{}
	if pod.Namespace == "" {
		tags[constants.AppTagNamespace] = constants.DefaultAppNamespace
	} else {
		tags[constants.AppTagNamespace] = pod.Namespace
	}
	if isStateAwareDisabled(pod) {
		tags[siCommon.AppTagStateAwareDisable] = "true"
	}

	// attach imagePullSecrets if present
	secrets := pod.Spec.ImagePullSecrets
	if len(secrets) > 0 {
		arr := make([]string, len(secrets))
		for i, secret := range secrets {
			arr[i] = secret.Name
		}
		tags[constants.AppTagImagePullSecrets] = strings.Join(arr, ",")
	}

	// get the user from Pod Labels
	user, groups := utils.GetUserFromPod(pod)

	var taskGroups []v1alpha1.TaskGroup = nil
	if !conf.GetSchedulerConf().DisableGangScheduling {
		taskGroups, err = utils.GetTaskGroupsFromAnnotation(pod)
		if err != nil {
			log.Logger().Error("unable to get taskGroups for pod",
				zap.String("namespace", pod.Namespace),
				zap.String("name", pod.Name),
				zap.Error(err))
			events.GetRecorder().Eventf(pod, nil, v1.EventTypeWarning, "TaskGroupsError", "TaskGroupsError",
				"unable to get taskGroups for pod, reason: %s", err.Error())
		}
		tags[constants.AnnotationTaskGroups] = pod.Annotations[constants.AnnotationTaskGroups]
	}

	ownerReferences := getOwnerReferences(pod)
	schedulingPolicyParams := utils.GetSchedulingPolicyParam(pod)
	tags[constants.AnnotationSchedulingPolicyParam] = pod.Annotations[constants.AnnotationSchedulingPolicyParam]

	var creationTime int64
	if recovery {
		creationTime = pod.CreationTimestamp.Unix()
	}

	return interfaces.ApplicationMetadata{
		ApplicationID:              appID,
		QueueName:                  utils.GetQueueNameFromPod(pod),
		User:                       user,
		Groups:                     groups,
		Tags:                       tags,
		TaskGroups:                 taskGroups,
		OwnerReferences:            ownerReferences,
		SchedulingPolicyParameters: schedulingPolicyParams,
		CreationTime:               creationTime,
	}, true
}
