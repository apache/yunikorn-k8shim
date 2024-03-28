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

package cache

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

func getTaskMetadata(pod *v1.Pod) (TaskMetadata, bool) {
	appID := utils.GetApplicationIDFromPod(pod)
	if appID == "" {
		log.Log(log.ShimCacheTask).Debug("unable to get task for pod",
			zap.String("namespace", pod.Namespace),
			zap.String("name", pod.Name))
		return TaskMetadata{}, false
	}

	placeholder := utils.GetPlaceholderFlagFromPodSpec(pod)

	var taskGroupName string
	if !conf.GetSchedulerConf().DisableGangScheduling {
		taskGroupName = utils.GetTaskGroupFromPodSpec(pod)
	}

	return TaskMetadata{
		ApplicationID: appID,
		TaskID:        string(pod.UID),
		Pod:           pod,
		Placeholder:   placeholder,
		TaskGroupName: taskGroupName,
	}, true
}

func getAppMetadata(pod *v1.Pod) (ApplicationMetadata, bool) {
	appID := utils.GetApplicationIDFromPod(pod)
	if appID == "" {
		log.Log(log.ShimCacheApplication).Debug("unable to get application for pod",
			zap.String("namespace", pod.Namespace),
			zap.String("name", pod.Name))
		return ApplicationMetadata{}, false
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

	// Make sure we set the force create flag to true if the pod has been scheduled already.
	// When we create and link the metadata to an application the application does not exist yet.
	// The force flag prevents rejections during initialisation of already allocated pods in a changed
	// queue configuration.
	// It will also pick up static (mirror) and DaemonSet pods. In certain circumstances this could cause
	// pods to be allowed into the recovery queue while they should not. If this becomes an issue we can
	// add a filter here.
	// NOTE: this could fail to set the flag if the oldest pod for the application is not scheduled and
	// later pods are.
	tags[common.AppTagCreateForce] = constants.False
	if len(pod.Spec.NodeName) != 0 {
		tags[common.AppTagCreateForce] = constants.True
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

	var taskGroups []TaskGroup = nil
	var err error = nil
	if !conf.GetSchedulerConf().DisableGangScheduling {
		taskGroups, err = GetTaskGroupsFromAnnotation(pod)
		if err != nil {
			log.Log(log.ShimCacheApplication).Error("unable to get taskGroups for pod",
				zap.String("namespace", pod.Namespace),
				zap.String("name", pod.Name),
				zap.Error(err))
			events.GetRecorder().Eventf(pod, nil, v1.EventTypeWarning, "TaskGroupsError", "TaskGroupsError",
				"unable to get taskGroups for pod, reason: %s", err.Error())
		}
		tags[constants.AnnotationTaskGroups] = pod.Annotations[constants.AnnotationTaskGroups]
	}

	ownerReferences := getOwnerReference(pod)
	schedulingPolicyParams := GetSchedulingPolicyParam(pod)
	tags[constants.AnnotationSchedulingPolicyParam] = pod.Annotations[constants.AnnotationSchedulingPolicyParam]
	creationTime := pod.CreationTimestamp.Unix()

	return ApplicationMetadata{
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

func getOwnerReference(pod *v1.Pod) []metav1.OwnerReference {
	// Just return the originator pod as the owner of placeholder pods
	controller := false
	blockOwnerDeletion := true
	ref := metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "Pod",
		Name:               pod.Name,
		UID:                pod.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
	return []metav1.OwnerReference{ref}
}
