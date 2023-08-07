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
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

// MUST: run the placeholder pod as non-root user
// It doesn't matter which user we use to start the placeholders,
// as long as it is not the root user. This is because the placeholder
// is just a dummy container, that doesn't run anything.
// On most of Linux distributions, uid bigger than 1000 is recommended
// for normal user uses. So we are using 1000(uid)/3000(gid) here to
// launch all the placeholder pods.
var runAsUser int64 = 1000
var runAsGroup int64 = 3000

type Placeholder struct {
	appID         string
	taskGroupName string
	pod           *v1.Pod
}

func newPlaceholder(placeholderName string, app *Application, taskGroup interfaces.TaskGroup) *Placeholder {
	// Here the owner reference is always the originator pod
	ownerRefs := app.getPlaceholderOwnerReferences()
	annotations := utils.MergeMaps(taskGroup.Annotations, map[string]string{
		constants.AnnotationPlaceholderFlag: "true",
		constants.AnnotationTaskGroupName:   taskGroup.Name,
	})

	// Add "yunikorn.apache.org/task-groups" annotation to the placeholder to aid recovery
	tgDef := app.GetTaskGroupsDefinition()
	if tgDef != "" {
		annotations = utils.MergeMaps(annotations, map[string]string{
			constants.AnnotationTaskGroups: tgDef,
		})
	}

	// Add "yunikorn.apache.org/schedulingPolicyParameters" to the placeholder to aid recovery
	schedParamsDef := app.GetSchedulingParamsDefinition()
	if schedParamsDef != "" {
		annotations = utils.MergeMaps(annotations, map[string]string{
			constants.AnnotationSchedulingPolicyParam: schedParamsDef,
		})
	}

	// Add imagePullSecrets to the placeholder
	imagePullSecrets := make([]v1.LocalObjectReference, 0)
	if secrets, ok := app.tags[constants.AppTagImagePullSecrets]; ok {
		for _, secret := range strings.Split(secrets, ",") {
			imagePullSecrets = append(imagePullSecrets, v1.LocalObjectReference{Name: secret})
		}
	}

	placeholderPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      placeholderName,
			Namespace: app.tags[constants.AppTagNamespace],
			Labels: utils.MergeMaps(taskGroup.Labels, map[string]string{
				constants.LabelApplicationID:   app.GetApplicationID(),
				constants.LabelQueueName:       app.GetQueue(),
				constants.LabelPlaceholderFlag: "true",
			}),
			Annotations:     annotations,
			OwnerReferences: ownerRefs,
		},
		Spec: v1.PodSpec{
			SecurityContext: &v1.PodSecurityContext{
				RunAsUser:  &runAsUser,
				RunAsGroup: &runAsGroup,
			},
			ImagePullSecrets: imagePullSecrets,
			Containers: []v1.Container{
				{
					Name:            constants.PlaceholderContainerName,
					Image:           conf.GetSchedulerConf().PlaceHolderImage,
					ImagePullPolicy: v1.PullIfNotPresent,
					Resources: v1.ResourceRequirements{
						Requests: utils.GetPlaceholderResourceRequest(taskGroup.MinResource),
					},
				},
			},
			RestartPolicy: constants.PlaceholderPodRestartPolicy,
			SchedulerName: constants.SchedulerName,
			NodeSelector:  taskGroup.NodeSelector,
			Tolerations:   taskGroup.Tolerations,
			Affinity:      taskGroup.Affinity,
		},
	}

	return &Placeholder{
		appID:         app.GetApplicationID(),
		taskGroupName: taskGroup.Name,
		pod:           placeholderPod,
	}
}

func (p *Placeholder) String() string {
	return fmt.Sprintf("appID: %s, taskGroup: %s, podName: %s/%s",
		p.appID, p.taskGroupName, p.pod.Namespace, p.pod.Name)
}
