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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
)

type Placeholder struct {
	appID         string
	taskGroupName string
	pod           *v1.Pod
}

func newPlaceholder(placeholderName string, app *Application, taskGroup v1alpha1.TaskGroup) *Placeholder {
	placeholderPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      placeholderName,
			Namespace: app.tags[constants.AppTagNamespace],
			Labels: map[string]string{
				constants.LabelApplicationID: app.GetApplicationID(),
				constants.LabelQueueName:     app.GetQueue(),
			},
			Annotations: map[string]string{
				constants.AnnotationPlaceholderFlag: "true",
				constants.AnnotationTaskGroupName:   taskGroup.Name,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  constants.PlaceholderContainerName,
					Image: constants.PlaceholderContainerImage,
					Resources: v1.ResourceRequirements{
						Requests: utils.GetPlaceholderResourceRequest(taskGroup.MinResource),
					},
				},
			},
			RestartPolicy: constants.PlaceholderPodRestartPolicy,
			SchedulerName: constants.SchedulerName,
			NodeSelector:  taskGroup.NodeSelector,
			Tolerations:   taskGroup.Tolerations,
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
