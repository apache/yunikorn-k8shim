/*
Copyright 2019 The Unity Scheduler Authors

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

package controller

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"k8s.io/api/core/v1"
)

type PodController struct {
	kubeClient client.KubeClient
}

func NewPodController(client client.KubeClient) *PodController {
	return &PodController{
		kubeClient: client,
	}
}

func (pc *PodController) Bind(p *v1.Pod, hostId string) error {
	if err := pc.kubeClient.Bind(p, hostId); err != nil {
		return err
	}
	glog.V(3).Infof("Successfully bound pod %s", p.Name)
	return nil
}

func (pc *PodController) DeletePod(pod *v1.Pod) {
	glog.V(3).Infof("Delete pod %s, because it is owned by a completed job", pod.Name)
	if err := pc.kubeClient.Delete(pod); err != nil {
		glog.V(1).Infof("Failed to delete pod %s, error: %#v", pod.Name, err)
	}
}