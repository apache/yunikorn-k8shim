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

package client

import (
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// fake client allows us to inject customized bind/delete pod functions
type FakeKubeClient struct {
	BindFn   func(pod *v1.Pod, hostId string) error
	DeleteFn func(pod *v1.Pod) error
}

func (c *FakeKubeClient) Bind(pod *v1.Pod, hostId string) error {
	return c.BindFn(pod, hostId)
}

func (c *FakeKubeClient) Delete(pod *v1.Pod) error {
	return c.DeleteFn(pod)
}

func (c *FakeKubeClient) GetClientSet() *kubernetes.Clientset {
	return nil
}
