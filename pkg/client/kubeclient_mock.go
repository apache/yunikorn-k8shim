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

package client

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"

	"go.uber.org/zap"
)

// KubeClientMock allows us to inject customized bind/delete pod functions
type KubeClientMock struct {
	bindFn         func(pod *v1.Pod, hostID string) error
	deleteFn       func(pod *v1.Pod) error
	createFn       func(pod *v1.Pod) (*v1.Pod, error)
	updateStatusFn func(pod *v1.Pod) (*v1.Pod, error)
	getFn          func(podName string) (*v1.Pod, error)
	clientSet      kubernetes.Interface
	pods           map[string]*v1.Pod
}

func NewKubeClientMock() *KubeClientMock {
	return &KubeClientMock{
		bindFn: func(pod *v1.Pod, hostID string) error {
			log.Logger().Info("pod bound",
				zap.String("PodName", pod.Name))
			return nil
		},
		deleteFn: func(pod *v1.Pod) error {
			log.Logger().Info("pod deleted",
				zap.String("PodName", pod.Name))
			return nil
		},
		createFn: func(pod *v1.Pod) (*v1.Pod, error) {
			log.Logger().Info("pod created",
				zap.String("PodName", pod.Name))
			return pod, nil
		},
		updateStatusFn: func(pod *v1.Pod) (*v1.Pod, error) {
			log.Logger().Info("pod status updated",
				zap.String("PodName", pod.Name))
			return pod, nil
		},
		getFn: func(podName string) (*v1.Pod, error) {
			log.Logger().Info("Getting pod",
				zap.String("PodName", podName))
			return nil, nil
		},
		clientSet: fake.NewSimpleClientset(),
		pods:      make(map[string]*v1.Pod),
	}
}

func (c *KubeClientMock) MockBindFn(bfn func(pod *v1.Pod, hostID string) error) {
	c.bindFn = bfn
}

func (c *KubeClientMock) MockDeleteFn(dfn func(pod *v1.Pod) error) {
	c.deleteFn = dfn
}

func (c *KubeClientMock) MockCreateFn(cfn func(pod *v1.Pod) (*v1.Pod, error)) {
	c.createFn = cfn
}

func (c *KubeClientMock) Bind(pod *v1.Pod, hostID string) error {
	return c.bindFn(pod, hostID)
}

func (c *KubeClientMock) Create(pod *v1.Pod) (*v1.Pod, error) {
	c.pods[getPodKey(pod)] = pod
	return c.createFn(pod)
}

func (c *KubeClientMock) UpdateStatus(pod *v1.Pod) (*v1.Pod, error) {
	c.pods[getPodKey(pod)] = pod
	return c.updateStatusFn(pod)
}

func (c *KubeClientMock) Get(podNamespace string, podName string) (*v1.Pod, error) {
	podKey := podNamespace + "/" + podName
	pod, ok := c.pods[podKey]
	if ok {
		return pod, nil
	}
	return nil, fmt.Errorf("pod not found: %s/%s", podNamespace, podName)
}

func (c *KubeClientMock) Delete(pod *v1.Pod) error {
	delete(c.pods, getPodKey(pod))
	return c.deleteFn(pod)
}

func (c *KubeClientMock) GetClientSet() kubernetes.Interface {
	return c.clientSet
}

func (c *KubeClientMock) GetConfigs() *rest.Config {
	return nil
}

func getPodKey(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}
