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
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubeClient interface {
	// bind a pod to a specific host
	Bind(pod *v1.Pod, hostID string) error

	// Create a pod
	Create(pod *v1.Pod) (*v1.Pod, error)

	// Delete a pod from a host
	Delete(pod *v1.Pod) error

	// Update a pod
	UpdatePod(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error)

	// Update the status of a pod
	UpdateStatus(pod *v1.Pod) (*v1.Pod, error)

	// Get a pod
	Get(podNamespace string, podName string) (*v1.Pod, error)

	// minimal expose this, only informers factory needs it
	GetClientSet() kubernetes.Interface

	GetConfigs() *rest.Config

	GetConfigMap(namespace string, name string) (*v1.ConfigMap, error)
}

func NewKubeClient(kc string) KubeClient {
	return newSchedulerKubeClient(kc)
}

func NewBootstrapKubeClient(kc string) KubeClient {
	return newBootstrapSchedulerKubeClient(kc)
}
