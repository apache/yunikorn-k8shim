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

package test

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientv1 "k8s.io/client-go/listers/core/v1"
)

type PodListerMock struct {
	pods map[*v1.Pod]struct{}
}

func NewPodListerMock() *PodListerMock {
	return &PodListerMock{
		pods: make(map[*v1.Pod]struct{}),
	}
}

func (n *PodListerMock) AddPod(pod *v1.Pod) {
	n.pods[pod] = struct{}{}
}

func (n *PodListerMock) DeletePod(pod *v1.Pod) {
	delete(n.pods, pod)
}

func (n *PodListerMock) List(selector labels.Selector) (ret []*v1.Pod, err error) {
	result := make([]*v1.Pod, 0)
	for pod := range n.pods {
		if selector.Matches(labels.Set(pod.Labels)) {
			result = append(result, pod)
		}
	}
	return result, nil
}

func (n *PodListerMock) Get(name string) (*v1.Pod, error) {
	for pod := range n.pods {
		if pod.Name == name {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("pod %s is not found", name)
}

func (n *PodListerMock) Pods(namespace string) clientv1.PodNamespaceLister {
	panic("implement me")
}
