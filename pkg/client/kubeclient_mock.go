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
	"sync"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// KubeClientMock allows us to inject customized bind/delete pod functions
type KubeClientMock struct {
	bindFn         func(pod *v1.Pod, hostID string) error
	deleteFn       func(pod *v1.Pod) error
	createFn       func(pod *v1.Pod) (*v1.Pod, error)
	updateFn       func(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error)
	updateStatusFn func(pod *v1.Pod) (*v1.Pod, error)
	getFn          func(podName string) (*v1.Pod, error)
	clientSet      kubernetes.Interface
	pods           map[string]*v1.Pod
	lock           sync.RWMutex
	bindStats      BindStats
	boundPods      []BoundPod
}

// BindStats statistics about KubeClientMock.Bind() calls
type BindStats struct {
	First    time.Time
	Last     time.Time
	FirstPod string
	LastPod  string
	Success  int64
	Errors   int64
}

type BoundPod struct {
	Pod  string
	Host string
}

func NewKubeClientMock(err bool) *KubeClientMock {
	kubeMock := &KubeClientMock{
		deleteFn: func(pod *v1.Pod) error {
			if err {
				return fmt.Errorf("error deleting pod")
			}
			log.Log(log.Test).Info("pod deleted",
				zap.String("PodName", pod.Name))
			return nil
		},
		createFn: func(pod *v1.Pod) (*v1.Pod, error) {
			if err {
				return pod, fmt.Errorf("error creating pod")
			}
			log.Log(log.Test).Info("pod created",
				zap.String("PodName", pod.Name))
			return pod, nil
		},
		updateFn: func(pod *v1.Pod, podMutator func(*v1.Pod)) (*v1.Pod, error) {
			if err {
				return pod, fmt.Errorf("error updating pod")
			}
			podMutator(pod)
			log.Log(log.Test).Info("pod updated",
				zap.String("PodName", pod.Name))
			return pod, nil
		},
		updateStatusFn: func(pod *v1.Pod) (*v1.Pod, error) {
			if err {
				return pod, fmt.Errorf("error updating pod status")
			}
			log.Log(log.Test).Info("pod status updated",
				zap.String("PodName", pod.Name))
			return pod, nil
		},
		getFn: func(podName string) (*v1.Pod, error) {
			if err {
				return nil, fmt.Errorf("error getting pod")
			}
			log.Log(log.Test).Info("Getting pod",
				zap.String("PodName", podName))
			return nil, nil
		},
		clientSet: fake.NewSimpleClientset(),
		pods:      make(map[string]*v1.Pod),
		lock:      sync.RWMutex{},
		boundPods: make([]BoundPod, 0, 1024),
	}

	kubeMock.bindFn = func(pod *v1.Pod, hostID string) error {
		// kubeMock must be locked for this
		stats := &kubeMock.bindStats

		if err {
			stats.Errors++
			return fmt.Errorf("binding error")
		}
		log.Log(log.Test).Info("pod bound",
			zap.String("PodName", pod.Name))

		now := time.Now()
		if stats.FirstPod == "" {
			stats.FirstPod = pod.Name
			stats.First = now
		}
		stats.Last = now
		stats.LastPod = pod.Name
		kubeMock.boundPods = append(kubeMock.boundPods, BoundPod{
			Pod:  pod.Name,
			Host: hostID,
		})
		stats.Success++

		return nil
	}

	return kubeMock
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.bindFn(pod, hostID)
}

func (c *KubeClientMock) Create(pod *v1.Pod) (*v1.Pod, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.pods[getPodKey(pod)] = pod
	return c.createFn(pod)
}

func (c *KubeClientMock) UpdatePod(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.pods[getPodKey(pod)] = pod
	return c.updateFn(pod, podMutator)
}

func (c *KubeClientMock) UpdateStatus(pod *v1.Pod) (*v1.Pod, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.pods[getPodKey(pod)] = pod
	return c.updateStatusFn(pod)
}

func (c *KubeClientMock) Get(podNamespace string, podName string) (*v1.Pod, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	podKey := podNamespace + "/" + podName
	pod, ok := c.pods[podKey]
	if ok {
		return pod, nil
	}
	return nil, fmt.Errorf("pod not found: %s/%s", podNamespace, podName)
}

func (c *KubeClientMock) Delete(pod *v1.Pod) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.pods, getPodKey(pod))
	return c.deleteFn(pod)
}

func (c *KubeClientMock) GetClientSet() kubernetes.Interface {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.clientSet
}

func (c *KubeClientMock) GetConfigs() *rest.Config {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return nil
}

func (c *KubeClientMock) GetConfigMap(namespace string, name string) (*v1.ConfigMap, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return nil, nil
}

func (c *KubeClientMock) GetBindStats() BindStats {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.bindStats
}

func (c *KubeClientMock) GetBoundPods(clear bool) []BoundPod {
	c.lock.Lock()
	defer c.lock.Unlock()
	boundPods := c.boundPods
	if clear {
		c.boundPods = nil
	}
	return boundPods
}

func getPodKey(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}
