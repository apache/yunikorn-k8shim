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
	"context"
	"fmt"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type SchedulerKubeClient struct {
	clientSet *kubernetes.Clientset
}

// Every client identifies the concern it serves in its user agent so that the traffic can be
// attributed on the API server side. That is all the concern is used for.
const (
	userAgentScheduler           = "yunikorn-scheduler"
	userAgentEvents              = "yunikorn-scheduler/events"
	userAgentBootstrap           = "yunikorn-bootstrap"
	userAgentAdmissionController = "yunikorn-admission-controller"
)

// clientRateLimit normalises the configured qps and burst into the values set on the REST
// config. A negative qps creates no client side limiter, a qps of 0 leaves client-go on its
// defaults (5 QPS / 10 burst). For a positive qps client-go requires a positive burst, so an
// unset burst defaults to the qps.
func clientRateLimit(qps, burst int) (float32, int) {
	if burst <= 0 {
		burst = max(0, qps)
	}
	return float32(max(-1, qps)), burst
}

// newRestConfig creates a REST config for a single purpose client, see clientRateLimit for
// the way the configured qps and burst are applied.
func newRestConfig(kc string, qps, burst int, concern string) *rest.Config {
	config := CreateRestConfigOrDie(kc)
	config.UserAgent = concern
	config.QPS, config.Burst = clientRateLimit(qps, burst)

	log.Log(log.ShimClient).Info("creating Kubernetes client",
		zap.String("concern", concern),
		zap.Float32("qps", config.QPS),
		zap.Int("burst", config.Burst))
	return config
}

func newClientSetOrDie(config *rest.Config) *kubernetes.Clientset {
	configuredClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Log(log.ShimClient).Fatal("failed to get Clientset", zap.Error(err))
	}
	return configuredClient
}

// newKubeClient creates a client for a single concern. Beside the pod writes the scheduler
// client also backs the volume binder and the predicate framework handle, so the configured
// QPS and burst are an opt-in policy cap on those paths as well.
// The QPS and burst are passed in by the caller: the values from the configuration singleton
// can only be read after UpdateConfigMaps has run.
func newKubeClient(kc string, qps, burst int, concern string) SchedulerKubeClient {
	return SchedulerKubeClient{
		clientSet: newClientSetOrDie(newRestConfig(kc, qps, burst, concern)),
	}
}

func CreateRestConfigOrDie(kc string) *rest.Config {
	config, err := CreateRestConfig(kc)
	if err != nil {
		log.Log(log.ShimClient).Fatal("unable to create REST config, aborting", zap.Error(err))
	}
	return config
}

func CreateRestConfig(kc string) (*rest.Config, error) {
	// attempt to use in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil && err != rest.ErrNotInCluster {
		log.Log(log.ShimClient).Error("failed to create REST config", zap.Error(err))
		return nil, err
	}
	if config != nil {
		return config, nil
	}

	// fall back to kubeconfig if present
	if kc == "" {
		kc = conf.GetDefaultKubeConfigPath()
	}
	log.Log(log.ShimClient).Info(fmt.Sprintf("Not running inside Kubernetes; using KUBECONFIG at %s", kc))
	config, err = clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		log.Log(log.ShimClient).Error("failed to create kubeClient configs", zap.Error(err))
		return config, err
	}
	return config, nil
}

func (nc SchedulerKubeClient) GetClientSet() kubernetes.Interface {
	return nc.clientSet
}

func (nc SchedulerKubeClient) Bind(pod *v1.Pod, hostID string) error {
	log.Log(log.ShimClient).Info("bind pod to node",
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)),
		zap.String("nodeID", hostID))

	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Bind(
		context.Background(),
		&v1.Binding{ObjectMeta: apis.ObjectMeta{
			Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: hostID,
			},
		},
		apis.CreateOptions{}); err != nil {
		log.Log(log.ShimClient).Error("failed to bind pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return err
	}
	return nil
}

func (nc SchedulerKubeClient) Create(pod *v1.Pod) (*v1.Pod, error) {
	return nc.clientSet.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, apis.CreateOptions{})
}

func (nc SchedulerKubeClient) Delete(pod *v1.Pod) error {
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, apis.DeleteOptions{}); err != nil {
		log.Log(log.ShimClient).Warn("failed to delete pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return err
	}
	return nil
}

func (nc SchedulerKubeClient) GetConfigMap(namespace string, name string) (*v1.ConfigMap, error) {
	configmap, err := nc.clientSet.CoreV1().ConfigMaps(namespace).Get(context.Background(), name, apis.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		log.Log(log.ShimClient).Warn("failed to get configmap",
			zap.String("namespace", namespace),
			zap.String("name", name),
			zap.Error(err))
		return nil, err
	}
	return configmap, nil
}

func (nc SchedulerKubeClient) UpdatePod(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error) {
	var updatedPod *v1.Pod
	var updateErr error
	// Retrieve the latest version of Pod before attempting update
	// RetryOnConflict uses exponential backoff to avoid exhausting the API server
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestPod, getErr := nc.clientSet.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, apis.GetOptions{})
		if getErr != nil {
			log.Log(log.ShimClient).Warn("failed to get latest version of Pod",
				zap.Error(getErr))
		}
		// allow mutator to update pod
		podMutator(latestPod)
		if updatedPod, updateErr = nc.clientSet.CoreV1().Pods(pod.Namespace).Update(context.Background(), latestPod, apis.UpdateOptions{}); updateErr != nil {
			log.Log(log.ShimClient).Warn("failed to update pod",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.Error(updateErr))
			return updateErr
		}
		return nil
	})
	if retryErr != nil {
		log.Log(log.ShimClient).Error("Update pod failed",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(retryErr))
		return pod, retryErr
	}
	log.Log(log.ShimClient).Info("Successfully updated pod",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name))
	return updatedPod, nil
}

func (nc SchedulerKubeClient) UpdateStatus(pod *v1.Pod) (*v1.Pod, error) {
	var updatedPod *v1.Pod
	var updateErr error
	newPodStatus := pod.Status
	// In case of conflicts, retry using the logic in
	// https://github.com/kubernetes/client-go/blob/v0.21.1/examples/create-update-delete-deployment/main.go#L118-L121
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Retrieve the latest version of Pod before attempting status update
		// RetryOnConflict uses exponential backoff to avoid exhausting the API server
		latestPod, getErr := nc.clientSet.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, apis.GetOptions{})
		if getErr != nil {
			log.Log(log.ShimClient).Warn("failed to get latest version of Pod",
				zap.Error(getErr))
		}
		latestPod.Status = newPodStatus

		if updatedPod, updateErr = nc.clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(context.Background(), latestPod, apis.UpdateOptions{}); updateErr != nil {
			log.Log(log.ShimClient).Warn("failed to update pod status",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.Error(updateErr))
			return updateErr
		}
		return nil
	})
	if retryErr != nil {
		log.Log(log.ShimClient).Error("Update pod status failed",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(retryErr))
		return pod, retryErr
	}
	log.Log(log.ShimClient).Info("Successfully updated pod status",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.Stringer("newStatus", &pod.Status))
	return updatedPod, nil
}
