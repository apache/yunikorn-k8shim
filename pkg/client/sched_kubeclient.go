/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type SchedulerKubeClient struct {
	clientSet *kubernetes.Clientset
}

func newSchedulerKubeClient(kc string) SchedulerKubeClient {
	// using kube config
	if kc != "" {
		config, err := clientcmd.BuildConfigFromFlags("", kc)
		if err != nil {
			log.Logger.Fatal("failed to create kubeClient configs", zap.Error(err))
		}
		configuredClient := kubernetes.NewForConfigOrDie(config)
		return SchedulerKubeClient{
			clientSet: configuredClient,
		}
	}

	// using in cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Logger.Fatal("failed to get InClusterConfig", zap.Error(err))
	}

	configuredClient, err := kubernetes.NewForConfig(config)
	return SchedulerKubeClient{
		clientSet: configuredClient,
	}
}

func (nc SchedulerKubeClient) GetClientSet() *kubernetes.Clientset {
	return nc.clientSet
}

func (nc SchedulerKubeClient) Bind(pod *v1.Pod, hostId string) error {
	log.Logger.Info("bind pod to node",
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)),
		zap.String("nodeId", hostId))

	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Bind(
		&v1.Binding{ObjectMeta: apis.ObjectMeta{
			Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: hostId,
		},
	}); err != nil {
		log.Logger.Error("failed to bind pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return err
	}
	return nil

}

func (nc SchedulerKubeClient) Delete(pod *v1.Pod) error {
	// TODO make this configurable for pods
	gracefulSeconds := int64(3)
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &apis.DeleteOptions{
		GracePeriodSeconds: &gracefulSeconds,
	}); err != nil {
		log.Logger.Error("failed to delete pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return err
	}
	return nil
}

