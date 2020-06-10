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

package helpers

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
)

var err error

type KubeCtl struct {
	clientSet      *kubernetes.Clientset
	kubeConfigPath string
	kubeConfig     *rest.Config
}

// findKubeConfig finds path from env:KUBECONFIG or ~/.kube/config
func (k *KubeCtl) findKubeConfig() error {
	env := os.Getenv("KUBECONFIG")
	if env != "" {
		k.kubeConfigPath = env
		return nil
	}
	k.kubeConfigPath, err = expand(configmanager.YuniKornTestConfig.KubeConfig)
	return err
}

func expand(path string) (string, error) {
	if len(path) == 0 {
		return path, nil
	}

	if path[0] != '~' {
		return path, nil
	}

	if len(path) > 1 && path[1] != '/' && path[1] != '\\' {
		return "", errors.New("cannot expand user-specific home dir")
	}

	dir, err1 := os.UserHomeDir()
	if err1 != nil {
		return "", err1
	}

	return filepath.Join(dir, path[1:]), nil
}

func (k *KubeCtl) SetClient() error {
	if k.findKubeConfig() != nil {
		return err
	}
	k.kubeConfig, err = clientcmd.BuildConfigFromFlags("", k.kubeConfigPath)
	if err != nil {
		return err
	}
	k.clientSet, err = kubernetes.NewForConfig(k.kubeConfig)
	return err
}

func (k *KubeCtl) GetPods(namespace string) (*v1.PodList, error) {
	return k.clientSet.CoreV1().Pods(namespace).List(metav1.ListOptions{})
}

func (k *KubeCtl) GetService(serviceName string, namespace string) (*v1.Service, error) {
	return k.clientSet.CoreV1().Services(namespace).Get(serviceName, metav1.GetOptions{})
}

// Func to create a namespace provided a name
func (k *KubeCtl) CreateNamespace(namespace string) (*v1.Namespace, error) {
	epath, err := GetAbsPath(configmanager.NSTemplatePath)
	if err != nil {
		return nil, err
	}
	rs, err := yaml2Obj(epath)
	if err != nil {
		return nil, err
	}
	if nsSpec, ok := rs.(*v1.Namespace); ok {
		nsSpec.Name = namespace
		nsSpec.Labels = map[string]string{"Name": namespace}
		return k.clientSet.CoreV1().Namespaces().Create(nsSpec)
	}
	return nil, fmt.Errorf("unable to type cast the yaml object to v1.Namespace")
}

func (k *KubeCtl) DeleteNamespace(namespace string) error {
	return k.clientSet.CoreV1().Namespaces().Delete(namespace, &metav1.DeleteOptions{})
}

func GetConfigMapObj(yamlPath string) (*v1.ConfigMap, error) {
	c, err := yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	return c.(*v1.ConfigMap), err
}

func (k *KubeCtl) CreateConfigMap(cMap *v1.ConfigMap, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Create(cMap)
}

func (k *KubeCtl) GetConfigMap(name string, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Get(name, metav1.GetOptions{})
}

func (k *KubeCtl) UpdateConfigMap(cMap *v1.ConfigMap, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Update(cMap)
}

func (k *KubeCtl) DeleteConfigMap(cName string, namespace string) error {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Delete(cName, &metav1.DeleteOptions{})
}

func GetPodObj(yamlPath string) (*v1.Pod, error) {
	o, err := yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	return o.(*v1.Pod), err
}

func (k *KubeCtl) CreatePod(pod *v1.Pod, namespace string) (*v1.Pod, error) {
	return k.clientSet.CoreV1().Pods(namespace).Create(pod)
}

func (k *KubeCtl) DeletePod(podName string, namespace string) error {
	return k.clientSet.CoreV1().Pods(namespace).Delete(podName, &metav1.DeleteOptions{})
}

// return a condition function that indicates whether the given pod is
// currently running
func (k *KubeCtl) isPodRunning(podName string, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		fmt.Printf(".") // progress bar!

		pod, err := k.clientSet.CoreV1().Pods(namespace).Get(podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case v1.PodRunning:
			return true, nil
		case v1.PodFailed, v1.PodSucceeded:
			return false, fmt.Errorf("pod ran to completion")
		}
		return false, nil
	}
}

// Poll up to timeout seconds for pod to enter running state.
// Returns an error if the pod never enters the running state.
func (k *KubeCtl) waitForPodRunning(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, k.isPodRunning(podName, namespace))
}

// Returns the list of currently scheduled or running pods in `namespace` with the given selector
func (k *KubeCtl) ListPods(namespace string, selector string) (*v1.PodList, error) {
	listOptions := metav1.ListOptions{LabelSelector: selector}
	podList, err := k.clientSet.CoreV1().Pods(namespace).List(listOptions)

	if err != nil {
		return nil, err
	}
	return podList, nil
}

// Wait up to timeout seconds for all pods in 'namespace' with given 'selector' to enter running state.
// Returns an error if no pods are found or not all discovered pods enter running state.
func (k *KubeCtl) WaitForPodBySelectorRunning(namespace string, selector string, timeout int) error {
	podList, err := k.ListPods(namespace, selector)
	if err != nil {
		return err
	}
	if len(podList.Items) == 0 {
		return fmt.Errorf("no pods in %s with selector %s", namespace, selector)
	}

	for _, pod := range podList.Items {
		if err := k.waitForPodRunning(namespace, pod.Name, time.Duration(timeout)*time.Second); err != nil {
			return err
		}
	}
	return nil
}
