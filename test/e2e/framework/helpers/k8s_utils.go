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
	"fmt"
	"github.com/mitchellh/go-homedir"
	. "github.com/onsi/ginkgo"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"yunikorn-qe/framework/cfg_manager"
)

var err error

type KubeCtl struct{
	clientSet *kubernetes.Clientset
	kubeConfigPath string
	kubeConfig *rest.Config
}

// FindKubeConfig finds path from env:KUBECONFIG or ~/.kube/config
func (k *KubeCtl) FindKubeConfig() {
	env := os.Getenv("KUBECONFIG")
	if env != "" {
		k.kubeConfigPath = env
	}
	k.kubeConfigPath, err = homedir.Expand(cfg_manager.YuniKornTestConfig.KubeConfig)
	if err != nil {
		fmt.Fprintln(GinkgoWriter, err)
	}
}

func (k *KubeCtl) SetClient() {
	k.FindKubeConfig()
	k.kubeConfig, err = clientcmd.BuildConfigFromFlags("", k.kubeConfigPath)
	check(err)
	// Create an rest client not targeting specific API version
	k.clientSet, err = kubernetes.NewForConfig(k.kubeConfig)
	check(err)
}

func (k *KubeCtl) GetPods(namespace string) (*v1.PodList, error){
	return k.clientSet.CoreV1().Pods(namespace).List(metav1.ListOptions{})
}

func (k *KubeCtl)  GetService(serviceName string, namespace string) (*v1.Service, error){
	 return k.clientSet.CoreV1().Services(namespace).Get(serviceName, metav1.GetOptions{})
}

// Func to create a namespace provided a name
func (k *KubeCtl) CreateNamespace(namespace string) (*v1.Namespace, error){
	nsSpec := yaml2Obj(cfg_manager.NSTemplatePath).(*v1.Namespace)
	nsSpec.Name = namespace
	nsSpec.Labels = map[string]string{"Name": namespace}
	return k.clientSet.CoreV1().Namespaces().Create(nsSpec)
}

func (k *KubeCtl)  DeleteNamespace(namespace string) error{
	return k.clientSet.CoreV1().Namespaces().Delete(namespace, &metav1.DeleteOptions{})
}

func GetConfigMapObj(yamlPath string) *v1.ConfigMap {
	return yaml2Obj(yamlPath).(*v1.ConfigMap)
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

func GetPodObj(yamlPath string) *v1.Pod {
	return yaml2Obj(yamlPath).(*v1.Pod)
}

func (k *KubeCtl) CreatePod(pod *v1.Pod, namespace string) (*v1.Pod, error) {
	return k.clientSet.CoreV1().Pods(namespace).Create(pod)
}

func (k *KubeCtl) DeletePod(podName string, namespace string) error {
	return k.clientSet.CoreV1().Pods(namespace).Delete(podName, &metav1.DeleteOptions{})
}