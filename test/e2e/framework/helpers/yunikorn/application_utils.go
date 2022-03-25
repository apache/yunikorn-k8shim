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

package yunikorn

import (
	"context"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	crdclientset "github.com/apache/yunikorn-k8shim/pkg/client/clientset/versioned"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func GetApplicationObj(yamlPath string) (*v1alpha1.Application, error) {
	manifest, err := os.Open(yamlPath)
	if err != nil {
		return nil, err
	}
	appObj := v1alpha1.Application{}
	if err := yaml.NewYAMLOrJSONDecoder(manifest, 100).Decode(&appObj); err != nil {
		return nil, err
	}
	return &appObj, nil
}

func CreateApplication(crdclientset crdclientset.Interface, app *v1alpha1.Application, namespace string) error {
	_, err := crdclientset.ApacheV1alpha1().Applications(namespace).Create(context.TODO(), app, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func UpdateApplication(crdclientset crdclientset.Interface, app *v1alpha1.Application, namespace string) error {
	_, err := crdclientset.ApacheV1alpha1().Applications(namespace).Update(context.TODO(), app, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func GetApplication(crdclientset crdclientset.Interface, namespace, name string) (*v1alpha1.Application, error) {
	app, err := crdclientset.ApacheV1alpha1().Applications(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return app, nil
}

func DeleteApplication(crdclientset crdclientset.Interface, namespace, name string) error {
	err := crdclientset.ApacheV1alpha1().Applications(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func NewApplicationClient() (*crdclientset.Clientset, error) {
	kubeconfig := configmanager.YuniKornTestConfig.KubeConfig
	appClient, err := NewClient(kubeconfig)
	if err != nil {
		return nil, err
	}
	return appClient, nil
}

func NewClient(kubeconfig string) (*crdclientset.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	appClient, err := crdclientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return appClient, nil
}
