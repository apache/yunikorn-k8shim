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

package admission_controller_test

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

var kubeClient k8s.KubeCtl
var ns = "admission-controller-test"
var blackNs = "kube-system"
var restClient yunikorn.RClient
var oldConfigMap *v1.ConfigMap
var replicas = int32(1)
var testPod = v1.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Name:   "sleepjob",
		Labels: map[string]string{"app": "sleep"},
	},
	Spec: v1.PodSpec{
		Containers: []v1.Container{
			{
				Name:    "sleepjob",
				Image:   "alpine:latest",
				Command: []string{"sleep", "30"},
			},
		},
	},
}

var testDeployment = appsv1.Deployment{
	Spec: appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: testPod.Labels,
		},
		Template: v1.PodTemplateSpec{
			ObjectMeta: testPod.ObjectMeta,
			Spec:       testPod.Spec,
		},
	},
	ObjectMeta: testPod.ObjectMeta,
}

func TestAdmissionController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Admission Controller Suite")
}

var _ = BeforeSuite(func() {
	restClient = yunikorn.RClient{}

	kubeClient = k8s.KubeCtl{}
	Expect(kubeClient.SetClient()).To(BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	By("Port-forward the scheduler pod")
	err := kubeClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(HaveOccurred())

	By(fmt.Sprintf("Creating test namepsace %s", ns))
	namespace, err := kubeClient.CreateNamespace(ns, nil)
	Ω(err).ShouldNot(HaveOccurred())
	Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))

	By("Get the default ConfigMap and copy it")
	cm, err := kubeClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace, constants.ConfigMapName)
	Ω(err).ShouldNot(HaveOccurred())

	oldConfigMap = cm.DeepCopy()
	Ω(cm).Should(BeEquivalentTo(oldConfigMap))
})

var _ = AfterSuite(func() {
	kubeClient = k8s.KubeCtl{}
	Expect(kubeClient.SetClient()).To(BeNil())

	By(fmt.Sprintf("Deleting test namepsace %s", ns))
	err := kubeClient.DeleteNamespace(ns)
	Ω(err).ShouldNot(HaveOccurred())

	By("Restore the old config maps")
	c, err := kubeClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace, configmanager.DefaultYuniKornConfigMap)
	Ω(err).ShouldNot(HaveOccurred())

	c.Data = oldConfigMap.Data
	cm, err := kubeClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err).NotTo(HaveOccurred())
	Ω(cm).NotTo(BeNil())
})
