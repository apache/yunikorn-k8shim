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
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amConf "github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const userInfoAnnotation = "yunikorn.apache.org/user.info"

type EventHandler struct {
	updateCh chan struct{}
}

func (e *EventHandler) OnAdd(_ interface{}) {}

func (e *EventHandler) OnUpdate(_, _ interface{}) {
	e.updateCh <- struct{}{}
}

func (e *EventHandler) OnDelete(_ interface{}) {}

func (e *EventHandler) WaitForUpdate(timeout time.Duration) bool {
	t := time.After(timeout)

	for {
		select {
		case <-t:
			return false
		case <-e.updateCh:
			return true
		}
	}
}

var _ = ginkgo.Describe("AdmissionController", func() {
	ginkgo.BeforeEach(func() {
		kubeClient = k8s.KubeCtl{}
		gomega.Expect(kubeClient.SetClient()).To(gomega.BeNil())
	})

	ginkgo.It("Verifying a pod is created in the test namespace", func() {
		ginkgo.By("has 1 running pod whose SchedulerName is yunikorn")
		pod, err := kubeClient.CreatePod(&testPod, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		// Wait for pod to move into running state
		err = kubeClient.WaitForPodBySelectorRunning(ns,
			fmt.Sprintf("app=%s", pod.ObjectMeta.Labels["app"]), 10)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(pod.Spec.SchedulerName).Should(gomega.BeEquivalentTo(constants.SchedulerName))
	})

	ginkgo.It("Verifying a pod is created on namespace blacklist", func() {
		ginkgo.By("Create a pod in namespace blacklist")
		pod, err := kubeClient.CreatePod(&testPod, blackNs)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, blackNs)

		err = kubeClient.WaitForPodBySelectorRunning(blackNs,
			fmt.Sprintf("app=%s", pod.ObjectMeta.Labels["app"]), 10)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(pod.Spec.SchedulerName).ShouldNot(gomega.BeEquivalentTo(constants.SchedulerName))

	})

	ginkgo.It("Verifying the scheduler configuration is overridden", func() {
		invalidConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: make(map[string]string),
		}

		res, err := restClient.ValidateSchedulerConfig(invalidConfigMap)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(res.Allowed).Should(gomega.BeEquivalentTo(false))
	})

	ginkgo.It("Configure the scheduler with an empty configmap", func() {
		emptyConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: make(map[string]string),
		}
		cm, err := kubeClient.UpdateConfigMap(&emptyConfigMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(cm).ShouldNot(gomega.BeNil())
	})

	ginkgo.It("Configure the scheduler with invalid configmap", func() {
		invalidConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: map[string]string{"queues.yaml": "invalid"},
		}
		invalidCm, err := kubeClient.UpdateConfigMap(&invalidConfigMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).Should(gomega.HaveOccurred())
		gomega.Ω(invalidCm).ShouldNot(gomega.BeNil())
	})

	ginkgo.It("Check that annotation is added to a pod & cannot be modified", func() {
		ginkgo.By("Create a pod")
		pod, err := kubeClient.CreatePod(&testPod, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		err = kubeClient.WaitForPodBySelector(ns,
			fmt.Sprintf("app=%s", pod.ObjectMeta.Labels["app"]), 10*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		pod, err = kubeClient.GetPod(pod.Name, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		userinfo := pod.Annotations["yunikorn.apache.org/user.info"]
		gomega.Ω(userinfo).Should(gomega.Not(gomega.BeNil()))

		ginkgo.By("Attempt to update userinfo annotation")
		_, err = kubeClient.UpdatePodWithAnnotation(pod, ns, "yunikorn.apache.org/user.info", "shouldnotsucceed")
		gomega.Ω(err).Should(gomega.HaveOccurred())
	})

	ginkgo.It("Check that annotation is added to a deployment", func() {
		ginkgo.By("Create a deployment")
		deployment, err := kubeClient.CreateDeployment(&testDeployment, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deleteDeployment(deployment, ns)
		err = kubeClient.WaitForPodBySelector(ns,
			fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]), 10*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Get running pod")
		var pods *v1.PodList
		pods, err = kubeClient.GetPods(ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		fmt.Fprintf(ginkgo.GinkgoWriter, "Found %d pods in namespace %s\n", len(pods.Items), ns)
		gomega.Ω(len(pods.Items)).To(gomega.Equal(1))
		fmt.Fprintf(ginkgo.GinkgoWriter, "Running pod is %s\n", pods.Items[0].Name)
		pod, err2 := kubeClient.GetPod(pods.Items[0].Name, ns)
		gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
		userinfo := pod.Annotations["yunikorn.apache.org/user.info"]
		gomega.Ω(userinfo).Should(gomega.Not(gomega.BeNil()))
	})

	ginkgo.It("Check that deployment is rejected when controller users are not trusted", func() {
		ginkgo.By("Retrieve existing configmap")
		configMap, err := kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data[amConf.AMAccessControlTrustControllers] = "false"
		ginkgo.By("Update configmap")
		stopChan := make(chan struct{})
		eventHandler := &EventHandler{updateCh: make(chan struct{})}
		err = kubeClient.StartConfigMapInformer(configmanager.YuniKornTestConfig.YkNamespace, stopChan, eventHandler)
		defer close(stopChan)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		updateOk := eventHandler.WaitForUpdate(30 * time.Second)
		gomega.Ω(updateOk).To(gomega.Equal(true))
		time.Sleep(time.Second)

		ginkgo.By("Create a deployment")
		deployment, err2 := kubeClient.CreateDeployment(&testDeployment, ns)
		gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
		defer deleteDeployment(deployment, ns)

		// pod is not expected to appear
		ginkgo.By("Check for sleep pods (should time out)")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			10*time.Second)
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error: %v\n", err)
		gomega.Ω(err).Should(gomega.HaveOccurred())
		ginkgo.By("Check deployment status")
		deployment, err = kubeClient.GetDeployment(testDeployment.Name, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		fmt.Fprintf(ginkgo.GinkgoWriter, "Replicas: %d, AvailableReplicas: %d, ReadyReplicas: %d\n",
			deployment.Status.Replicas, deployment.Status.AvailableReplicas, deployment.Status.ReadyReplicas)
		gomega.Ω(deployment.Status.Replicas).To(gomega.Equal(int32(0)))
		gomega.Ω(deployment.Status.AvailableReplicas).To(gomega.Equal(int32(0)))
		gomega.Ω(deployment.Status.ReadyReplicas).To(gomega.Equal(int32(0)))

		// restore setting
		ginkgo.By("Restore trustController setting")
		configMap, err = kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		configMap.Data[amConf.AMAccessControlTrustControllers] = "true"
		_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		updateOk = eventHandler.WaitForUpdate(30 * time.Second)
		gomega.Ω(updateOk).To(gomega.Equal(true))
		time.Sleep(time.Second)

		// pod is expected to appear
		ginkgo.By("Check for sleep pod")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			60*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("Check that deployment is rejected when external user is not trusted", func() {
		ginkgo.By("Retrieve existing configmap")
		configMap, err := kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data[amConf.AMAccessControlExternalUsers] = ""
		ginkgo.By("Update configmap")
		stopChan := make(chan struct{})
		eventHandler := &EventHandler{updateCh: make(chan struct{})}
		err = kubeClient.StartConfigMapInformer(configmanager.YuniKornTestConfig.YkNamespace, stopChan, eventHandler)
		defer close(stopChan)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		updateOk := eventHandler.WaitForUpdate(30 * time.Second)
		gomega.Ω(updateOk).To(gomega.Equal(true))
		time.Sleep(time.Second)

		ginkgo.By("Create a deployment")
		deployment := testDeployment.DeepCopy()
		deployment.Spec.Template.Annotations = make(map[string]string)
		deployment.Spec.Template.Annotations[userInfoAnnotation] = "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}"
		_, err = kubeClient.CreateDeployment(deployment, ns)
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error received from API server: %v\n", err)
		gomega.Ω(err).Should(gomega.HaveOccurred())
		gomega.Ω(err).To(gomega.BeAssignableToTypeOf(&errors.StatusError{}))

		// modify setting
		ginkgo.By("Changing allowed externalUser setting")
		configMap, err = kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		configMap.Data[amConf.AMAccessControlExternalUsers] = "(^minikube-user$|^kubernetes-admin$)" // works with Minikube & KIND
		_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		updateOk = eventHandler.WaitForUpdate(30 * time.Second)
		gomega.Ω(updateOk).To(gomega.Equal(true))
		time.Sleep(time.Second)

		// submit deployment again
		ginkgo.By("Submit deployment again")
		_, err = kubeClient.CreateDeployment(deployment, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deleteDeployment(deployment, ns)

		// pod is expected to appear
		ginkgo.By("Check for sleep pod")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			60*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		// call the healthCheck api to check scheduler health
		ginkgo.By("Check YuniKorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(checks).Should(gomega.Equal(""), checks)
	})
})

func deletePod(pod *v1.Pod, namespace string) {
	if pod != nil {
		ginkgo.By("Delete pod " + pod.Name)
		err := kubeClient.DeletePod(pod.Name, namespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	}
}

func deleteDeployment(deployment *appsv1.Deployment, namespace string) {
	if deployment != nil {
		ginkgo.By("Delete deployment " + deployment.Name)
		err := kubeClient.DeleteDeployment(deployment.Name, namespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		pods, err2 := kubeClient.GetPods(ns)
		gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
		ginkgo.By("Forcibly deleting pods")
		for _, pod := range pods.Items {
			//nolint:errcheck
			_ = kubeClient.DeletePod(pod.Name, namespace)
		}

		err = kubeClient.WaitForPodCount(namespace, 0, 10*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	}
}
