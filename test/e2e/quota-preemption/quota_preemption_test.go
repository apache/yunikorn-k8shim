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
package quota_preemption_test

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)
var dev string
var ns *v1.Namespace

var _ = ginkgo.Describe("QuotaPreemption", func() {

	ginkgo.BeforeEach(func() {
		dev = "dev-" + common.RandSeq(5)
		ginkgo.By(fmt.Sprintf("Creating namespace %s for testing", dev))
		var err error
		ns, err = kClient.CreateNamespace(dev, nil)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
		ginkgo.By("Get previous config")
		oldConfigMap, err = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(oldConfigMap).NotTo(gomega.BeNil())
	})


	ginkgo.It("Check_Basic_Quota_Preemption", func() {
		ginkgo.By("Quota preemption should be triggered when quota is reduced and delay is set.")
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with quota preemption enabled")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all deployment pods to be running")
		err = kClient.WaitForPodCount(dev, 3, 5*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 5)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All pods are running as expected before quota preemption is triggered")

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// wait for quota preemption to take effect, delay is set to 2s in the config
		time.Sleep(5 * time.Second)

		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
		Ω(err).NotTo(gomega.HaveOccurred())
		runningCount := 0
		pendingCount := 0
		for _, pod := range pods.Items {
			switch pod.Status.Phase {
			case v1.PodRunning:
				runningCount++
			case v1.PodPending:
				pendingCount++
			}
		}
		Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods to be running after quota preemption, but got %d", runningCount)
		Ω(pendingCount).To(gomega.Equal(1), "Expected 1 pod to be pending after quota preemption, but got %d", pendingCount)
	})

	ginkgo.It("Check_Quota_Preemption_With_Delay", func() {
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with quota preemption enabled")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all deployment pods to be running")
		err = kClient.WaitForPodCount(dev, 3, 5*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 5)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All pods are running as expected before quota preemption is triggered")

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-reduced-quota-preemption-20-seconds-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Quota preemption should not be triggered immediately due to the delay setting")
		time.Sleep(5 * time.Second)
		// check pods after config update, all pods should still be running as quota preemption delay is set to 50s
		err = kClient.WaitForPodCount(dev, 3, 5*time.Second)
		ginkgo.By("All pods are still running immediately after config update as expected due to quota preemption delay")

		// wait for quota preemption to take effect, delay is set to 50s in the config
		time.Sleep(60 * time.Second)

		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
		Ω(err).NotTo(gomega.HaveOccurred())
		runningCount := 0
		pendingCount := 0
		for _, pod := range pods.Items {
			switch pod.Status.Phase {
			case v1.PodRunning:
				runningCount++
			case v1.PodPending:
				pendingCount++
			}
		}
		Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods to be running after quota preemption, but got %d", runningCount)
		Ω(pendingCount).To(gomega.Equal(1), "Expected 1 pod to be pending after quota preemption, but got %d", pendingCount)
	})

	ginkgo.It("Quota_Preemption_With_Zero_Delay", func(){
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-zero-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with quota preemption enabled and zero delay")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all deployment pods to be running")
		err = kClient.WaitForPodCount(dev, 3, 5*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 5)
		Ω(err).NotTo(gomega.HaveOccurred())

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-preemption-disabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// wait for quota preemption to take effect
		time.Sleep(5 * time.Second)

		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
		Ω(err).NotTo(gomega.HaveOccurred())
		runningCount := 0
		pendingCount := 0
		for _, pod := range pods.Items {
			switch pod.Status.Phase {
			case v1.PodRunning:
				runningCount++
			case v1.PodPending:
				pendingCount++
			}
		}
		Ω(runningCount).To(gomega.Equal(3), "Expected 3 pods to be running after quota preemption, but got %d", runningCount)
		Ω(pendingCount).To(gomega.Equal(0), "Expected 0 pods to be pending after quota preemption, but got %d", pendingCount)
	})

	ginkgo.It("Basic_Quota_Preemption_Disabled", func() {
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-disabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with quota preemption disabled")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())
		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all deployment pods to be running")
		err = kClient.WaitForPodCount(dev, 3, 5*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 5)
		Ω(err).NotTo(gomega.HaveOccurred())

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-preemption-disabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// wait for quota preemption to take effect
		time.Sleep(5 * time.Second)

		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
		Ω(err).NotTo(gomega.HaveOccurred())
		runningCount := 0
		pendingCount := 0
		for _, pod := range pods.Items {
			switch pod.Status.Phase {
			case v1.PodRunning:
				runningCount++
			case v1.PodPending:
				pendingCount++
			}
		}
		Ω(runningCount).To(gomega.Equal(3), "Expected 3 pods to be running after quota preemption, but got %d", runningCount)
		Ω(pendingCount).To(gomega.Equal(0), "Expected 0 pods to be pending after quota preemption, but got %d", pendingCount)
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
		ginkgo.By("Tear down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		// reset config
		ginkgo.By("Resetting the config map to the original state")
		yunikorn.RestoreConfigMapWrapper(oldConfigMap)
	})
})
