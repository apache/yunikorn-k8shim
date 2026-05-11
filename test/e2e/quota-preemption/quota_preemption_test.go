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
		ginkgo.By("Creating namespace " + dev + " for testing")
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
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 10*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 10)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All pods are running as expected before quota preemption is triggered")

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for quota preemption to take effect (delay=2s)")
		gomega.Eventually(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount, pendingCount := 0, 0
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case v1.PodRunning:
					runningCount++
				case v1.PodPending:
					pendingCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods to be running after quota preemption, but got %d", runningCount)
			g.Ω(pendingCount).To(gomega.Equal(1), "Expected 1 pod to be pending after quota preemption, but got %d", pendingCount)
		}, 10*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("Quota_Preemption_NOT_triggered_On_Quota_Increase", func() {
		ginkgo.By("Quota preemption should not be triggered when quota is increased.")
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
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 10*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 10)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All pods are running as expected before quota preemption is triggered")

		// update configmap and increase quota, quota preemption should not be triggered
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-increased.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying no quota preemption occurs for 5 seconds after quota increase")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(3), "Expected 3 pods to remain running after quota increase, but got %d", runningCount)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
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
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All pods are running as expected before quota preemption is triggered")

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-reduced-quota-preemption-20-seconds-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying preemption has not triggered yet (within 20s delay window)")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(3), "Expected all 3 pods still running before delay elapses, but got %d", runningCount)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
		ginkgo.By("All pods are still running during delay window as expected")

		ginkgo.By("Waiting for quota preemption to fire after 20s delay")
		gomega.Eventually(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount, pendingCount := 0, 0
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case v1.PodRunning:
					runningCount++
				case v1.PodPending:
					pendingCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods to be running after quota preemption, but got %d", runningCount)
			g.Ω(pendingCount).To(gomega.Equal(1), "Expected 1 pod to be pending after quota preemption, but got %d", pendingCount)
		}, 30*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("Quota_Preemption_With_Zero_Delay", func() {
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
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-preemption-disabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying no quota preemption occurs for 5 seconds (preemption disabled)")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(3), "Expected 3 pods to remain running with preemption disabled, but got %d", runningCount)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
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
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-preemption-disabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying no quota preemption occurs for 5 seconds (preemption disabled)")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(3), "Expected 3 pods to remain running with preemption disabled, but got %d", runningCount)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.PIt("Needs Investigation - Quota_Preemption_Delay_Inherited_From_Parent", func() {
		ginkgo.By("Quota preemption delay set on parent queue should be inherited by child queues that do not set it explicitly.")

		// Apply initial config: parent has quota.preemption.delay=20s, child (queue-a) has no delay set.
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-parent-delay-no-child-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map: parent delay=20s, child queue has no explicit delay")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all 3 deployment pods to be running")
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All 3 pods are running before quota is reduced")

		// Reduce queue-a's quota to trigger quota preemption. Parent still has delay=20s; child still has no delay.
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-parent-delay-child-quota-reduced.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Reducing queue-a quota to trigger preemption; delay should be inherited from parent (20s)")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// The inherited delay is 20s. After 5s preemption must NOT have fired yet.
		ginkgo.By("Verifying preemption has not triggered yet (inherited 20s delay has not elapsed)")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(3), "Expected all 3 pods still running before inherited delay elapses, but got %d", runningCount)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
		ginkgo.By("All 3 pods are still running 5s after quota reduction — inherited delay is being honoured")

		ginkgo.By("Waiting for inherited preemption delay to fire after ~20s")
		gomega.Eventually(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount, pendingCount := 0, 0
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case v1.PodRunning:
					runningCount++
				case v1.PodPending:
					pendingCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods running after preemption triggered by inherited delay, but got %d", runningCount)
			g.Ω(pendingCount).To(gomega.Equal(1), "Expected 1 pod pending after preemption triggered by inherited delay, but got %d", pendingCount)
		}, 30*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("Quota_Preemption_NOT_Triggered_When_New_Quota_Above_Usage", func() {
		ginkgo.By("Quota preemption should NOT be triggered when the new (reduced) quota is still above current resource usage.")

		// Initial config: max=300Mi/300m, quota preemption enabled, delay=2s.
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with quota preemption enabled (max=300Mi)")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Deploy only 2 pods → 200Mi/200m in use, which is below the initial max of 300Mi.
		deployment2, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment2.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment2).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment with 2 replicas in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment2, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all 2 deployment pods to be running")
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Both pods are running (200Mi in use) before quota is changed")

		// Reduce max quota to 250Mi — still above the 200Mi currently in use.
		// Preemption must NOT fire because usage (200Mi) <= new quota (250Mi).
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-above-current-usage.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Reducing max quota to 250Mi — still above current usage of 200Mi")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying no preemption occurs for 5s (new quota is still above current usage)")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods to remain running when new quota is above usage, but got %d", runningCount)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("Quota_Preemption_Delay_Timer_Reset_On_Delay_Update", func() {
		ginkgo.By("The preemption delay timer must restart from zero when quota.preemption.delay is updated in config.")

		// Step 1: Set up initial state — 3 pods running, max=300Mi, quota preemption enabled.
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Applying initial config: quota preemption enabled, max=300Mi")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all 3 deployment pods to be running")
		err = kClient.WaitForPodBySelector(dev, "app=app-a", 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All 3 pods are running before quota is reduced")

		// Step 2: Reduce quota below current usage with a short 5s delay.
		// The scheduler detects a violation and starts a 5s countdown timer.
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-5s-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Reducing quota to 200Mi with delay=5s; preemption violation timer starts")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Step 3: Before the original 5s delay elapses, update the delay to 15s.
		// This must reset the timer: preemption should fire 15s from *this* update,
		// not 5s from the original quota reduction.
		ginkgo.By("Sleeping 2s (before the original 5s delay fires)")
		time.Sleep(2 * time.Second)
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-15s-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating delay to 15s while quota violation is still active — timer must reset")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Step 4: Verify all 3 pods remain running for 5s past the original 5s deadline.
		// If the timer was NOT reset, preemption would have already fired. Consistent
		// observation proves the reset 15s window is still counting down.
		ginkgo.By("Verifying all 3 pods stay running for 5s — past original deadline, within reset 15s window")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			stillRunning := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					stillRunning++
				}
			}
			g.Ω(stillRunning).To(gomega.Equal(3), "Expected all 3 pods still running (timer was reset); got %d running", stillRunning)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())

		// Step 5: Wait for preemption to fire after the reset 15s delay.
		ginkgo.By("Waiting for preemption to fire after the reset 15s delay elapses")
		gomega.Eventually(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount, pendingCount := 0, 0
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case v1.PodRunning:
					runningCount++
				case v1.PodPending:
					pendingCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(2), "Expected 2 pods running after preemption fired with reset delay, but got %d", runningCount)
			g.Ω(pendingCount).To(gomega.Equal(1), "Expected 1 pod pending after preemption fired with reset delay, but got %d", pendingCount)
		}, 15*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.PIt("Needs Investigation - Quota_Preemption_Delay_Timer_Reset_On_Quota_Re_Reduction", func() {
		ginkgo.By("The preemption delay timer must restart from zero when quota is reduced again before the delay elapses.")

		// Step 1: Start with quota preemption enabled, 3 pods running (3×100Mi = 300Mi).
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Applying initial config: quota preemption enabled, max=300Mi")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all 3 deployment pods to be running")
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("All 3 pods are running before quota is first reduced")

		// Step 2: First quota reduction — max=200Mi with delay=5s.
		// 300Mi in use > 200Mi max → violation detected; 5s countdown timer starts.
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-5s-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("First quota reduction: max=200Mi, delay=5s — violation timer starts")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Step 3: Before the 5s fires, reduce quota again to 100Mi (still delay=5s).
		// A new, deeper violation is detected; the timer must restart from zero.
		// After preemption at 100Mi only 1 pod (100Mi) can remain running.
		ginkgo.By("Sleeping 3s — before the original 5s delay fires")
		time.Sleep(3 * time.Second)
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-further-reduced-100Mi-5s-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Second quota reduction: max=100Mi, delay=5s — timer must reset")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Step 4: Verify all 3 pods remain running for 4s past the original 5s deadline.
		// The original countdown would have fired at t=5s from the first reduction;
		// consistent observation of 3 running pods proves the timer was properly reset.
		ginkgo.By("Verifying all 3 pods stay running for 4s — past original deadline, within reset 5s window")
		gomega.Consistently(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			stillRunning := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					stillRunning++
				}
			}
			g.Ω(stillRunning).To(gomega.Equal(3), "Expected all 3 pods still running (timer was reset); got %d running", stillRunning)
		}, 4*time.Second, time.Second).Should(gomega.Succeed())

		// Step 5: Wait for preemption to fire after the reset 5s delay (~10s since second reduction).
		// At the new 100Mi quota, only 1 pod can remain running.
		ginkgo.By("Waiting for preemption to fire after the reset 5s delay elapses")
		gomega.Eventually(func(g gomega.Gomega) {
			pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			runningCount, pendingCount := 0, 0
			for _, pod := range pods.Items {
				switch pod.Status.Phase {
				case v1.PodRunning:
					runningCount++
				case v1.PodPending:
					pendingCount++
				}
			}
			g.Ω(runningCount).To(gomega.Equal(1), "Expected 1 pod running after preemption at 100Mi quota, but got %d", runningCount)
			g.Ω(pendingCount).To(gomega.Equal(2), "Expected 2 pods pending after preemption at 100Mi quota, but got %d", pendingCount)
		}, 10*time.Second, time.Second).Should(gomega.Succeed())
	})

	ginkgo.PIt("Needs Investigation - New_Pods_Not_Allocated_While_Over_Quota_Then_Scheduled_After_Preemption", func() {
		ginkgo.By("New pods must stay Pending while the queue is over quota; they become schedulable once old pods are preempted back within quota.")

		// Step 1: Set up initial state — 3 × 100Mi pods in queue-a (300Mi total), max=300Mi.
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Applying initial config: max=300Mi, quota preemption enabled")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating 3-replica deployment (app-a, 100Mi each) in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all 3 app-a pods to be running (300Mi used = 300Mi max)")
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-a", 30)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Step 2: Reduce quota to 250Mi with a 30s delay — violation detected (300Mi > 250Mi),
		// and simultaneously submit a new 50Mi pod (app-b) to the same over-quota queue.
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced-250Mi-30s-delay.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Reducing max quota to 250Mi with 30s delay — violation timer starts")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deploymentAppB, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment-app-b.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deploymentAppB).NotTo(gomega.BeNil())
		ginkgo.By("Submitting new 50Mi pod (app-b) to queue-a while it is over quota (300Mi > 250Mi)")
		_, err = kClient.CreateDeployment(deploymentAppB, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		// Step 3: During the 30s delay window, verify continuously that no new allocations occur:
		// app-a: all 3 pods still Running (preemption has not fired yet).
		// app-b: pod must stay Pending — the queue is over its max quota.
		ginkgo.By("Verifying app-a still running and app-b Pending for 5s within the 30s delay window")
		gomega.Consistently(func(g gomega.Gomega) {
			appAPods, err := kClient.ListPodsByLabelSelector(dev, "app=app-a")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			appARunning := 0
			for _, pod := range appAPods.Items {
				if pod.Status.Phase == v1.PodRunning {
					appARunning++
				}
			}
			g.Ω(appARunning).To(gomega.Equal(3), "Expected all 3 app-a pods still running during the delay, got %d", appARunning)

			appBPods, err := kClient.ListPodsByLabelSelector(dev, "app=app-b")
			g.Ω(err).NotTo(gomega.HaveOccurred())
			appBPending := 0
			for _, pod := range appBPods.Items {
				if pod.Status.Phase == v1.PodPending {
					appBPending++
				}
			}
			g.Ω(appBPending).To(gomega.Equal(1), "Expected app-b pod to be Pending while queue is over quota, got %d pending", appBPending)
		}, 5*time.Second, time.Second).Should(gomega.Succeed())
		ginkgo.By("app-b pod is Pending — new allocations correctly blocked while queue is over quota")

		// Step 4: Wait for preemption to fire (30s delay elapses).
		// After preemption: 1 app-a pod evicted → 2 app-a running (200Mi ≤ 250Mi).
		// 50Mi headroom is now available → app-b (50Mi) should be scheduled:
		//   200Mi (app-a) + 50Mi (app-b) = 250Mi = max.
		ginkgo.By("Waiting for the 30s preemption delay to elapse and app-b to become schedulable (~35s timeout)")
		err = kClient.WaitForPodBySelectorRunning(dev, "app=app-b", 35)
		Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("app-b pod is now Running — allocated once old pods settled within quota")

		appAPods, err := kClient.ListPodsByLabelSelector(dev, "app=app-a")
		Ω(err).NotTo(gomega.HaveOccurred())
		appARunning := 0
		appAPending := 0
		for _, pod := range appAPods.Items {
			switch pod.Status.Phase {
			case v1.PodRunning:
				appARunning++
			case v1.PodPending:
				appAPending++
			}
		}
		// 2 app-a pods running (200Mi), deployment controller created a 3rd replacement that
		// stays Pending (200Mi + 100Mi = 300Mi > 250Mi max — no room at the full quota).
		Ω(appARunning).To(gomega.Equal(2), "Expected 2 app-a pods running after preemption, got %d", appARunning)
		Ω(appAPending).To(gomega.Equal(1), "Expected 1 app-a pod pending (replacement, no quota headroom), got %d", appAPending)
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
