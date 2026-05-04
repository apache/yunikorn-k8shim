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

	ginkgo.It("Basic_Quota_Preemption", func() {
		configMap, err := k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-preemption-enabled.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with quota preemption enabled")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		deployment1, err := k8s.GetDeploymentObj("../testdata/quota-preemption/deployments/deployment1.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(deployment1).NotTo(gomega.BeNil())
		ginkgo.By("Creating deployment1 in namespace " + dev)
		_, err = kClient.CreateDeployment(deployment1, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
		Ω(err).NotTo(gomega.HaveOccurred())
		for _, pod := range pods.Items {
			if pod.Status.Phase != v1.PodRunning {
				ginkgo.Fail(fmt.Sprintf("Expected pod %s to be in running state, but got %s", pod.Name, pod.Status.Phase))
			}
		}

		// update configmap and reduce quota to trigger quota preemption
		configMap, err = k8s.GetConfigMapObj("../testdata/quota-preemption/configs/yunikorn-configs-quota-reduced.yaml")
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(configMap).NotTo(gomega.BeNil())
		ginkgo.By("Updating the config map with reduced quota to trigger quota preemption")
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())

		// wait for quota preemption to take effect
		time.Sleep(5 * time.Second)

		pods, err = kClient.ListPodsByLabelSelector(dev, "queue=root.parent.queue-a")
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
