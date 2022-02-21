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

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = ginkgo.Describe("AdmissionController", func() {

	ginkgo.BeforeEach(func() {
		kubeClient = k8s.KubeCtl{}
		gomega.Expect(kubeClient.SetClient()).To(gomega.BeNil())
	})

	ginkgo.It("Verifying a pod is created in the test namespace", func() {

		ginkgo.By("has 1 running pod whose SchedulerName is yunikorn")
		sleepPodConfigs := common.SleepPodConfig{Name: sleepPodName, NS: ns}
		sleepRespPod, err := kubeClient.CreatePod(common.InitSleepPod(sleepPodConfigs), ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		// Wait for pod to move into running state
		err = kubeClient.WaitForPodBySelectorRunning(ns,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]), 10)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(sleepRespPod.Spec.SchedulerName).Should(gomega.BeEquivalentTo(constants.SchedulerName))
	})

	ginkgo.It("Verifying a pod is created on namespace blacklist", func() {
		ginkgo.By("Create a pod in namespace blacklist")
		podConfigs := common.SleepPodConfig{Name: sleepPodName, NS: blackNs}
		pod, err := kubeClient.CreatePod(common.InitSleepPod(podConfigs), blackNs)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(pod.Spec.SchedulerName).ShouldNot(gomega.BeEquivalentTo(constants.SchedulerName))
	})
	ginkgo.It("Verifying the scheduler configuration is overridden", func() {

		invalidConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.DefaultConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: make(map[string]string),
		}

		res, err := restClient.ValidateSchedulerConfig(invalidConfigMap)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(res.Allowed).Should(gomega.BeEquivalentTo(false))
	})

	ginkgo.AfterEach(func() {
		// call the healthCheck api to check scheduler health
		ginkgo.By("Check YuniKorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(checks).Should(gomega.Equal(""), checks)
	})
})
