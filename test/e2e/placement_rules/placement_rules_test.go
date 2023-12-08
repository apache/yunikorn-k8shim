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

package placement_rules

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var annotation = "ann-" + common.RandSeq(10)
var oldConfigMap = new(v1.ConfigMap)

var _ = BeforeSuite(func() {
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	By("Port-forward the scheduler pod")
	err := kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(HaveOccurred())

	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "", annotation)
})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

var _ = Describe("Placement_rules", func() {
	var ns string

	BeforeEach(func() {
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("Creating namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	AfterEach(func() {
		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())

		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		// call the healthCheck api to check scheduler health
		By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(Equal(""), checks)
	})

	It("Verify_provied_rule_with_tag_namespace_rule_with_default_queue", func() {
		By("Update placement rule to ConfigMap with admission controller default queue")
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", "", func(sc *configs.SchedulerConfig) error {
			sc.Partitions[0].PlacementRules = []configs.PlacementRule{
				{
					Name:   "provided",
					Create: true,
				},
				{
					Name:   "tag",
					Value:  "namespace",
					Create: true,
				}}
			return nil
		})

		By("Deploy the sleep pod to the development namespace")
		initPod, podErr := k8s.InitSleepPod(k8s.SleepPodConfig{NS: ns})
		Ω(podErr).NotTo(HaveOccurred())
		sleepRespPod, err := kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodRunning(ns, initPod.Name, 30*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Check application is under default queue")
		appsInfo, err := restClient.GetAppInfo("default", conf.DefaultFilteringQueueName, sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appsInfo).NotTo(BeNil())
		Ω(appsInfo.QueueName).To(Equal(conf.DefaultFilteringQueueName))
	})

	It("Verify_provied_rule_with_tag_namespace_rule_without_default_queue", func() {
		By("Update placement rule to ConfigMap without admission controller default queue")
		amConf := map[string]string{"admissionController.filtering.defaultQueue": ""}
		yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", "", amConf, func(sc *configs.SchedulerConfig) error {
			sc.Partitions[0].PlacementRules = []configs.PlacementRule{
				{
					Name:   "provided",
					Create: true,
				},
				{
					Name:   "tag",
					Value:  "namespace",
					Create: true,
				}}
			return nil
		})

		By("Deploy the sleep pod to the development namespace")
		initPod, podErr := k8s.InitSleepPod(k8s.SleepPodConfig{NS: ns})
		Ω(podErr).NotTo(HaveOccurred())
		sleepRespPod, err := kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodRunning(ns, initPod.Name, 30*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Check application is under queue generated by tag placement rule")
		appsInfo, err := restClient.GetAppInfo("default", "root."+ns, sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appsInfo).NotTo(BeNil())
		Ω(appsInfo.QueueName).To(Equal("root." + ns))
	})
})
