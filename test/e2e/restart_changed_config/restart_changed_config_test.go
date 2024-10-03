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

package restartchangedconfig_test

import (
	"runtime"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var (
	suiteName        string
	kClient          k8s.KubeCtl
	restClient       yunikorn.RClient
	oldConfigMap     = new(v1.ConfigMap)
	dev              = "dev-" + common.RandSeq(5)
	test             = "test-" + common.RandSeq(5)
	sleepPodConfigs  = k8s.SleepPodConfig{Name: "sleepjob", NS: dev}
	sleepPod2Configs = k8s.SleepPodConfig{Name: "sleepjob2", NS: test}
)

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}

	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.BeforeEach(func() {
	ginkgo.By("create development namespace")
	ns, err := kClient.CreateNamespace(dev, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	ns, err = kClient.CreateNamespace(test, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterSuite(func() {
	// call the healthCheck api to check scheduler health
	ginkgo.By("Check YuniKorn's health")
	checks, err2 := yunikorn.GetFailedHealthChecks()
	Ω(err2).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)

	ginkgo.By("Restoring the old config maps")
	var c, err1 = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.DefaultYuniKornConfigMap)
	Ω(err1).NotTo(gomega.HaveOccurred())
	Ω(c).NotTo(gomega.BeNil())
	c.Data = oldConfigMap.Data
	var e, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(gomega.HaveOccurred())
	Ω(e).NotTo(gomega.BeNil())
})

var _ = ginkgo.Describe("PodInRecoveryQueue", func() {
	ginkgo.It("Pod_Restored_In_Recovery_Queue", func() {
		ginkgo.By("Deploy 1st sleep pod to the dev namespace")
		podConf, podErr := k8s.InitSleepPod(sleepPodConfigs)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		podDev, err := kClient.CreatePod(podConf, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodRunning(dev, podDev.Name, 1*time.Minute)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Deploy 2nd sleep pod to the test namespace")
		podConf, podErr = k8s.InitSleepPod(sleepPod2Configs)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		var podTest *v1.Pod
		podTest, err = kClient.CreatePod(podConf, test)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodRunning(test, podTest.Name, 1*time.Minute)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fifo", func(sc *configs.SchedulerConfig) error {
			// remove placement rules: only allow dev queue
			sc.Partitions[0].PlacementRules = []configs.PlacementRule{{
				Name:   types.Tag,
				Value:  "namespace",
				Create: false,
			}}

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:   dev,
				Parent: false,
			}); err != nil {
				return err
			}
			return nil
		})

		ginkgo.By("Check pod in the dev namespace")
		var appsInfo *dao.ApplicationDAOInfo
		appsInfo, err = restClient.GetAppInfo("default", "root."+dev, podDev.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())

		ginkgo.By("Check pod in the test namespace")
		var appsInfo2 *dao.ApplicationDAOInfo
		appsInfo2, err = restClient.GetAppInfo("default", "root."+test, podTest.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo2).NotTo(gomega.BeNil())

		ginkgo.By("Restart the scheduler pod")
		yunikorn.RestartYunikorn(&kClient)
		ginkgo.By("Port-forward scheduler pod after restart")
		yunikorn.RestorePortForwarding(&kClient)

		// in case of the plugin startup takes some time, wait until we see the partition
		ginkgo.By("Check shim registration")
		err = restClient.WaitForRegistration("default", 5)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Check pod in the dev namespace")
		appsInfo, err = restClient.GetAppInfo("default", "root."+dev, podDev.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())

		ginkgo.By("Check pod in the test namespace: recovery queue")
		appsInfo2, err = restClient.GetAppInfo("default", "", podTest.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo2).NotTo(gomega.BeNil())
		gomega.Ω(appsInfo2.QueueName).Should(gomega.BeEquivalentTo("root.@recovery@"))
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev, test})
		ginkgo.By("Tear down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Tear down namespace: " + test)
		err = kClient.TearDownNamespace(test)
		Ω(err).NotTo(gomega.HaveOccurred())
	})
})
