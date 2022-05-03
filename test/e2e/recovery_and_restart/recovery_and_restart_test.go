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

package recoveryandrestart_test

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var oldConfigMap *v1.ConfigMap
	var sleepRespPod *v1.Pod
	var dev = "dev" + common.RandSeq(5)

	// Define sleepPod
	sleepPodConfigs := common.SleepPodConfig{Name: "sleepjob", NS: dev}
	sleepPod2Configs := common.SleepPodConfig{Name: "sleepjob2", NS: dev}

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}

		ginkgo.By("Enable basic scheduling config over config maps")
		var c, err = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(c).NotTo(gomega.BeNil())

		oldConfigMap = c.DeepCopy()
		Ω(c).Should(gomega.BeEquivalentTo(oldConfigMap))

		// Define basic configMap
		configStr, err2 := common.CreateBasicConfigMap().ToYAML()
		Ω(err2).NotTo(gomega.HaveOccurred())

		c.Data[configmanager.DefaultPolicyGroup] = configStr
		var d, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err3).NotTo(gomega.HaveOccurred())
		Ω(d).NotTo(gomega.BeNil())

		ginkgo.By("create development namespace")
		ns1, err := kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

		ginkgo.By("Deploy the sleep pod to the development namespace")
		sleepRespPod, err = kClient.CreatePod(common.InitSleepPod(sleepPodConfigs), dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			60)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart the scheduler pod")
		schedulerPodName, err := kClient.GetSchedulerPod()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeletePodGracefully(schedulerPodName, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(configmanager.YuniKornTestConfig.YkNamespace, fmt.Sprintf("component=%s", configmanager.YKScheduler), 10)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Port-forward scheduler pod after restart")
		// kill running kubectl port-forward process if it exists
		kClient.KillPortForwardProcess()
		// port-forward the scheduler pod
		err = kClient.PortForwardYkSchedulerPod()
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy 2nd sleep pod to the development namespace")
		sleepRespPod2, err := kClient.CreatePod(common.InitSleepPod(sleepPod2Configs), dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod2.ObjectMeta.Labels["app"]),
			60)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Pod_Alloc_Props", func() {
		appsInfo, err := restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo["allocations"]).NotTo(gomega.BeNil())
		allocations, ok := appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(allocations["allocationKey"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["nodeId"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["partition"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["uuid"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["applicationId"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap, ok := allocations["resource"].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(int64(resMap["memory"].(float64))).To(gomega.Equal(mem))
		gomega.Ω(int64(resMap["vcore"].(float64))).To(gomega.Equal(core))
	})

	ginkgo.AfterSuite(func() {
		// call the healthCheck api to check scheduler health
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)

		ginkgo.By("Tear down namespace: " + dev)
		err = kClient.TearDownNamespace(dev)
		Ω(err).NotTo(gomega.HaveOccurred())

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
})
