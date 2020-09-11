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

package basicscheduling_test

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
)

var _ = ginkgo.Describe("", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var sleepRespPod *v1.Pod
	var dev = "dev" + common.RandSeq(5)
	var appsInfo map[string]interface{}
	var r = regexp.MustCompile(`memory:(\d+) vcore:(\d+)`)

	// Define sleepPod
	sleepPodConfigs := common.SleepPodConfig{Name: "sleepjob", NS: dev}

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		gomega.Ω(kClient.SetClient()).To(gomega.BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}

		By("Enable basic scheduling config over config maps")
		var c, err = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(err).NotTo(HaveOccurred())
		Ω(c).NotTo(BeNil())

		oldConfigMap = c.DeepCopy()
		Ω(c).Should(BeEquivalentTo(oldConfigMap))

		// Define basic configMap
		configStr, err2 := common.CreateBasicConfigMap().ToYAML()
		Ω(err2).NotTo(HaveOccurred())

		c.Data[configmanager.DefaultPolicyGroup] = configStr
		var d, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err3).NotTo(HaveOccurred())
		Ω(d).NotTo(BeNil())

		ginkgo.By("create development namespace")
		ns1, err := kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

		ginkgo.By("Deploy the sleep pod to the development namespace")
		sleepRespPod, err = kClient.CreatePod(common.InitSleepPod(sleepPodConfigs), dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		//Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			10)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		appsInfo, err = restClient.GetAppInfo(sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
	})

	ginkgo.It("Verify_App_Queue_Info", func() {
		ginkgo.By("Verify that the sleep pod is mapped to development queue")
		gomega.Ω(appsInfo["applicationID"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		gomega.Ω(appsInfo["queueName"]).To(gomega.ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
	})

	ginkgo.It("Verify_Job_State", func() {
		ginkgo.By("Verify that the job is scheduled & starting by YuniKorn")
		gomega.Ω(appsInfo["applicationState"]).To(gomega.Equal("Starting"))
		gomega.Ω("yunikorn").To(gomega.Equal(sleepRespPod.Spec.SchedulerName))
	})

	ginkgo.It("Verify_Pod_Alloc_Props", func() {
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo["allocations"]).NotTo(gomega.BeNil())
		allocations, ok := appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(allocations["allocationKey"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["nodeId"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["partition"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["uuid"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["applicationId"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		gomega.Ω(allocations["queueName"]).To(gomega.ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
		core := strconv.FormatInt(sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue(), 10)
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().String()
		matches := r.FindStringSubmatch(allocations["resource"].(string))
		gomega.Ω(matches[1] + "M").To(gomega.Equal(mem))
		gomega.Ω(matches[2]).To(gomega.ContainSubstring(core))
	})

	ginkgo.AfterSuite(func() {
		ginkgo.By("Tear down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())

		By("Restoring the old config maps")
		var c, err1 = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(err1).NotTo(HaveOccurred())
		Ω(c).NotTo(BeNil())
		c.Data = oldConfigMap.Data
		var e, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err3).NotTo(HaveOccurred())
		Ω(e).NotTo(BeNil())

	})
})
