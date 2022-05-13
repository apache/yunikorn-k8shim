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
	tests "github.com/apache/yunikorn-k8shim/test"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

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
	var annotation = "ann-" + common.RandSeq(10)
	var oldConfigMap = new(v1.ConfigMap)

	// Define sleepPod
	sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev}

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		gomega.Ω(kClient.SetClient()).To(gomega.BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}

		By("Port-forward the scheduler pod")
		err := kClient.PortForwardYkSchedulerPod()
		Ω(err).NotTo(HaveOccurred())

		tests.UpdateConfigMapWrapper(oldConfigMap, "fifo", annotation)

		ginkgo.By("create development namespace")
		ns1, err := kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

		ginkgo.By("Deploy the sleep pod to the development namespace")
		initPod, podErr := k8s.InitSleepPod(sleepPodConfigs)
		gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod, err = kClient.CreatePod(initPod, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			10)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		appsInfo, err = restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
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
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap, ok := allocations["resource"].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(int64(resMap["memory"].(float64))).To(gomega.Equal(mem))
		gomega.Ω(int64(resMap["vcore"].(float64))).To(gomega.Equal(core))
	})

	ginkgo.AfterEach(func() {
		// call the healthCheck api to check scheduler health
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)
	})

	ginkgo.AfterSuite(func() {
		ginkgo.By("Tear down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())

		tests.RestoreConfigMapWrapper(oldConfigMap, annotation)
	})
})

//func TestIntMinBasic(t *testing.T) {
//	var restClient yunikorn.RClient
//
//	qInfo, qErr := restClient.GetQueue2("default", "root")
//	if qErr != nil {
//		print(qInfo)
//	}
//
//	pInfo, pErr := restClient.GetPartitions()
//	if pErr != nil {
//		print(pInfo)
//	}
//}
