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
	"runtime"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var sleepRespPod *v1.Pod
var appsInfo *dao.ApplicationDAOInfo
var oldConfigMap = new(v1.ConfigMap)

// Define sleepPod
var sleepPodConfigs = k8s.SleepPodConfig{Name: "sleepjob", NS: dev}

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	gomega.Ω(kClient.SetClient()).To(gomega.Succeed())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	yunikorn.EnsureYuniKornConfigsPresent()
	By("Port-forward the scheduler pod")
	err := kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(HaveOccurred())
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo")
})
var _ = ginkgo.BeforeEach(func() {
	dev = "dev" + common.RandSeq(5)
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
	err = kClient.WaitForPodRunning(dev, sleepPodConfigs.Name, 30*time.Second)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	appsInfo, err = restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(appsInfo).NotTo(gomega.BeNil())
})
var _ = ginkgo.AfterEach(func() {
	ginkgo.By("Tear down namespace: " + dev)
	err := kClient.TearDownNamespace(dev)
	Ω(err).NotTo(HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

var _ = ginkgo.Describe("", func() {

	ginkgo.It("Verify_App_Queue_Info", func() {
		ginkgo.By("Verify that the sleep pod is mapped to development queue")
		gomega.Ω(appsInfo.ApplicationID).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		gomega.Ω(appsInfo.QueueName).To(gomega.ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
	})

	ginkgo.It("Verify_Job_State", func() {
		ginkgo.By("Verify that the job is scheduled & running by YuniKorn")
		gomega.Ω(appsInfo.State).To(gomega.Equal("Running"))
		gomega.Ω("yunikorn").To(gomega.Equal(sleepRespPod.Spec.SchedulerName))
	})

	ginkgo.It("Verify_Pod_Alloc_Props", func() {
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo.Allocations).NotTo(gomega.BeEmpty())
		allocation := appsInfo.Allocations[0]
		gomega.Ω(allocation).NotTo(gomega.BeNil())
		gomega.Ω(allocation.AllocationKey).NotTo(gomega.BeNil())
		gomega.Ω(allocation.NodeID).NotTo(gomega.BeNil())
		gomega.Ω(allocation.ApplicationID).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap := allocation.ResourcePerAlloc
		Ω(len(resMap)).NotTo(gomega.BeZero())
		Ω(resMap["memory"]).To(gomega.Equal(mem))
		Ω(resMap["vcore"]).To(gomega.Equal(core))
	})

	ginkgo.It("Verify_BestEffort_QOS_Pod_Scheduling", func() {
		ginkgo.By("Create a pod with QOS class set to BestEffort")
		bestEffortPodConfig := k8s.SleepPodConfig{Name: "besteffortpod", NS: dev, QOSClass: v1.PodQOSBestEffort}
		initPod, podErr := k8s.InitSleepPod(bestEffortPodConfig)
		gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		bestEffortPod, err := kClient.CreatePod(initPod, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for the pod to move to running state")
		err = kClient.WaitForPodRunning(dev, bestEffortPodConfig.Name, 30*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the pod is scheduled and running")
		appsInfo, err = restClient.GetAppInfo("default", "root."+dev, bestEffortPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		gomega.Ω(appsInfo.State).To(gomega.Equal("Running"))

		ginkgo.By("Verify that the pod's QOS class is BestEffort")
		gomega.Ω(bestEffortPod.Status.QOSClass).To(gomega.Equal(v1.PodQOSBestEffort))

		ginkgo.By("Verify that the pod's scheduler name is yunikorn")
		gomega.Ω("yunikorn").To(gomega.Equal(bestEffortPod.Spec.SchedulerName))
		allocation := appsInfo.Allocations[0]
		gomega.Ω(allocation).NotTo(gomega.BeNil())
		gomega.Ω(allocation.AllocationKey).NotTo(gomega.BeNil())
		gomega.Ω(allocation.NodeID).NotTo(gomega.BeNil())
		gomega.Ω(allocation.ApplicationID).To(gomega.Equal(bestEffortPod.ObjectMeta.Labels["applicationId"]))
		core := bestEffortPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := bestEffortPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap := allocation.ResourcePerAlloc
		Ω(len(resMap)).NotTo(gomega.BeZero())
		Ω(resMap["memory"]).To(gomega.Equal(mem))
		Ω(resMap["vcore"]).To(gomega.Equal(core))
	})

	ginkgo.It("Verify_NonBestEffort_QOS_Pod_Scheduling", func() {
		ginkgo.By("Create a pod with QOS class set to Burstable")
		burstablePodConfig := k8s.SleepPodConfig{Name: "burstablepod", NS: dev, QOSClass: v1.PodQOSBurstable}
		initPod, podErr := k8s.InitSleepPod(burstablePodConfig)
		gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		burstablePod, err := kClient.CreatePod(initPod, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for the pod to move to running state")
		err = kClient.WaitForPodRunning(dev, burstablePodConfig.Name, 30*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify that the pod is scheduled and running")
		appsInfo, err = restClient.GetAppInfo("default", "root."+dev, burstablePod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		gomega.Ω(appsInfo.State).To(gomega.Equal("Running"))

		ginkgo.By("Verify that the pod's QOS class is not BestEffort")
		gomega.Ω(burstablePod.Status.QOSClass).NotTo(gomega.Equal(v1.PodQOSBestEffort))

		ginkgo.By("Verify that the pod's scheduler name is yunikorn")
		gomega.Ω("yunikorn").To(gomega.Equal(burstablePod.Spec.SchedulerName))
		allocation := appsInfo.Allocations[0]
		gomega.Ω(allocation).NotTo(gomega.BeNil())
		gomega.Ω(allocation.AllocationKey).NotTo(gomega.BeNil())
		gomega.Ω(allocation.NodeID).NotTo(gomega.BeNil())
		gomega.Ω(allocation.ApplicationID).To(gomega.Equal(burstablePod.ObjectMeta.Labels["applicationId"]))
		core := burstablePod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := burstablePod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap := allocation.ResourcePerAlloc
		Ω(len(resMap)).NotTo(gomega.BeZero())
		Ω(resMap["memory"]).To(gomega.Equal(mem))
		Ω(resMap["vcore"]).To(gomega.Equal(core))
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
		// call the healthCheck api to check scheduler health
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)
	})
})
