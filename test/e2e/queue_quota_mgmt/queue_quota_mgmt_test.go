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

package queuequotamgmt_test

import (
	"fmt"
	"math"
	"time"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"

	v1 "k8s.io/api/core/v1"
)

var _ = Describe("", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var sleepRespPod *v1.Pod
	var maxCPUAnnotation = "yunikorn.apache.org/namespace.max.cpu"
	var maxMemAnnotation = "yunikorn.apache.org/namespace.max.memory"
	var quotaAnnotation = "yunikorn.apache.org/namespace.quota"
	var oldAnnotations map[string]string
	var expectedOldAnnotations map[string]int64
	var newAnnotations map[string]string
	var expectedNewAnnotations map[string]int64
	var oldAndNewAnnotations map[string]string
	var expectedOldAndNewAnnotations map[string]int64
	var emptyOldAnnotations map[string]string
	var wrongOldAnnotations map[string]string
	var wrongNewAnnotations map[string]string
	var ns string
	var queueInfo *dao.PartitionQueueDAOInfo
	var maxResource yunikorn.ResourceUsage
	var usedResource yunikorn.ResourceUsage
	var usedPerctResource yunikorn.ResourceUsage
	var pods []string

	BeforeEach(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}
		pods = []string{}
		oldAnnotations = map[string]string{}
		newAnnotations = map[string]string{}
		oldAndNewAnnotations = map[string]string{}
		emptyOldAnnotations = map[string]string{}
		wrongOldAnnotations = map[string]string{}
		wrongNewAnnotations = map[string]string{}
		expectedOldAnnotations = map[string]int64{}
		expectedNewAnnotations = map[string]int64{}
		expectedOldAndNewAnnotations = map[string]int64{}
		oldAnnotations[maxCPUAnnotation] = "300m"
		oldAnnotations[maxMemAnnotation] = "300M"
		newAnnotations[quotaAnnotation] = "{\"cpu\": \"400m\", \"memory\": \"400M\", \"nvidia.com/gpu\": \"1\"}"
		oldAndNewAnnotations[maxCPUAnnotation] = "300m"
		oldAndNewAnnotations[maxMemAnnotation] = "300M"
		oldAndNewAnnotations[quotaAnnotation] = "{\"cpu\": \"600m\", \"memory\": \"600M\", \"nvidia.com/gpu\": \"2\"}"
		emptyOldAnnotations[maxCPUAnnotation] = "nil"
		emptyOldAnnotations[maxMemAnnotation] = "nil"
		wrongOldAnnotations[maxCPUAnnotation] = "a"
		wrongOldAnnotations[maxMemAnnotation] = "b"
		wrongNewAnnotations[quotaAnnotation] = "{\"cpu\": \"error\", \"memory\": \"error\"}"
		expectedOldAnnotations[siCommon.CPU] = 300
		expectedOldAnnotations[siCommon.Memory] = 300
		expectedNewAnnotations[siCommon.CPU] = 400
		expectedNewAnnotations[siCommon.Memory] = 400
		expectedNewAnnotations["nvidia.com/gpu"] = 1
		expectedOldAndNewAnnotations[siCommon.CPU] = 600
		expectedOldAndNewAnnotations[siCommon.Memory] = 600
		expectedOldAndNewAnnotations["nvidia.com/gpu"] = 2
	})

	/*
		1. Create a namespace with different types of annotations set to max CPU, max Mem, max GPU etc
		2. Submit pod with CPU and Mem requests
		3. Verify the CPU and memory usage reported by Yunikorn is accurate.
		4. Submit another pod with resource request that exceeds the limits
		5. Verify the pod is in un-schedulable state
		6. Wait until pod-1 completes and verify that pod-2 is scheduled now.
	*/
	verifyQuotaAllocationUsingAnnotation := func(annotations map[string]string, resources map[string]int64, podCount int64) {
		maxCPU := resources[siCommon.CPU]
		maxMem := resources[siCommon.Memory]
		maxGPU := int64(0)
		ok := false
		if maxGPU, ok = resources["nvidia.com/gpu"]; ok {
			maxGPU = resources["nvidia.com/gpu"]
		}

		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace with maxCPU: %dm and maxMem: %dM", ns, maxCPU, maxMem))
		ns1, err1 := kClient.CreateNamespace(ns, annotations)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		// Create sleep pod template
		var reqCPU int64 = 100
		var reqMem int64 = 100

		var iter int64
		nextPod := podCount + 1
		for iter = 1; iter <= podCount; iter++ {
			sleepPodConfigs := k8s.SleepPodConfig{NS: ns, Time: 60, CPU: reqCPU, Mem: reqMem}
			sleepObj, podErr := k8s.InitSleepPod(sleepPodConfigs)
			Ω(podErr).NotTo(HaveOccurred())

			pods = append(pods, sleepObj.Name)
			By(fmt.Sprintf("App-%d: Deploy the sleep app:%s to %s namespace", iter, sleepObj.Name, ns))
			sleepRespPod, err = kClient.CreatePod(sleepObj, ns)
			Ω(err).NotTo(HaveOccurred())

			Ω(kClient.WaitForPodRunning(sleepRespPod.Namespace, sleepRespPod.Name, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

			// Verify that the resources requested by above sleep pod is accounted for in the queues response
			queueInfo, err = restClient.GetSpecificQueueInfo("default", "root."+ns)
			Ω(err).NotTo(HaveOccurred())
			Ω(queueInfo).NotTo(BeNil())
			Ω(queueInfo.QueueName).Should(Equal("root." + ns))
			Ω(queueInfo.Status).Should(Equal("Active"))
			Ω(queueInfo.Properties).Should(BeEmpty())
			maxResource.ParseResourceUsage(queueInfo.MaxResource)
			usedResource.ParseResourceUsage(queueInfo.AllocatedResource)
			usedPerctResource.ParseResourceUsage(queueInfo.AbsUsedCapacity)

			By(fmt.Sprintf("App-%d: Verify max capacity on the queue is accurate", iter))
			Ω(maxResource.GetResourceValue(siCommon.CPU)).Should(Equal(maxCPU))
			Ω(maxResource.GetResourceValue(siCommon.Memory)).Should(Equal(maxMem * 1000 * 1000))
			Ω(maxResource.GetResourceValue("nvidia.com/gpu")).Should(Equal(maxGPU))

			By(fmt.Sprintf("App-%d: Verify used capacity on the queue is accurate after 1st pod deployment", iter))
			Ω(usedResource.GetResourceValue(siCommon.CPU)).Should(Equal(reqCPU * iter))
			Ω(usedResource.GetResourceValue(siCommon.Memory)).Should(Equal(reqMem * iter * 1000 * 1000))

			var perctCPU = int64(math.Floor((float64(reqCPU*iter) / float64(maxCPU)) * 100))
			var perctMem = int64(math.Floor((float64(reqMem*iter) / float64(maxMem)) * 100))
			By(fmt.Sprintf("App-%d: Verify used percentage capacity on the queue is accurate after 1st pod deployment", iter))
			Ω(usedPerctResource.GetResourceValue(siCommon.CPU)).Should(Equal(perctCPU))
			Ω(usedPerctResource.GetResourceValue(siCommon.Memory)).Should(Equal(perctMem))
		}

		By(fmt.Sprintf("App-%d: Submit another app which exceeds the queue quota limitation", nextPod))
		sleepPodConfigs := k8s.SleepPodConfig{NS: ns, Time: 60, CPU: reqCPU, Mem: reqMem}
		sleepObj, app4PodErr := k8s.InitSleepPod(sleepPodConfigs)
		Ω(app4PodErr).NotTo(HaveOccurred())

		By(fmt.Sprintf("App-%d: Deploy the sleep app:%s to %s namespace", nextPod, sleepObj.Name, ns))
		sleepRespPod, err = kClient.CreatePod(sleepObj, ns)
		Ω(err).NotTo(HaveOccurred())
		pods = append(pods, sleepObj.Name)

		By(fmt.Sprintf("App-%d: Verify app:%s in accepted state", nextPod, sleepObj.Name))
		// Wait for pod to move to accepted state
		err = restClient.WaitForAppStateTransition("default", "root."+ns, sleepRespPod.ObjectMeta.Labels["applicationId"],
			yunikorn.States().Application.Accepted,
			240)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Pod-%d: Verify pod:%s is in pending state", nextPod, sleepObj.Name))
		err = kClient.WaitForPodPending(ns, sleepObj.Name, time.Duration(60)*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("App-1: Wait for 1st app:%s to complete, to make enough capacity to run the last app", pods[0]))
		// Wait for pod to move to accepted state
		err = kClient.WaitForPodSucceeded(ns, pods[0], time.Duration(360)*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Pod-%d: Verify Pod:%s moved to running state", nextPod, sleepObj.Name))
		Ω(kClient.WaitForPodRunning(sleepRespPod.Namespace, sleepRespPod.Name, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

	}

	It("Verify_Queue_Quota_Allocation_Using_Old_Annotations", func() {
		verifyQuotaAllocationUsingAnnotation(oldAnnotations, expectedOldAnnotations, 3)
	})

	It("Verify_Queue_Quota_Allocation_Using_New_Annotations", func() {
		verifyQuotaAllocationUsingAnnotation(newAnnotations, expectedNewAnnotations, 4)
	})

	It("Verify_Queue_Quota_Allocation_Using_Old_New_Annotations", func() {
		verifyQuotaAllocationUsingAnnotation(oldAndNewAnnotations, expectedOldAndNewAnnotations, 6)
	})

	It("Verify_NS_Without_Quota", func() {
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace without quota information", ns))
		ns1, err1 := kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	DescribeTable("", func(annotations map[string]string) {
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace with empty annotations", ns))
		ns1, err1 := kClient.CreateNamespace(ns, annotations)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		// Create sleep pod
		var reqCPU int64 = 300
		var reqMem int64 = 300
		sleepPodConfigs := k8s.SleepPodConfig{NS: ns, Time: 60, CPU: reqCPU, Mem: reqMem}
		sleepObj, podErr := k8s.InitSleepPod(sleepPodConfigs)
		Ω(podErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy the sleep app:%s to %s namespace", sleepObj.Name, ns))
		sleepRespPod, err = kClient.CreatePod(sleepObj, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(sleepRespPod.Namespace, sleepRespPod.Name, time.Duration(60)*time.Second)).NotTo(HaveOccurred())
	},
		Entry("Verify_NS_With_Empty_Quota_Annotations", emptyOldAnnotations),
		Entry("Verify_NS_With_Wrong_Quota_Annotations", wrongOldAnnotations),
	)

	// Hierarchical Queues - Quota enforcement
	// For now, these cases are skipped
	PIt("Verify_Child_Max_Resource_Must_Be_Less_Than_Parent_Max", func() {
		// P2 case - addressed later
	})

	PIt("Verify_Sum_Of_All_Child_Resource_Must_Be_Less_Than_Parent_Max", func() {
		// P2 case - addressed later
	})

	AfterEach(func() {
		By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(Equal(""), checks)

		By("Tearing down namespace: " + ns)
		err = kClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
