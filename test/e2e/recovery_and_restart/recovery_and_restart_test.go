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
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	gangSleepJobPrefix   = "gang-sleep-job"
	normalSleepJobPrefix = "normal-sleep-job"
	taskGroupA           = "groupa"
	taskGroupB           = "groupb"
	taskGroupAprefix     = "tg-" + taskGroupA + "-" + gangSleepJobPrefix
	taskGroupBprefix     = "tg-" + taskGroupB + "-" + gangSleepJobPrefix
	taskGroupE2E         = "e2e-task-group"
	taskGroupE2EPrefix   = "tg-" + taskGroupE2E
	parallelism          = 3
	taintKey             = "e2e_test"
)

var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)
var sleepRespPod *v1.Pod
var dev = "dev" + common.RandSeq(5)
var annotation = "ann-" + common.RandSeq(10)

// Define sleepPod
var sleepPodConfigs = k8s.SleepPodConfig{Name: "sleepjob", NS: dev}
var sleepPod2Configs = k8s.SleepPodConfig{Name: "sleepjob2", NS: dev}

var _ = ginkgo.BeforeSuite(func() {
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}

	annotation = "ann-" + common.RandSeq(10)
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "", annotation)

	ginkgo.By("create development namespace")
	ns1, err := kClient.CreateNamespace(dev, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

	ginkgo.By("Deploy the sleep pod to the development namespace")
	sleepObj, podErr := k8s.InitSleepPod(sleepPodConfigs)
	Ω(podErr).NotTo(gomega.HaveOccurred())
	sleepRespPod, err = kClient.CreatePod(sleepObj, dev)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	// Wait for pod to move to running state
	err = kClient.WaitForPodBySelectorRunning(dev,
		fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
		60)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Restart the scheduler pod")
	yunikorn.RestartYunikorn(&kClient)

	ginkgo.By("Port-forward scheduler pod after restart")
	yunikorn.RestorePortForwarding(&kClient)

	ginkgo.By("Deploy 2nd sleep pod to the development namespace")
	sleepObj2, podErr := k8s.InitSleepPod(sleepPod2Configs)
	Ω(podErr).NotTo(gomega.HaveOccurred())
	sleepRespPod2, err := kClient.CreatePod(sleepObj2, dev)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	// Wait for pod to move to running state
	err = kClient.WaitForPodBySelectorRunning(dev,
		fmt.Sprintf("app=%s", sleepRespPod2.ObjectMeta.Labels["app"]),
		60)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {
	// call the healthCheck api to check scheduler health
	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)

	ginkgo.By("Tear down namespace: " + dev)
	err = kClient.TearDownNamespace(dev)
	Ω(err).NotTo(gomega.HaveOccurred())

	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

var _ = ginkgo.Describe("", func() {

	ginkgo.It("Verify_Pod_Alloc_Props", func() {
		appsInfo, err := restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		gomega.Ω(len(appsInfo.Allocations)).NotTo(gomega.BeZero())
		allocations := appsInfo.Allocations[0]
		gomega.Ω(allocations).NotTo(gomega.BeNil())
		gomega.Ω(allocations.AllocationKey).NotTo(gomega.BeNil())
		gomega.Ω(allocations.NodeID).NotTo(gomega.BeNil())
		gomega.Ω(allocations.Partition).NotTo(gomega.BeNil())
		gomega.Ω(allocations.UUID).NotTo(gomega.BeNil())
		gomega.Ω(allocations.ApplicationID).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap := allocations.ResourcePerAlloc
		gomega.Ω(len(resMap)).NotTo(gomega.BeZero())
		gomega.Ω(resMap["memory"]).To(gomega.Equal(mem))
		gomega.Ω(resMap["vcore"]).To(gomega.Equal(core))
	})

	ginkgo.It("Verify_SleepJobs_Restart_YK", func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		defer yunikorn.RestorePortForwarding(&kClient)

		appID1 := normalSleepJobPrefix + "-" + common.RandSeq(5)
		sleepPodConfig1 := k8s.SleepPodConfig{Name: "normal-sleep-job", NS: dev, Time: 300, AppID: appID1}
		pod1, podErr := k8s.InitSleepPod(sleepPodConfig1)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		appID2 := normalSleepJobPrefix + "-" + common.RandSeq(5)
		sleepPodConfig2 := k8s.SleepPodConfig{Name: "normal-sleep-job-2", NS: dev, Time: 300, AppID: appID2}
		pod2, podErr2 := k8s.InitSleepPod(sleepPodConfig2)
		Ω(podErr2).NotTo(gomega.HaveOccurred())

		ginkgo.By("Submitting two normal sleep jobs")
		job1 := k8s.InitTestJob(appID1, parallelism, parallelism, pod1)
		_, createErr := kClient.CreateJob(job1, dev)
		Ω(createErr).NotTo(gomega.HaveOccurred())
		job2 := k8s.InitTestJob(appID2, parallelism, parallelism, pod2)
		_, createErr2 := kClient.CreateJob(job2, dev)
		Ω(createErr2).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart the scheduler pod immediately")
		yunikorn.RestartYunikorn(&kClient)

		ginkgo.By("Listing pods")
		pods, err := kClient.GetPods(dev)
		Ω(err).NotTo(gomega.HaveOccurred())
		fmt.Fprintf(ginkgo.GinkgoWriter, "Total number of pods in namespace %s: %d\n",
			dev, len(pods.Items))
		for _, pod := range pods.Items {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Pod name: %-40s\tStatus: %s\n", pod.GetName(), pod.Status.Phase)
		}

		ginkgo.By("Waiting for sleep pods to be running")
		err = kClient.WaitForJobPodsRunning(dev, job1.Name, parallelism, 60*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForJobPodsRunning(dev, job2.Name, parallelism, 60*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting sleep jobs")
		err = kClient.DeleteJob(job1.Name, dev)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeleteJob(job2.Name, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting sleep pods")
		sleep1Pods, err2 := kClient.ListPods(dev, "applicationId="+sleepPodConfig1.AppID)
		Ω(err2).NotTo(gomega.HaveOccurred())
		sleep2Pods, err3 := kClient.ListPods(dev, "applicationId="+sleepPodConfig2.AppID)
		Ω(err3).NotTo(gomega.HaveOccurred())

		sleepPods := make([]v1.Pod, 0)
		sleepPods = append(sleepPods, sleep1Pods.Items...)
		sleepPods = append(sleepPods, sleep2Pods.Items...)

		for _, pod := range sleepPods {
			podName := pod.GetName()
			err := kClient.DeletePod(podName, dev)
			Ω(err).NotTo(gomega.HaveOccurred())
			fmt.Fprintf(ginkgo.GinkgoWriter, "Deleted pod %s\n", podName)
		}
	})

	ginkgo.It("Verify_GangScheduling_TwoGangs_Restart_YK", func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		defer yunikorn.RestorePortForwarding(&kClient)

		appID := gangSleepJobPrefix + "-" + common.RandSeq(5)
		sleepPodConfig := k8s.SleepPodConfig{Name: "gang-sleep-job", NS: dev, Time: 1, AppID: appID}
		taskGroups := k8s.InitTaskGroups(sleepPodConfig, taskGroupA, taskGroupB, parallelism)
		pod, podErr := k8s.InitSleepPod(sleepPodConfig)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		pod = k8s.DecoratePodForGangScheduling(30, "Soft", taskGroupA,
			taskGroups, pod)

		ginkgo.By("Submitting gang sleep job")
		job := k8s.InitTestJob(appID, parallelism, parallelism, pod)
		_, err := kClient.CreateJob(job, dev)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting job pods to be created")
		createErr := kClient.WaitForJobPodsCreated(dev, job.Name, parallelism, 30*time.Second)
		Ω(createErr).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for placeholders in task group A (expected state: Running)")
		err = kClient.WaitForPlaceholders(dev, taskGroupAprefix, parallelism, 30*time.Second, v1.PodRunning)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for placeholders in task group B (expected state: Pending)")
		err = kClient.WaitForPlaceholders(dev, taskGroupBprefix, parallelism+1, 30*time.Second, v1.PodPending)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart the scheduler pod")
		yunikorn.RestartYunikorn(&kClient)

		// make sure that Yunikorn's internal state have been properly restored
		ginkgo.By("Submit sleep job")
		sleepJob3AppID := "sleepjob-" + common.RandSeq(5)
		sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, AppID: sleepJob3AppID}
		sleepPod, podErr2 := k8s.InitSleepPod(sleepPod3Configs)
		Ω(podErr2).NotTo(gomega.HaveOccurred())
		sleepRespPod, err = kClient.CreatePod(sleepPod, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("applicationId=%s", sleepJob3AppID),
			60)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// After YK is up again, we expect to see the same pods in the same state
		ginkgo.By("Verify the number of pods & their status")
		pods, err2 := kClient.GetPods(dev)
		Ω(err2).NotTo(gomega.HaveOccurred())
		var groupAPlaceholderCount int
		var groupBPlaceholderCount int
		var jobPodCount int
		fmt.Fprintf(ginkgo.GinkgoWriter, "Total number of pods in namespace %s: %d\n",
			dev, len(pods.Items))
		for _, pod := range pods.Items {
			podPhase := pod.Status.Phase
			podName := pod.GetName()
			fmt.Fprintf(ginkgo.GinkgoWriter, "Pod name: %-40s\tStatus: %s\n", podName, podPhase)
			if strings.HasPrefix(podName, taskGroupAprefix) {
				groupAPlaceholderCount++
				Ω(podPhase).To(gomega.Equal(v1.PodRunning))
				continue
			}
			if strings.HasPrefix(podName, taskGroupBprefix) {
				groupBPlaceholderCount++
				Ω(podPhase).To(gomega.Equal(v1.PodPending))
				continue
			}
			if strings.HasPrefix(podName, gangSleepJobPrefix) {
				jobPodCount++
				Ω(podPhase).To(gomega.Equal(v1.PodPending))
				continue
			}
		}
		Ω(groupAPlaceholderCount).To(gomega.Equal(parallelism))
		Ω(groupBPlaceholderCount).To(gomega.Equal(parallelism + 1))

		// Wait for placeholder timeout & replacement to real pod
		ginkgo.By("Waiting for placeholder timeout & sleep pods to finish")
		err = kClient.WaitForJobPodsSucceeded(dev, job.Name, parallelism, 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_GangScheduling_PendingPlaceholders_Restart_YK", func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		defer yunikorn.RestorePortForwarding(&kClient)

		ginkgo.By("Retrieving allocatable memory of worker nodes")
		nodes, err := kClient.GetNodes()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Items cannot be empty")

		var worker2Res *resource.Quantity
		foundWorker := false
		for _, node := range nodes.Items {
			if node.Name == common.KindWorker2 {
				worker2Res = node.Status.Allocatable.Memory()
				foundWorker = true
				break
			}
		}
		Ω(foundWorker).To(gomega.Equal(true), "worker node not found")

		memoryGiB := worker2Res.ScaledValue(resource.Giga)
		placeholderCount := int32(memoryGiB/3 + 2) // get a reasonable number to have both Running/Pending PH pods
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s allocatable memory in GiB = %d, number of placeholders to use = %d\n",
			common.KindWorker2, memoryGiB, placeholderCount)

		ginkgo.By("Tainting worker node #2")
		err = kClient.TaintNode(common.KindWorker2, taintKey, "value", v1.TaintEffectNoSchedule)
		Ω(err).NotTo(gomega.HaveOccurred())
		removeTaint := true
		defer func() {
			if removeTaint {
				ginkgo.By("Untainting worker node #2 (defer)")
				err = kClient.UntaintNode(common.KindWorker2, taintKey)
				Ω(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Submitting gang job")
		appID := gangSleepJobPrefix + "-" + common.RandSeq(5)
		sleepPodConfig := k8s.SleepPodConfig{Name: "gang-sleep-job", NS: dev, Time: 1, AppID: appID, Mem: 3000, CPU: 10}
		taskGroups := k8s.InitTaskGroup(sleepPodConfig, taintKey, placeholderCount)
		pod, podErr := k8s.InitSleepPod(sleepPodConfig)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		pod = k8s.DecoratePodForGangScheduling(900, "Soft", taskGroupE2E,
			taskGroups, pod)
		job := k8s.InitTestJob(appID, 1, 1, pod)
		_, createErr := kClient.CreateJob(job, dev)
		Ω(createErr).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for placeholders to be Running/Pending")
		err = kClient.WaitForPlaceholdersStableState(dev, taskGroupE2EPrefix, 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Restart the scheduler pod")
		yunikorn.RestartYunikorn(&kClient)

		ginkgo.By("Untainting worker node #2")
		err = kClient.UntaintNode(common.KindWorker2, taintKey)
		removeTaint = false
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for placeholder replacement & sleep pods to finish")
		err = kClient.WaitForJobPodsSucceeded(dev, job.Name, 1, 60*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
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
