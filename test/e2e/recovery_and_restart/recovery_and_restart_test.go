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
	"runtime"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	gangSleepJobPrefix   = "gang-sleep-job"
	normalSleepJobPrefix = "normal-sleep-job"
	taskGroupA           = "groupa"
	taskGroupB           = "groupb"
	taskGroupE2E         = "e2e-task-group"
	taskGroupE2EPrefix   = "tg-" + gangSleepJobPrefix + "-" + taskGroupE2E
	parallelism          = 3
	taintKey             = "e2e_test"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)
var sleepRespPod *v1.Pod

// Define sleepPod
var sleepPodConfigs = k8s.SleepPodConfig{Name: "sleepjob", NS: dev}
var sleepPod2Configs = k8s.SleepPodConfig{Name: "sleepjob2", NS: dev}

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

	ginkgo.By("Restart the scheduler pod")
	yunikorn.RestartYunikorn(&kClient)

	ginkgo.By("Port-forward scheduler pod after restart")
	yunikorn.RestorePortForwarding(&kClient)
})

var _ = ginkgo.BeforeEach(func() {
	dev = "dev" + common.RandSeq(5)
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
		fmt.Sprintf("applicationId=%s", sleepRespPod.ObjectMeta.Labels["applicationId"]),
		60)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Deploy 2nd sleep pod to the development namespace")
	sleepObj2, podErr := k8s.InitSleepPod(sleepPod2Configs)
	Ω(podErr).NotTo(gomega.HaveOccurred())
	sleepRespPod2, err := kClient.CreatePod(sleepObj2, dev)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	// Wait for pod to move to running state
	err = kClient.WaitForPodBySelectorRunning(dev,
		fmt.Sprintf("applicationId=%s", sleepRespPod2.ObjectMeta.Labels["applicationId"]),
		60)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.AfterEach(func() {
	ginkgo.By("Tear down namespace: " + dev)
	err := kClient.TearDownNamespace(dev)
	Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {

	// call the healthCheck api to check scheduler health
	ginkgo.By("Check Yunikorn's health")
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

var _ = ginkgo.Describe("", func() {

	ginkgo.It("Verify_Pod_Alloc_Props", func() {
		err := restClient.WaitForAppStateTransition("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"], "Running", 30)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		appsInfo, err := restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		gomega.Ω(appsInfo.Allocations).NotTo(gomega.BeNil())
		allocations := appsInfo.Allocations[0]
		gomega.Ω(allocations).NotTo(gomega.BeNil())
		gomega.Ω(allocations.AllocationKey).NotTo(gomega.BeNil())
		gomega.Ω(allocations.NodeID).NotTo(gomega.BeNil())
		gomega.Ω(allocations.ApplicationID).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap := allocations.ResourcePerAlloc
		gomega.Ω(resMap).NotTo(gomega.BeEmpty())
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
		defer kClient.DeleteWorkloadAndPods(job1.Name, k8s.Job, dev)
		job2 := k8s.InitTestJob(appID2, parallelism, parallelism, pod2)
		_, createErr2 := kClient.CreateJob(job2, dev)
		defer kClient.DeleteWorkloadAndPods(job2.Name, k8s.Job, dev)
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
	})

	ginkgo.It("Verify_GangScheduling_TwoGangs_Restart_YK", func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		defer yunikorn.RestorePortForwarding(&kClient)

		appID := gangSleepJobPrefix + "-" + common.RandSeq(5)
		sleepPodConfig := k8s.SleepPodConfig{Name: "gang-sleep-job", NS: dev, Time: 1, AppID: appID}
		taskGroups, taskGroupErr := k8s.InitTaskGroups(sleepPodConfig, taskGroupA, taskGroupB, parallelism)
		Ω(taskGroupErr).NotTo(gomega.HaveOccurred())
		pod, podErr := k8s.InitSleepPod(sleepPodConfig)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		pod = k8s.DecoratePodForGangScheduling(30, "Soft", taskGroupA,
			taskGroups, pod)

		ginkgo.By("Submitting gang sleep job")
		job := k8s.InitTestJob(appID, parallelism, parallelism, pod)
		_, err := kClient.CreateJob(job, dev)
		Ω(err).NotTo(gomega.HaveOccurred())
		defer kClient.DeleteWorkloadAndPods(job.Name, k8s.Job, dev)

		ginkgo.By("Waiting job pods to be created")
		createErr := kClient.WaitForJobPodsCreated(dev, job.Name, parallelism, 30*time.Second)
		Ω(createErr).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for placeholders in task group A (expected state: Running)")
		groupAPrefix := "tg-" + appID + "-" + taskGroupA + "-"
		stateRunning := v1.PodRunning
		err = kClient.WaitForPlaceholders(dev, groupAPrefix, parallelism, 30*time.Second, &stateRunning)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for placeholders in task group B (expected state: Pending)")
		groupBPrefix := "tg-" + appID + "-" + taskGroupB + "-"
		statePending := v1.PodPending
		err = kClient.WaitForPlaceholders(dev, groupBPrefix, parallelism+1, 30*time.Second, &statePending)
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
			if strings.HasPrefix(podName, groupAPrefix) {
				groupAPlaceholderCount++
				Ω(podPhase).To(gomega.Equal(v1.PodRunning))
				continue
			}
			if strings.HasPrefix(podName, groupBPrefix) {
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

		ginkgo.By("Trying to find an available worker node")
		nodes, err := kClient.GetNodes()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(len(nodes.Items) >= 2).Should(gomega.Equal(true), "Not enough nodes in the cluster, need at least 2")

		var workerResource *resource.Quantity
		masterPresent := false
		var selectedNode string
		var nodesToTaint []string
		for _, node := range nodes.Items {
			// skip master if it's marked as such
			node := node
			if k8s.IsMasterNode(&node) {
				masterPresent = true
				continue
			}

			if selectedNode == "" {
				workerResource = node.Status.Allocatable.Memory()
				selectedNode = node.Name
			} else {
				nodesToTaint = append(nodesToTaint, node.Name)
			}
		}

		memoryGiB := workerResource.ScaledValue(resource.Giga)
		placeholderCount := int32(memoryGiB/3 + 2) // get a reasonable number to have both Running/Pending PH pods
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s allocatable memory in GiB = %d, number of placeholders to use = %d\n",
			selectedNode, memoryGiB, placeholderCount)

		ginkgo.By("Tainting all nodes except " + selectedNode)
		err = kClient.TaintNodes(nodesToTaint, taintKey, "value", v1.TaintEffectNoSchedule)
		Ω(err).NotTo(gomega.HaveOccurred())

		removeTaint := true
		defer func() {
			if removeTaint {
				ginkgo.By("Untainting nodes (defer)")
				err = kClient.UntaintNodes(nodesToTaint, taintKey)
				Ω(err).NotTo(gomega.HaveOccurred(), "Could not remove taint from nodes "+strings.Join(nodesToTaint, ","))
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

		ginkgo.By("Restart the scheduler pod and update tolerations if needed")
		includeMaster := masterPresent && len(nodes.Items) == 2
		// YK can be scheduled on the master only if there are 2 nodes in the cluster
		newTolerations := getSchedulerPodTolerations(includeMaster)
		yunikorn.RestartYunikornAndAddTolerations(&kClient, true, newTolerations)

		ginkgo.By("Untainting nodes")
		removeTaint = false
		err = kClient.UntaintNodes(nodesToTaint, taintKey)
		Ω(err).NotTo(gomega.HaveOccurred(), "Could not remove taint from nodes "+strings.Join(nodesToTaint, ","))

		ginkgo.By("Waiting for placeholder replacement & sleep pods to finish")
		err = kClient.WaitForJobPodsSucceeded(dev, job.Name, 1, 60*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
	})
})

func getSchedulerPodTolerations(includeMaster bool) []v1.Toleration {
	newTolerations := make([]v1.Toleration, 0)
	if includeMaster {
		for key := range common.MasterTaints {
			t := v1.Toleration{
				Key:      key,
				Effect:   v1.TaintEffectNoSchedule,
				Operator: v1.TolerationOpEqual,
			}
			newTolerations = append(newTolerations, t)
		}
	}

	t := v1.Toleration{
		Key:      taintKey,
		Effect:   v1.TaintEffectNoSchedule,
		Operator: v1.TolerationOpEqual,
	}
	newTolerations = append(newTolerations, t)
	return newTolerations
}
