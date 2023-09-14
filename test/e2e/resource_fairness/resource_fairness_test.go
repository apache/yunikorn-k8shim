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

package resourcefairness_test

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var nodes *v1.NodeList
var workerID1 string
var workerID2 string
var worker1 dao.NodeDAOInfo
var worker2 dao.NodeDAOInfo
var namespace *v1.Namespace
var err error
var queuePath string

var maxCPU int64 = 500
var maxMem int64 = 500
var nodesToTaint = []string{}
var taintKey = "e2e_test_fairness"
var ns = "test-" + common.RandSeq(10)

var _ = BeforeSuite(func() {
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(BeNil())

	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	ginkgo.By("Port-forward the scheduler pod")
	err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("create development namespace")
	namespace, err = kClient.CreateNamespace(ns, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(namespace.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

	// get node available resource
	nodes, err = kClient.GetNodes()
	Ω(err).NotTo(HaveOccurred())
	Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Nodes can't be zero")

	for _, node := range nodes.Items {
		node := node
		if k8s.IsMasterNode(&node) || !k8s.IsComputeNode(&node) {
			continue
		}
		if workerID1 == "" {
			workerID1 = node.Name
		} else if workerID2 == "" {
			workerID2 = node.Name
		} else {
			nodesToTaint = append(nodesToTaint, node.Name)
		}
	}
	Ω(workerID1).NotTo(gomega.BeEmpty(), "worker node not found")
	Ω(workerID2).NotTo(gomega.BeEmpty(), "only found one worker node")

	ginkgo.By("Tainting some nodes")
	err = kClient.TaintNodes(nodesToTaint, taintKey, "value", v1.TaintEffectNoSchedule)
	Ω(err).NotTo(HaveOccurred())

	var nodesDAOInfo *[]dao.NodeDAOInfo
	nodesDAOInfo, err = restClient.GetNodes(constants.DefaultPartition)
	Ω(err).NotTo(HaveOccurred())
	Ω(nodesDAOInfo).NotTo(gomega.BeNil())

	for _, node := range *nodesDAOInfo {
		if node.NodeID == workerID1 {
			worker1 = node
		}
		if node.NodeID == workerID2 {
			worker2 = node
		}
	}
})

var _ = Describe("FairScheduling:", func() {
	// Validates waitQueue order of requested app resources, according to fairAppScheduling.
	// Step 1: Deploy 4 apps, which sum to 95% queueCPU / 35% queueMem
	//              -> Resource priority order: 1)CPU. 2)Mem
	// Step 2: Deploy 1 more blocked pod each for apps0-2
	// Step 3: Kill an App3 pod.
	// Step 4: App with least cpu use is allocated pod next. Break tie using mem.
	It("Test_Wait_Queue_Order", func() {

		By(fmt.Sprintf("Creating test namespace %s", ns))
		namespace, err = kClient.UpdateNamespace(ns, map[string]string{"vcore": "500m", "memory": "500M"})
		Ω(err).ShouldNot(HaveOccurred())
		Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))

		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		queuePath = "root." + ns
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fair", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:   ns,
				Parent: false,
				Resources: configs.Resources{Max: map[string]string{"vcore": "500m", "memory": "500M"},
					Guaranteed: map[string]string{"vcore": "500m", "memory": "500M"}},
				Properties: map[string]string{"application.sort.policy": "fair"},
			}); err != nil {
				return err
			}
			return nil
		})

		// Create appIDs
		var apps []string
		for j := 0; j < 4; j++ {
			id := fmt.Sprintf("app%d-%s", j, common.RandSeq(5))
			apps = append(apps, id)
		}

		// Initial allocation to fill ns quota
		appPods := map[string][]string{}
		appAllocs := map[string]map[string]float64{
			apps[0]: {"cpu": 0.3, "mem": 0.1, "pods": 1},
			apps[1]: {"cpu": 0.2, "mem": 0.05, "pods": 1},
			apps[2]: {"cpu": 0.1, "mem": 0.15, "pods": 1},
			apps[3]: {"cpu": 0.4, "mem": 0.05, "pods": 4},
		}
		for appID, req := range appAllocs {
			appPods[appID] = []string{}
			numPods := int(req["pods"])
			By(fmt.Sprintf("[%s] Deploy %d pods", appID, numPods))
			for i := 0; i < numPods; i++ {
				// Calculate individual podRequest by dividing total appAlloc by numPods
				reqCPU := int64(req["cpu"] / req["pods"] * float64(maxCPU))
				reqMem := int64(req["mem"] / req["pods"] * float64(maxMem))
				podName := fmt.Sprintf("%s-pod%d", appID, i)
				appPods[appID] = append(appPods[appID], podName)

				// Deploy pod
				sleepPodConf := k8s.SleepPodConfig{
					Name: podName, NS: ns, AppID: appID, CPU: reqCPU, Mem: reqMem, Labels: map[string]string{
						constants.LabelQueueName: queuePath}}
				initPod, podErr := k8s.InitSleepPod(sleepPodConf)
				Ω(podErr).NotTo(HaveOccurred())
				_, err = kClient.CreatePod(initPod, ns)
				Ω(err).NotTo(HaveOccurred())
				err = kClient.WaitForPodRunning(ns, podName, 120*time.Second)
				Ω(err).NotTo(HaveOccurred())
			}
		}

		// Submit one blocked pod for each app in random order. Each requests 0.1 qCPU and 0.05 qMem.
		By("Submitting additional blocked pod for each app")
		randOrder := rand.Perm(3)
		for _, i := range randOrder {
			cpuPct, memPct, appID := 0.1, 0.05, apps[i]
			reqCPU, reqMem := int64(cpuPct*float64(maxCPU)), int64(memPct*float64(maxMem))
			podNum := len(appPods[appID])
			podName := fmt.Sprintf("%s-pod%d", appID, podNum)
			appPods[appID] = append(appPods[appID], podName)

			By(fmt.Sprintf("[%s] Submit %s", appID, podName))
			sleepPodConf := k8s.SleepPodConfig{
				Name: podName, NS: ns, AppID: appID, CPU: reqCPU, Mem: reqMem, Labels: map[string]string{
					constants.LabelQueueName: queuePath}}
			initPod, podErr := k8s.InitSleepPod(sleepPodConf)
			Ω(podErr).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			err = kClient.WaitForPodPending(ns, podName, 10*time.Second)
			Ω(err).NotTo(HaveOccurred())

			// Wait till requests has been added to application
			err := wait.PollImmediate(300*time.Millisecond, 60*time.Second, func() (bool, error) {
				app, err := restClient.GetAppInfo("default", queuePath, appID)
				if err != nil {
					return false, nil
				}
				if len(app.Requests) == 1 {
					return true, nil
				}
				return false, nil
			})
			Ω(err).NotTo(HaveOccurred())
		}

		// Log correct app priority and pod wait order
		appOrder := []string{apps[2], apps[1], apps[0]}
		var waitOrder []string
		for _, appID := range appOrder {
			l := len(appPods[appID])
			waitOrder = append(waitOrder, appPods[appID][l-1])
		}

		// Verify wait order by releasing app3 pod. Then check for correct pod running.
		for _, podName := range waitOrder {
			// Delete app3 pod to release resource
			app3Pods, l := appPods[apps[3]], len(appPods[apps[3]])
			pod := app3Pods[l-1]
			appPods[apps[3]] = app3Pods[:l-1]

			By(fmt.Sprintf("Delete %s", pod))
			err := kClient.DeletePod(pod, ns)
			Ω(err).NotTo(HaveOccurred())

			By(fmt.Sprintf("Verify %s is now running", podName))
			err = kClient.WaitForPodRunning(ns, podName, 120*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}
	})

	// Validates the order of node allocation for requested pod resources, following the fairNodeScheduling approach.
	// Step 1: Deploy 2 apps, which utilizing 5% of the resources on worker1 and 10% on worker2.
	// Step 2: Deploy 1 apps, according to fair scheduling principles, will be allocated to worker1.
	//         This allocation increases the resource allocation on worker1 to 15%.
	// Step 3: Deploy 1 apps, according to fair scheduling principles, will be allocated to worker2.
	It("Verify_basic_node_sorting_with_fairness_policy", func() {
		By(fmt.Sprintf("update test namespace %s", ns))
		namespace, err = kClient.UpdateNamespace(ns, nil)
		Ω(err).ShouldNot(HaveOccurred())
		Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))

		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		queuePath = "root.sandbox"
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fair", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:   "sandbox",
				Parent: false,
			}); err != nil {
				return err
			}
			return nil
		})

		sleepPodConfs := []k8s.SleepPodConfig{}

		// Select worker1, utilizing 5% of the resources on worker1
		sleepPod1Conf := k8s.SleepPodConfig{
			Name: "pod1", NS: ns, AppID: "app1",
			CPU:          fillNodeUtil(&worker1, "vcore", float64(0.05)),
			Mem:          fillNodeUtil(&worker1, "memory", float64(0.05)) / 1000 / 1000,
			RequiredNode: workerID1,
			Labels:       map[string]string{constants.LabelQueueName: queuePath}}

		// Select worker2, utilizing 10% of the resources on worker2
		sleepPod2Conf := k8s.SleepPodConfig{
			Name: "pod2", NS: ns, AppID: "app2",
			CPU:          fillNodeUtil(&worker2, "vcore", float64(0.10)),
			Mem:          fillNodeUtil(&worker2, "memory", float64(0.10)) / 1000 / 1000,
			RequiredNode: workerID2,
			Labels:       map[string]string{constants.LabelQueueName: queuePath}}

		// It wouldn't select node, but would utilizing 10% of the resources on worker1
		sleepPod3Conf := k8s.SleepPodConfig{
			Name: "pod3", NS: ns, AppID: "app3",
			CPU:    fillNodeUtil(&worker1, "vcore", float64(0.10)),
			Mem:    fillNodeUtil(&worker1, "memory", float64(0.10)) / 1000 / 1000,
			Labels: map[string]string{constants.LabelQueueName: queuePath}}

		// It wouldn't select node, but would utilizing 10% of the resources on worker2
		sleepPod4Conf := k8s.SleepPodConfig{
			Name: "pod4", NS: ns, AppID: "app4",
			CPU:    fillNodeUtil(&worker2, "vcore", float64(0.10)),
			Mem:    fillNodeUtil(&worker2, "memory", float64(0.10)) / 1000 / 1000,
			Labels: map[string]string{constants.LabelQueueName: queuePath}}

		sleepPodConfs = append(sleepPodConfs, sleepPod1Conf)
		sleepPodConfs = append(sleepPodConfs, sleepPod2Conf)
		sleepPodConfs = append(sleepPodConfs, sleepPod3Conf)
		sleepPodConfs = append(sleepPodConfs, sleepPod4Conf)

		// Deploy pod
		for _, config := range sleepPodConfs {
			ginkgo.By("Deploy the sleep pod " + config.Name)
			initPod, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			err = kClient.WaitForPodRunning(ns, config.Name, 3600*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}

		// Verify pod3, pod4 have been deployed on the correct node.
		var RespPod *v1.Pod
		ginkgo.By(sleepPod3Conf.Name + " should deploy on " + workerID1)
		RespPod, err = kClient.GetPod(sleepPod3Conf.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(RespPod.Spec.NodeName).To(gomega.Equal(workerID1), "Pod should place on "+workerID1)

		ginkgo.By(sleepPod4Conf.Name + " should deploy on " + workerID2)
		RespPod, err = kClient.GetPod(sleepPod4Conf.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(RespPod.Spec.NodeName).To(gomega.Equal(workerID2), "Pod should place on "+workerID2)
	})

	// Validates the order of node allocation for requested pod resources, considering fairNodeScheduling with resource weights.
	// Step 1: Set the Resource weights to {"vcore":  2.0,	"memory": 1.0}
	// Step 2: Deploy 2 apps, utilizing 13% of the resource on worker1,
	//                            , and 17% of on worker2,
	// Step 3: Deploy 1 apps, according to fair scheduling principles, will be allocated to worker1.
	//         This allocation increases the resource allocation on worker1 to 20%.
	// Step 4: Deploy 1 apps, according to fair scheduling principles, will be allocated to worker2.
	It("Verify_node_sorting_fairness_policy_with_resource_weight", func() {
		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		queuePath = "root.sandbox"
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fair", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			sc.Partitions[0].NodeSortPolicy = configs.NodeSortingPolicy{
				Type: "fair",
				ResourceWeights: map[string]float64{
					"vcore":  2.0,
					"memory": 1.0,
				},
			}
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:   "sandbox",
				Parent: false,
			}); err != nil {
				return err
			}
			return nil
		})

		sleepPodConfs := []k8s.SleepPodConfig{}

		// Select worker1, utilizing 13% of the resources on worker1
		sleepPod1Conf := k8s.SleepPodConfig{
			Name: "pod1", NS: ns, AppID: "app1",
			CPU:          fillNodeUtil(&worker1, "vcore", float64(0.10)),
			Mem:          fillNodeUtil(&worker1, "memory", float64(0.20)) / 1000 / 1000,
			RequiredNode: workerID1,
			Labels:       map[string]string{constants.LabelQueueName: queuePath}}

		// Select worker1, utilizing 17% of the resources on worker2
		sleepPod2Conf := k8s.SleepPodConfig{
			Name: "pod2", NS: ns, AppID: "app2",
			CPU:          fillNodeUtil(&worker2, "vcore", float64(0.20)),
			Mem:          fillNodeUtil(&worker2, "memory", float64(0.10)) / 1000 / 1000,
			RequiredNode: workerID2,
			Labels:       map[string]string{constants.LabelQueueName: queuePath}}

		// It wouldn't select node, but would utilizing 17% of the resources on worker1
		sleepPod3Conf := k8s.SleepPodConfig{
			Name: "pod3", NS: ns, AppID: "app3",
			CPU:    fillNodeUtil(&worker1, "vcore", float64(0.10)),
			Mem:    0,
			Labels: map[string]string{constants.LabelQueueName: queuePath}}

		// It wouldn't select node, but would utilizing 13% of the resources on worker2
		sleepPod4Conf := k8s.SleepPodConfig{
			Name: "pod4", NS: ns, AppID: "app4",
			CPU:    0,
			Mem:    fillNodeUtil(&worker2, "memory", float64(0.10)) / 1000 / 1000,
			Labels: map[string]string{constants.LabelQueueName: queuePath}}

		sleepPodConfs = append(sleepPodConfs, sleepPod1Conf)
		sleepPodConfs = append(sleepPodConfs, sleepPod2Conf)
		sleepPodConfs = append(sleepPodConfs, sleepPod3Conf)
		sleepPodConfs = append(sleepPodConfs, sleepPod4Conf)

		// Deploy pod
		for _, config := range sleepPodConfs {
			ginkgo.By("Deploy the sleeppod " + config.Name)
			initPod, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			err = kClient.WaitForPodRunning(ns, config.Name, 30*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}

		// Verify pod3, pod4 have been deployed on the correct node.
		ginkgo.By(sleepPod3Conf.Name + " should deploy on " + workerID1)
		RespPod, err := kClient.GetPod(sleepPod3Conf.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(RespPod.Spec.NodeName).To(gomega.Equal(workerID1), "Pod should place on "+workerID1)

		ginkgo.By(sleepPod4Conf.Name + " should deploy on " + workerID2)
		RespPod, err = kClient.GetPod(sleepPod4Conf.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(RespPod.Spec.NodeName).To(gomega.Equal(workerID2), "Pod should place on "+workerID2)
	})

	AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}

		// Delete all sleep pods
		ginkgo.By("Delete all sleep pods")
		err := kClient.DeletePods(ns)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pods in namespace %s - reason is %s\n", ns, err.Error())
		}
	})
})

func fillNodeUtil(node *dao.NodeDAOInfo, resourceType string, percent float64) int64 {
	fillingResource := percent*float64(node.Capacity[resourceType]) - float64(node.Allocated[resourceType])
	return int64(fillingResource)
}
