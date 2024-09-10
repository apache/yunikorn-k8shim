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

package simple_preemptor_test

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)

// Nodes
var Worker1 = ""
var Worker2 = ""
var Worker1Res = resource.NewQuantity(8*1000*1000, resource.DecimalSI)
var Worker2Res = resource.NewQuantity(8*1000*1000, resource.DecimalSI)
var sleepPodMemLimit1 int64
var sleepPodMemLimit2 int64
var taintKey = "e2e_test_simple_preemptor"
var nodesToTaint []string

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	var nodes *v1.NodeList
	nodes, err = kClient.GetNodes()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Events cant be empty")

	// Extract node allocatable resources
	for _, node := range nodes.Items {
		// skip master if it's marked as such
		node := node
		if k8s.IsMasterNode(&node) || !k8s.IsComputeNode(&node) {
			continue
		}
		if Worker1 == "" {
			Worker1 = node.Name
			Worker1Res = node.Status.Allocatable.Memory()
		} else if Worker2 == "" {
			Worker2 = node.Name
			Worker2Res = node.Status.Allocatable.Memory()
		} else {
			nodesToTaint = append(nodesToTaint, node.Name)
		}
	}
	ginkgo.By("Worker1:" + Worker1)
	ginkgo.By("Worker2:" + Worker2)

	ginkgo.By("Tainting some nodes..")
	err = kClient.TaintNodes(nodesToTaint, taintKey, "value", v1.TaintEffectNoSchedule)
	Ω(err).NotTo(gomega.HaveOccurred())

	var pods *v1.PodList
	totalPodQuantity1 := *resource.NewQuantity(0, resource.DecimalSI)
	totalPodQuantity2 := *resource.NewQuantity(0, resource.DecimalSI)
	pods, err = kClient.GetPods("yunikorn")
	if err == nil {
		for _, pod := range pods.Items {
			for _, c := range pod.Spec.Containers {
				if pod.Spec.NodeName == Worker1 {
					totalPodQuantity1.Add(*resource.NewQuantity(c.Resources.Requests.Memory().Value(), resource.DecimalSI))
				} else if pod.Spec.NodeName == Worker2 {
					totalPodQuantity2.Add(*resource.NewQuantity(c.Resources.Requests.Memory().Value(), resource.DecimalSI))
				}
			}
		}
	}
	Worker1Res.Sub(totalPodQuantity1)
	sleepPodMemLimit1 = int64(float64(Worker1Res.Value())/3.5) / (1000 * 1000)
	Worker2Res.Sub(totalPodQuantity2)
	sleepPodMemLimit2 = int64(float64(Worker2Res.Value())/3.5) / (1000 * 1000)
})

var _ = ginkgo.BeforeEach(func() {
	dev = "dev" + common.RandSeq(5)
	ginkgo.By("create development namespace")
	ns, err := kClient.CreateNamespace(dev, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterSuite(func() {

	ginkgo.By("Untainting some nodes")
	err := kClient.UntaintNodes(nodesToTaint, taintKey)
	Ω(err).NotTo(gomega.HaveOccurred(), "Could not remove taint from nodes "+strings.Join(nodesToTaint, ","))

	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

var _ = ginkgo.Describe("SimplePreemptor", func() {
	ginkgo.It("Verify_basic_simple_preemption", func() {
		// Use case: Only one pod is running and same pod has been selected as victim
		// Define sleepPod
		sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev, Mem: sleepPodMemLimit1 * 2, Time: 600, RequiredNode: Worker1}
		sleepPod2Configs := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: sleepPodMemLimit2 * 2, Time: 600}
		sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, RequiredNode: Worker2, Mem: sleepPodMemLimit2 * 2, Time: 600}

		for _, config := range []k8s.SleepPodConfig{sleepPodConfigs, sleepPod2Configs, sleepPod3Configs} {
			ginkgo.By("Deploy the sleep pod " + config.Name + " to the development namespace")
			sleepObj, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, err := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())

			// Wait for pod to move to running state
			err = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				60)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}
		// assert sleeppod2 is killed
		err := kClient.WaitForPodEvent(dev, "sleepjob2", "Killing", 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_simple_preemption", func() {
		// Use case: When 3 sleep pods (2 opted out, regular) are running, regular pod should be victim to free up resources for 4th sleep pod
		// Define sleepPod
		sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev, Mem: sleepPodMemLimit1, Time: 600, RequiredNode: Worker1}
		sleepPod2Configs := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: sleepPodMemLimit1, Time: 600, RequiredNode: Worker1}
		sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, Mem: sleepPodMemLimit1, Time: 600, RequiredNode: Worker1, AppID: "test01"}

		// add the same AppID to sleepPod3Configs so that sleepPod4Configs is not the originator pod
		sleepPod4Configs := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: k8s.Allow, AppID: "test01"}
		sleepPod5Configs := k8s.SleepPodConfig{Name: "sleepjob5", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: k8s.Deny}
		sleepPod6Configs := k8s.SleepPodConfig{Name: "sleepjob6", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: k8s.Deny}
		sleepPod7Configs := k8s.SleepPodConfig{Name: "sleepjob7", NS: dev, Mem: sleepPodMemLimit2, Time: 600, RequiredNode: Worker2}

		for _, config := range []k8s.SleepPodConfig{sleepPodConfigs, sleepPod2Configs,
			sleepPod3Configs, sleepPod4Configs, sleepPod5Configs, sleepPod6Configs} {
			ginkgo.By("Deploy the sleep pod " + config.Name + " to the development namespace")
			sleepObj, podErr := k8s.InitSleepPod(config)

			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, err := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
			// Wait for pod to move to running state
			err = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				240)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Deploy the sleep pod " + sleepPod7Configs.Name + " to the development namespace")
		sleepObj, podErr := k8s.InitSleepPod(sleepPod7Configs)

		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// Wait for pod to move to running state
		err = kClient.WaitForPodRunning(dev, sleepRespPod.Name, 1*time.Minute)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// assert sleeppod4 is killed
		err = kClient.WaitForPodEvent(dev, "sleepjob4", "Killing", 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
		ginkgo.By("Tearing down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(gomega.HaveOccurred())
	})
})
