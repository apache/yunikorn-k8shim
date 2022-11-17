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
	"strings"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = ginkgo.Describe("SimplePreemptor", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var ns *v1.Namespace
	var oldConfigMap *v1.ConfigMap
	var dev = "dev" + common.RandSeq(5)

	// Nodes
	Worker1 := "yk8s-worker"
	Worker2 := "yk8s-worker2"
	Worker1Res := resource.NewQuantity(8*1000*1000, resource.DecimalSI)
	Worker2Res := resource.NewQuantity(8*1000*1000, resource.DecimalSI)
	var sleepPodMemLimit1 int64
	var sleepPodMemLimit2 int64

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}
		Ω(restClient).NotTo(gomega.BeNil())

		yunikorn.EnsureYuniKornConfigsPresent()

		ginkgo.By("Port-forward the scheduler pod")
		var err = kClient.PortForwardYkSchedulerPod()
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enable basic scheduling config over config maps")
		var c, configErr = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(configErr).NotTo(gomega.HaveOccurred())
		Ω(c).NotTo(gomega.BeNil())

		oldConfigMap = c.DeepCopy()
		Ω(c).Should(gomega.BeEquivalentTo(oldConfigMap))

		// Define basic configMap
		sc := common.CreateBasicConfigMap()
		configStr, yamlErr := common.ToYAML(sc)
		Ω(yamlErr).NotTo(gomega.HaveOccurred())

		c.Data[configmanager.DefaultPolicyGroup] = configStr
		var d, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err3).NotTo(gomega.HaveOccurred())
		Ω(d).NotTo(gomega.BeNil())

		ginkgo.By("create development namespace")
		ns, err = kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

		var nodes *v1.NodeList
		nodes, err = kClient.GetNodes()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Events cant be empty")

		// Extract node allocatable resources
		for _, node := range nodes.Items {
			if node.Name == Worker1 {
				Worker1Res = node.Status.Allocatable.Memory()
			} else if node.Name == Worker2 {
				Worker2Res = node.Status.Allocatable.Memory()
			}
		}

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

	ginkgo.It("Verify_basic_simple_preemption. Use case: Only one pod is running and same pod has been selected as victim", func() {

		// Define sleepPod
		sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev, Mem: sleepPodMemLimit1 * 2, Time: 600, RequiredNode: "yk8s-worker"}
		sleepPod2Configs := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: sleepPodMemLimit2 * 2, Time: 600}
		sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, RequiredNode: "yk8s-worker2", Mem: sleepPodMemLimit2 * 2, Time: 600}

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
		err := kClient.WaitForPodEvent(dev, "sleepjob2", "Killing", 120)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_simple_preemption. Use case: When 3 sleep pods (2 opted out, regular) are running, regular pod should be victim to free up resources for 4th sleep pod", func() {

		// Define sleepPod
		sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev, Mem: sleepPodMemLimit1, Time: 600, RequiredNode: "yk8s-worker"}
		sleepPod2Configs := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: sleepPodMemLimit1, Time: 600, RequiredNode: "yk8s-worker"}
		sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, Mem: sleepPodMemLimit1, Time: 600, RequiredNode: "yk8s-worker"}

		// random uid in pod spec to ensure this Non-opted pod doesn't become originator
		sleepPod4Configs := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: true, UID: types.UID("ddd")}
		sleepPod5Configs := k8s.SleepPodConfig{Name: "sleepjob5", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: false}
		sleepPod6Configs := k8s.SleepPodConfig{Name: "sleepjob6", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: false}
		sleepPod7Configs := k8s.SleepPodConfig{Name: "sleepjob7", NS: dev, Mem: sleepPodMemLimit2, Time: 600, RequiredNode: "yk8s-worker2"}

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
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			600)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// assert sleeppod6 is killed
		err = kClient.WaitForPodEvent(dev, "sleepjob4", "Killing", 1200)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.AfterEach(func() {

		// Delete all sleep pods
		ginkgo.By("Delete all sleep pods")
		pods, err := kClient.GetPodNamesFromNS(ns.Name)
		if err == nil {
			for _, each := range pods {
				if strings.Contains(each, "sleep") {
					ginkgo.By("Deleting sleep pod: " + each)
					err = kClient.DeletePod(each, ns.Name)
					if err != nil {
						if statusErr, ok := err.(*k8serrors.StatusError); ok {
							if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
								fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pod %s - reason is %s, it "+
									"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
								continue
							}
						}
					}
				}
			}
		} else {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get pods from namespace %s - reason is %s\n", ns.Name, err.Error())
		}
	})

	ginkgo.AfterSuite(func() {
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)

		ginkgo.By("Tearing down namespace: " + ns.Name)
		err = kClient.TearDownNamespace(ns.Name)
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
