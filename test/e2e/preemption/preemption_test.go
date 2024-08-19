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

package preemption_test

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var ns *v1.Namespace
var dev string
var oldConfigMap = new(v1.ConfigMap)

// Nodes
var Worker = ""
var WorkerMemRes int64
var sleepPodMemLimit int64
var sleepPodMemLimit2 int64
var taintKey = "e2e_test_preemption"
var nodesToTaint []string
var cantBeScheduledMesg = "The sleep pod %s can't be scheduled"
var deployMesg = "Deploy the sleep pod %s to the development namespace"

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

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	var nodes *v1.NodeList
	nodes, err = kClient.GetNodes()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Nodes cant be empty")

	// Extract node allocatable resources
	for _, node := range nodes.Items {
		// skip master if it's marked as such
		node := node
		if k8s.IsMasterNode(&node) || !k8s.IsComputeNode(&node) {
			continue
		}
		if Worker == "" {
			Worker = node.Name
		} else {
			nodesToTaint = append(nodesToTaint, node.Name)
		}
	}
	Ω(Worker).NotTo(gomega.BeEmpty(), "Worker node not found")

	ginkgo.By("Tainting some nodes..")
	err = kClient.TaintNodes(nodesToTaint, taintKey, "value", v1.TaintEffectNoSchedule)
	Ω(err).NotTo(gomega.HaveOccurred())

	nodesDAOInfo, err := restClient.GetNodes(constants.DefaultPartition)
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(nodesDAOInfo).NotTo(gomega.BeNil())

	for _, node := range *nodesDAOInfo {
		if node.NodeID == Worker {
			WorkerMemRes = node.Available["memory"]
		}
	}
	WorkerMemRes /= (1000 * 1000) // change to M
	fmt.Fprintf(ginkgo.GinkgoWriter, "Worker node %s available memory %dM\n", Worker, WorkerMemRes)

	sleepPodMemLimit = int64(float64(WorkerMemRes) / 3)
	Ω(sleepPodMemLimit).NotTo(gomega.BeZero(), "Sleep pod memory limit cannot be zero")
	fmt.Fprintf(ginkgo.GinkgoWriter, "Sleep pod limit memory %dM\n", sleepPodMemLimit)

	sleepPodMemLimit2 = int64(float64(WorkerMemRes) / 4)
	Ω(sleepPodMemLimit2).NotTo(gomega.BeZero(), "Sleep pod memory limit cannot be zero")
	fmt.Fprintf(ginkgo.GinkgoWriter, "Sleep pod limit memory %dM\n", sleepPodMemLimit2)
})

var _ = ginkgo.AfterSuite(func() {

	ginkgo.By("Untainting some nodes")
	err := kClient.UntaintNodes(nodesToTaint, taintKey)
	Ω(err).NotTo(gomega.HaveOccurred(), "Could not remove taint from nodes "+strings.Join(nodesToTaint, ","))

	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
})

var _ = ginkgo.Describe("Preemption", func() {
	ginkgo.BeforeEach(func() {
		dev = "dev-" + common.RandSeq(5)
		ginkgo.By("Creating development namespace: " + dev)
		var err error
		ns, err = kClient.CreateNamespace(dev, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.It("Verify_basic_preemption", func() {
		ginkgo.By("A queue uses resource more than the guaranteed value even after removing one of the pods. The cluster doesn't have enough resource to deploy a pod in another queue which uses resource less than the guaranteed value.")
		// update config
		ginkgo.By(fmt.Sprintf("Update root.sandbox1 and root.sandbox2 with guaranteed memory %dM", sleepPodMemLimit))
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox1",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox2",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}
			return nil
		})

		// Define sleepPod
		sleepPodConfigs := createSandbox1SleepPodCofigs(3, 600, "root.sandbox1")
		sleepPod4Config := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit, Time: 600, Optedout: k8s.Allow, Labels: map[string]string{"queue": "root.sandbox2"}}
		sleepPodConfigs = append(sleepPodConfigs, sleepPod4Config)

		for _, config := range sleepPodConfigs {
			ginkgo.By(fmt.Sprintf(deployMesg, config.Name))
			sleepObj, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				120)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		}

		// assert one of the pods in root.sandbox1 is preempted
		ginkgo.By("One of the pods in root.sanbox1 is preempted")
		sandbox1RunningPodsCnt := 0
		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.sandbox1")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if pod.Status.Phase == v1.PodRunning {
				sandbox1RunningPodsCnt++
			}
		}
		Ω(sandbox1RunningPodsCnt).To(gomega.Equal(2), "One of the pods in root.sandbox1 should be preempted")
	})

	ginkgo.It("Verify_no_preemption_on_resources_less_than_guaranteed_value", func() {
		ginkgo.By("A queue uses resource less than the guaranteed value can't be preempted.")
		// update config
		ginkgo.By(fmt.Sprintf("Update root.sandbox1 and root.sandbox2 with guaranteed memory %dM", WorkerMemRes))
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox1",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", WorkerMemRes)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox2",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", WorkerMemRes)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}
			return nil
		})

		// Define sleepPod
		sandbox1SleepPodConfigs := createSandbox1SleepPodCofigs(3, 30, "root.sandbox1")
		sleepPod4Config := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit, Time: 30, Optedout: k8s.Allow, Labels: map[string]string{"queue": "root.sandbox2"}}

		// Deploy pods in root.sandbox1
		for _, config := range sandbox1SleepPodConfigs {
			ginkgo.By(fmt.Sprintf(deployMesg, config.Name))
			sleepObj, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				30)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		}

		// Deploy sleepjob4 pod in root.sandbox2
		ginkgo.By(fmt.Sprintf(deployMesg, sleepPod4Config.Name))
		sleepObj, podErr := k8s.InitSleepPod(sleepPod4Config)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod4, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// sleepjob4 pod can't be scheduled before pods in root.sandbox1 are succeeded
		ginkgo.By(fmt.Sprintf(cantBeScheduledMesg, sleepPod4Config.Name))
		err = kClient.WaitForPodUnschedulable(sleepRespPod4, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// pods in root.sandbox1 can be succeeded
		ginkgo.By("The pods in root.sandbox1 can be succeeded")
		for _, config := range sandbox1SleepPodConfigs {
			err = kClient.WaitForPodSucceeded(dev, config.Name, 30*time.Second)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}
	})

	ginkgo.It("Verify_no_preemption_outside_fence", func() {
		ginkgo.By("The preemption can't go outside the fence.")
		// update config
		ginkgo.By(fmt.Sprintf("Update root.sandbox1 and root.sandbox2 with guaranteed memory %dM. The root.sandbox2 has fence preemption policy.", sleepPodMemLimit))
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox1",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox2",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s", "preemption.policy": "fence"},
			}); err != nil {
				return err
			}
			return nil
		})

		// Define sleepPod
		sandbox1SleepPodConfigs := createSandbox1SleepPodCofigs(3, 30, "root.sandbox1")
		sleepPod4Config := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit, Time: 30, Optedout: k8s.Allow, Labels: map[string]string{"queue": "root.sandbox2"}}

		// Deploy pods in root.sandbox1
		for _, config := range sandbox1SleepPodConfigs {
			ginkgo.By(fmt.Sprintf(deployMesg, config.Name))
			sleepObj, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				30)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		}

		// Deploy sleepjob4 pod in root.sandbox2
		ginkgo.By(fmt.Sprintf(deployMesg, sleepPod4Config.Name))
		sleepObj, podErr := k8s.InitSleepPod(sleepPod4Config)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod4, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// sleepjob4 pod can't be scheduled before pods in root.sandbox1 are succeeded
		ginkgo.By(fmt.Sprintf(cantBeScheduledMesg, sleepPod4Config.Name))
		err = kClient.WaitForPodUnschedulable(sleepRespPod4, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// pods in root.sandbox1 can be succeeded
		ginkgo.By("The pods in root.sandbox1 can be succeeded")
		for _, config := range sandbox1SleepPodConfigs {
			err = kClient.WaitForPodSucceeded(dev, config.Name, 30*time.Second)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}
	})

	ginkgo.It("Verify_preemption_on_priority_queue", func() {
		ginkgo.By("A task can only preempt a task with lower or equal priority")
		// update config
		ginkgo.By(fmt.Sprintf("Update root.parent.sandbox1, root.parent.low-priority, root.parent.high-priority with guaranteed memory %dM", sleepPodMemLimit))
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:      "parent",
				Resources: configs.Resources{Max: map[string]string{"memory": fmt.Sprintf("%dM", 3*sleepPodMemLimit)}},
			}); err != nil {
				return err
			}
			if err = common.AddQueue(sc, "default", "root.parent", configs.QueueConfig{
				Name:       "high-priority",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s", "priority.offset": "100"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root.parent", configs.QueueConfig{
				Name:       "sandbox1",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s", "priority.offset": "0"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root.parent", configs.QueueConfig{
				Name:       "low-priority",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit)}},
				Properties: map[string]string{"preemption.delay": "1s", "priority.offset": "-100"},
			}); err != nil {
				return err
			}
			return nil
		})

		// Define sleepPod
		sandbox1SleepPodConfigs := createSandbox1SleepPodCofigs(3, 60, "root.parent.sandbox1")
		sleepPod4Config := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit, Time: 600, Optedout: k8s.Allow, Labels: map[string]string{"queue": "root.parent.low-priority"}}
		sleepPod5Config := k8s.SleepPodConfig{Name: "sleepjob5", NS: dev, Mem: sleepPodMemLimit, Time: 600, Optedout: k8s.Allow, Labels: map[string]string{"queue": "root.parent.high-priority"}}

		for _, config := range sandbox1SleepPodConfigs {
			ginkgo.By(fmt.Sprintf(deployMesg, config.Name))
			sleepObj, podErr := k8s.InitSleepPod(config)
			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				60)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		}

		// Deploy sleepjob4 pod in root.parent.low-priority
		ginkgo.By(fmt.Sprintf(deployMesg, sleepPod4Config.Name))
		sleepObj, podErr := k8s.InitSleepPod(sleepPod4Config)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod4, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// Deploy sleepjob5 pod in root.parent.high-priority
		ginkgo.By(fmt.Sprintf(deployMesg, sleepPod5Config.Name))
		sleepObj, podErr = k8s.InitSleepPod(sleepPod5Config)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod5, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// sleepjob4 pod can't be scheduled before pods in root.parent.sandbox1 are succeeded
		ginkgo.By(fmt.Sprintf(cantBeScheduledMesg, sleepPod4Config.Name))
		podErr = kClient.WaitForPodPending(dev, sleepPod4Config.Name, time.Duration(60)*time.Second)
		Ω(podErr).NotTo(HaveOccurred())

		ginkgo.By("Verify the sleep pod " + sleepPod4Config.Name + " request failed scheduling")
		podErr = restClient.WaitForAllocationLog("default", "root.parent.low-priority", sleepRespPod4.ObjectMeta.Labels["applicationId"], sleepPod4Config.Name, 60)
		Ω(podErr).NotTo(HaveOccurred())
		log, podErr := restClient.GetAllocationLog("default", "root.parent.low-priority", sleepRespPod4.ObjectMeta.Labels["applicationId"], sleepPod4Config.Name)
		Ω(podErr).NotTo(HaveOccurred())
		Ω(log).NotTo(gomega.BeNil(), "Log can't be empty")
		logEntries := yunikorn.AllocLogToStrings(log)
		Ω(logEntries).To(gomega.ContainElement(gomega.MatchRegexp(".*Not enough queue quota")), "Log entry message mismatch")

		// sleepjob5 pod can be scheduled before pods in root.parent.sandbox1 are succeeded
		ginkgo.By("The sleep pod " + sleepPod5Config.Name + " can be scheduled")
		err = kClient.WaitForPodScheduled(ns.Name, sleepRespPod5.Name, 90*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// assert one of the pods in root.sandbox1 is preempted
		ginkgo.By("One of the pods in root.parent.sanbox1 is preempted")
		sandbox1RunningPodsCnt := 0
		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.parent.sandbox1")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if pod.Status.Phase == v1.PodRunning {
				sandbox1RunningPodsCnt++
			}
		}
		Ω(sandbox1RunningPodsCnt).To(gomega.Equal(2), "One of the pods in root.parent.sandbox1 should be preempted")
	})

	ginkgo.It("Verify_allow_preemption_tag", func() {
		ginkgo.By("The value of 'false' for the allow preemption annotation on the PriorityClass moves the Pod to the back of the preemption list")
		// update config
		ginkgo.By(fmt.Sprintf("Update root.sandbox3, root.sandbox4 and root.sandbox5 with guaranteed memory %dM", sleepPodMemLimit2))
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox3",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit2)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox4",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit2)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "sandbox5",
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", sleepPodMemLimit2)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}
			return nil
		})

		// Define PriorityClass
		var preemptAllowPriorityClass = schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "allow-preemption",
				Annotations: map[string]string{constants.AnnotationAllowPreemption: constants.True},
			},
		}
		var preemptNotAllowPriorityClass = schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "preemption-not-allow",
				Annotations: map[string]string{constants.AnnotationAllowPreemption: constants.False},
			},
		}

		// Create PriorityClass
		ginkgo.By(fmt.Sprintf("Creating priority class %s", preemptAllowPriorityClass.Name))
		_, err := kClient.CreatePriorityClass(&preemptAllowPriorityClass)
		gomega.Ω(err).ShouldNot(HaveOccurred())
		ginkgo.By(fmt.Sprintf("Creating priority class %s", preemptNotAllowPriorityClass.Name))
		_, err = kClient.CreatePriorityClass(&preemptNotAllowPriorityClass)
		gomega.Ω(err).ShouldNot(HaveOccurred())

		// Define sleepPod
		sleepPod1Config := k8s.SleepPodConfig{Name: "sleepjob1", NS: dev, Mem: sleepPodMemLimit2, Time: 60, Optedout: k8s.NotConfig, Labels: map[string]string{"queue": "root.sandbox3"}}
		sleepPod2Config := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: sleepPodMemLimit2, Time: 60, Optedout: k8s.NotConfig, Labels: map[string]string{"queue": "root.sandbox3"}}
		sleepPod3Config := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, Mem: sleepPodMemLimit2, Time: 60, Optedout: k8s.NotConfig, Labels: map[string]string{"queue": "root.sandbox4"}}
		sleepPod4Config := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: sleepPodMemLimit2, Time: 60, Optedout: k8s.NotConfig, Labels: map[string]string{"queue": "root.sandbox4"}}
		sleepPod5Config := k8s.SleepPodConfig{Name: "sleepjob5", NS: dev, Mem: sleepPodMemLimit2, Time: 600, Optedout: k8s.NotConfig, Labels: map[string]string{"queue": "root.sandbox5"}}

		for _, config := range []k8s.SleepPodConfig{sleepPod1Config, sleepPod2Config, sleepPod3Config, sleepPod4Config} {
			ginkgo.By(fmt.Sprintf(deployMesg, config.Name))
			sleepObj, podErr := k8s.InitSleepPod(config)

			// Setting PriorityClasses for Pods in a specific queue
			if config.Name == "sleepjob3" || config.Name == "sleepjob4" {
				sleepObj.Spec.PriorityClassName = "preemption-not-allow"
			} else {
				sleepObj.Spec.PriorityClassName = "allow-preemption"
			}

			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				60*60*2)
			gomega.Ω(podErr).NotTo(gomega.HaveOccurred())
		}

		// Deploy sleepjob5 pod in root.sandbox5
		ginkgo.By(fmt.Sprintf(deployMesg, sleepPod5Config.Name))
		sleepObj, podErr := k8s.InitSleepPod(sleepPod5Config)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod5, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// sleepjob5 pod can be scheduled before pods in root.sandbox3 are succeeded
		ginkgo.By("The sleep pod " + sleepPod5Config.Name + " can be scheduled")
		err = kClient.WaitForPodScheduled(ns.Name, sleepRespPod5.Name, 90*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// assert one of the pods in root.sandbox3 is preempted
		ginkgo.By("One of the pods in root.sanbox4 is preempted")
		sandbox3RunningPodsCnt := 0
		pods, err := kClient.ListPodsByLabelSelector(dev, "queue=root.sandbox3")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if pod.Status.Phase == v1.PodRunning {
				sandbox3RunningPodsCnt++
			}
		}
		Ω(sandbox3RunningPodsCnt).To(gomega.Equal(1), "One of the pods in root.sandbox3 should be preempted")

		ginkgo.By(fmt.Sprintf("Removing priority class %s", preemptAllowPriorityClass.ObjectMeta.Name))
		err = kClient.DeletePriorityClass(preemptAllowPriorityClass.ObjectMeta.Name)
		gomega.Ω(err).ShouldNot(HaveOccurred())
		ginkgo.By(fmt.Sprintf("Removing priority class %s", preemptNotAllowPriorityClass.ObjectMeta.Name))
		err = kClient.DeletePriorityClass(preemptNotAllowPriorityClass.ObjectMeta.Name)
		gomega.Ω(err).ShouldNot(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})

		ginkgo.By("Tear down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())

		// reset config
		ginkgo.By("Restoring YuniKorn configuration")
		yunikorn.RestoreConfigMapWrapper(oldConfigMap)
	})
})

func createSandbox1SleepPodCofigs(cnt, time int, queueName string) []k8s.SleepPodConfig {
	sandbox1Configs := make([]k8s.SleepPodConfig, 0, cnt)
	for i := 0; i < cnt; i++ {
		sandbox1Configs = append(sandbox1Configs, k8s.SleepPodConfig{Name: fmt.Sprintf("sleepjob%d", i+1), NS: dev, Mem: sleepPodMemLimit, Time: time, Optedout: k8s.Allow, Labels: map[string]string{"queue": queueName}})
	}
	return sandbox1Configs
}
