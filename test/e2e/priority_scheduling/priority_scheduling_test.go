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

package priority_test

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var rr = &v1.ResourceRequirements{
	Requests: v1.ResourceList{
		"cpu":    resource.MustParse("100m"),
		"memory": resource.MustParse("100M"),
	},
}

var _ = ginkgo.Describe("Static_Queue_Priority", func() {
	var ns string
	var err error
	var oldConfigMap = new(v1.ConfigMap)
	var annotation string
	var sleepPodConf, lowPodConf, normalPodConf, highPodConf k8s.TestPodConfig

	ginkgo.BeforeEach(func() {
		var namespace *v1.Namespace

		ns = "test-" + common.RandSeq(10)

		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fifo", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "fence",
				Parent:     true,
				Resources:  configs.Resources{Max: map[string]string{"vcore": "100m", "memory": "100M"}},
				Properties: map[string]string{"priority.policy": "fence"},
			}); err != nil {
				return err
			}
			if err = common.AddQueue(sc, "default", "root.fence", configs.QueueConfig{Name: "child1"}); err != nil {
				return err
			}
			if err = common.AddQueue(sc, "default", "root.fence", configs.QueueConfig{Name: "child2"}); err != nil {
				return err
			}

			return nil
		})

		By(fmt.Sprintf("Creating test namespace %s", ns))
		namespace, err = kubeClient.CreateNamespace(ns, map[string]string{})
		Ω(err).ShouldNot(HaveOccurred())
		Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))

		rr = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("100m"),
				"memory": resource.MustParse("100M"),
			},
		}

		sleepPodConf = k8s.TestPodConfig{
			Name: "test-sleep-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.fence.child2",
				constants.LabelApplicationID: "app-sleep-" + common.RandSeq(5)},
			Namespace: ns,
			Resources: rr,
		}

		lowPodConf = k8s.TestPodConfig{
			Name: "test-low-priority-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.fence.child1",
				constants.LabelApplicationID: "app-low-" + common.RandSeq(5)},
			Namespace:         ns,
			Resources:         rr,
			PriorityClassName: lowPriorityClass.Name,
		}

		normalPodConf = k8s.TestPodConfig{
			Name: "test-normal-priority-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.fence.child2",
				constants.LabelApplicationID: "app-normal-" + common.RandSeq(5)},
			Resources:         rr,
			Namespace:         ns,
			PriorityClassName: normalPriorityClass.Name,
		}

		highPodConf = k8s.TestPodConfig{
			Name: "test-high-priority-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.fence.child1",
				constants.LabelApplicationID: "app-high-" + common.RandSeq(5)},
			Namespace:         ns,
			Resources:         rr,
			PriorityClassName: highPriorityClass.Name,
		}
	})

	ginkgo.It("Verify_Dynamic_Queue_Scheduling_Order", func() {
		validatePodSchedulingOrder(ns, sleepPodConf, lowPodConf, normalPodConf, highPodConf)
	})

	ginkgo.AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		By(fmt.Sprintf("Tearing down namespace %s", ns))
		err = kubeClient.TearDownNamespace(ns)
		Ω(err).ShouldNot(HaveOccurred())

		By("Restoring YuniKorn configuration")
		yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
	})
})

var _ = ginkgo.Describe("Dynamic_Queue_Priority", func() {
	var ns string
	var err error
	var oldConfigMap = new(v1.ConfigMap)
	var annotation string
	var sleepPodConf, lowPodConf, normalPodConf, highPodConf k8s.TestPodConfig

	ginkgo.BeforeEach(func() {
		var namespace *v1.Namespace

		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo", annotation)

		ns = "test-" + common.RandSeq(10)

		By(fmt.Sprintf("Creating test namespace %s", ns))
		namespace, err = kubeClient.CreateNamespace(ns, map[string]string{
			constants.NamespaceQuota: "{\"cpu\": \"100m\", \"memory\": \"100M\"}"})
		Ω(err).ShouldNot(HaveOccurred())
		Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))

		rr = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("100m"),
				"memory": resource.MustParse("100M"),
			},
		}

		sleepPodConf = k8s.TestPodConfig{
			Name:      "test-sleep-" + common.RandSeq(5),
			Labels:    map[string]string{constants.LabelApplicationID: "app-sleep-" + common.RandSeq(5)},
			Namespace: ns,
			Resources: rr,
		}

		lowPodConf = k8s.TestPodConfig{
			Name:              "test-low-priority-" + common.RandSeq(5),
			Labels:            map[string]string{constants.LabelApplicationID: "app-low-" + common.RandSeq(5)},
			Namespace:         ns,
			Resources:         rr,
			PriorityClassName: lowPriorityClass.Name,
		}

		normalPodConf = k8s.TestPodConfig{
			Name:              "test-normal-priority-" + common.RandSeq(5),
			Labels:            map[string]string{constants.LabelApplicationID: "app-normal-" + common.RandSeq(5)},
			Resources:         rr,
			Namespace:         ns,
			PriorityClassName: normalPriorityClass.Name,
		}

		highPodConf = k8s.TestPodConfig{
			Name:              "test-high-priority-" + common.RandSeq(5),
			Labels:            map[string]string{constants.LabelApplicationID: "app-high-" + common.RandSeq(5)},
			Namespace:         ns,
			Resources:         rr,
			PriorityClassName: highPriorityClass.Name,
		}
	})

	ginkgo.It("Verify_Dynamic_Queue_App_Scheduling_Order", func() {
		validatePodSchedulingOrder(ns, sleepPodConf, lowPodConf, normalPodConf, highPodConf)
	})

	ginkgo.AfterEach(func() {
		By("Tear down namespace: " + ns)
		err := kubeClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())

		By("Restoring YuniKorn configuration")
		yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
	})
})

func validatePodSchedulingOrder(ns string, sleepPodConf, lowPodConf, normalPodConf, highPodConf k8s.TestPodConfig) {
	var err error
	var sleepPod *v1.Pod

	By("Create sleep pod to consume queue")
	sleepPod, err = k8s.InitTestPod(sleepPodConf)
	Ω(err).NotTo(gomega.HaveOccurred())
	sleepPod, err = kubeClient.CreatePod(sleepPod, ns)
	Ω(err).NotTo(gomega.HaveOccurred())
	err = kubeClient.WaitForPodRunning(ns, sleepPod.Name, 30*time.Second)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Submit low priority pod")
	lowPod, err := k8s.InitTestPod(lowPodConf)
	Ω(err).NotTo(gomega.HaveOccurred())
	lowPod, err = kubeClient.CreatePod(lowPod, ns)
	Ω(err).NotTo(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)

	By("Submit normal priority pod")
	normalPod, err := k8s.InitTestPod(normalPodConf)
	Ω(err).NotTo(gomega.HaveOccurred())
	normalPod, err = kubeClient.CreatePod(normalPod, ns)
	Ω(err).NotTo(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)

	By("Submit high priority pod")
	highPod, err := k8s.InitTestPod(highPodConf)
	Ω(err).NotTo(gomega.HaveOccurred())
	highPod, err = kubeClient.CreatePod(highPod, ns)
	Ω(err).NotTo(gomega.HaveOccurred())
	time.Sleep(1 * time.Second)

	By("Wait for scheduler state to settle")
	time.Sleep(10 * time.Second)

	By("Ensure no test pods are running")
	ensureNotRunning(ns, lowPod, normalPod, highPod)

	By("Kill sleep pod to make room for test pods")
	err = kubeClient.DeletePod(sleepPod.Name, ns)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Wait for high-priority pod to begin running")
	err = kubeClient.WaitForPodRunning(ns, highPod.Name, 30*time.Second)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Ensure low and normal priority pods are not running")
	ensureNotRunning(ns, lowPod, normalPod)

	By("Kill high-priority pod")
	err = kubeClient.DeletePod(highPod.Name, ns)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Wait for normal-priority pod to begin running")
	err = kubeClient.WaitForPodRunning(ns, normalPod.Name, 30*time.Second)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Ensure low priority pod is not running")
	ensureNotRunning(ns, lowPod)

	By("Kill normal-priority pod")
	err = kubeClient.DeletePod(normalPod.Name, ns)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Wait for low-priority pod to begin running")
	err = kubeClient.WaitForPodRunning(ns, lowPod.Name, 30*time.Second)
	Ω(err).NotTo(gomega.HaveOccurred())

	By("Kill low-priority pod")
	err = kubeClient.DeletePod(lowPod.Name, ns)
	Ω(err).NotTo(gomega.HaveOccurred())
}

func ensureNotRunning(ns string, pods ...*v1.Pod) {
	for _, pod := range pods {
		podResult, err := kubeClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(podResult.Status.Phase).ShouldNot(Equal(v1.PodRunning), pod.Name)
	}
}
