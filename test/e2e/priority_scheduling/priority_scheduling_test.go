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
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

const (
	requestCPU = "100m"
	requestMem = "100M"
)

var (
	ns string
	rr = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse(requestCPU),
			v1.ResourceMemory: resource.MustParse(requestMem),
		},
	}
)

var _ = ginkgo.Describe("PriorityScheduling", func() {
	var namespace *v1.Namespace
	var err error
	var oldConfigMap = new(v1.ConfigMap)
	var annotation string
	var sleepPodConf, lowPodConf, normalPodConf, highPodConf k8s.TestPodConfig

	ginkgo.BeforeEach(func() {
		ns = "test-" + common.RandSeq(10)

		By(fmt.Sprintf("Creating test namespace %s", ns))
		namespace, err = kubeClient.CreateNamespace(ns, map[string]string{})
		Ω(err).ShouldNot(HaveOccurred())
		Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))
	})

	ginkgo.It("Verify_Static_Queue_App_Scheduling_Order", func() {
		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fifo", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "fence",
				Parent:     true,
				Resources:  configs.Resources{Max: map[string]string{siCommon.CPU: requestCPU, siCommon.Memory: requestMem}},
				Properties: map[string]string{configs.PriorityPolicy: "fence"},
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
		validatePodSchedulingOrder(ns, sleepPodConf, lowPodConf, normalPodConf, highPodConf)
	})

	ginkgo.It("Verify_Dynamic_Queue_App_Scheduling_Order", func() {
		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo", annotation)

		By(fmt.Sprintf("Update test namespace quota %s", ns))
		namespace, err = kubeClient.UpdateNamespace(ns, map[string]string{
			constants.NamespaceQuota: fmt.Sprintf("{\"%s\": \"%s\", \"%s\": \"%s\"}", v1.ResourceCPU, requestCPU, v1.ResourceMemory, requestMem),
		})
		Ω(err).ShouldNot(HaveOccurred())
		Ω(namespace.Status.Phase).Should(Equal(v1.NamespaceActive))

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
		validatePodSchedulingOrder(ns, sleepPodConf, lowPodConf, normalPodConf, highPodConf)
	})

	ginkgo.It("Verify_Priority_Offset_Queue_App_Scheduling_Order", func() {
		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fifo", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       "priority",
				Parent:     true,
				Resources:  configs.Resources{Max: map[string]string{siCommon.CPU: "100m", siCommon.Memory: "100M"}},
				Properties: map[string]string{configs.PriorityPolicy: "fence"},
			}); err != nil {
				return err
			}
			if err = common.AddQueue(sc, "default", "root.priority", configs.QueueConfig{
				Name:       "high",
				Properties: map[string]string{configs.PriorityOffset: "100"},
			}); err != nil {
				return err
			}
			if err = common.AddQueue(sc, "default", "root.priority", configs.QueueConfig{
				Name:       "normal",
				Properties: map[string]string{configs.PriorityOffset: "0"},
			}); err != nil {
				return err
			}
			if err = common.AddQueue(sc, "default", "root.priority", configs.QueueConfig{
				Name:       "low",
				Properties: map[string]string{configs.PriorityOffset: "-100"},
			}); err != nil {
				return err
			}

			return nil
		})

		sleepPodConf = k8s.TestPodConfig{
			Name: "test-sleep-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.priority.high",
				constants.LabelApplicationID: "app-sleep-" + common.RandSeq(5)},
			Namespace: ns,
			Resources: rr,
		}

		lowPodConf = k8s.TestPodConfig{
			Name: "test-low-priority-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.priority.low",
				constants.LabelApplicationID: "app-low-" + common.RandSeq(5)},
			Namespace: ns,
			Resources: rr,
		}

		normalPodConf = k8s.TestPodConfig{
			Name: "test-normal-priority-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.priority.normal",
				constants.LabelApplicationID: "app-normal-" + common.RandSeq(5)},
			Resources: rr,
			Namespace: ns,
		}

		highPodConf = k8s.TestPodConfig{
			Name: "test-high-priority-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.priority.high",
				constants.LabelApplicationID: "app-high-" + common.RandSeq(5)},
			Namespace: ns,
			Resources: rr,
		}
		validatePodSchedulingOrder(ns, sleepPodConf, lowPodConf, normalPodConf, highPodConf)
	})

	ginkgo.It("Verify_Gang_Scheduling_With_Priority", func() {
		By("Setting custom YuniKorn configuration")
		annotation = "ann-" + common.RandSeq(10)
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fifo", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil

			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:      "default",
				Parent:    false,
				Resources: configs.Resources{Max: map[string]string{siCommon.CPU: "100m", siCommon.Memory: "100M"}},
			}); err != nil {
				return err
			}

			return nil
		})
		sleepPodConf = k8s.TestPodConfig{
			Name: "test-sleep-" + common.RandSeq(5),
			Labels: map[string]string{
				constants.LabelQueueName:     "root.default",
				constants.LabelApplicationID: "app-sleep-" + common.RandSeq(5)},
			Namespace: ns,
			Resources: rr,
		}

		taskGroupMinResource := map[string]resource.Quantity{}
		for k, v := range rr.Requests {
			taskGroupMinResource[k.String()] = v
		}
		lowPodConf = createPodConfWithTaskGroup("low", lowPriorityClass.Name, taskGroupMinResource)
		normalPodConf = createPodConfWithTaskGroup("normal", normalPriorityClass.Name, taskGroupMinResource)
		highPodConf = createPodConfWithTaskGroup("high", highPriorityClass.Name, taskGroupMinResource)

		var sleepPod, lowPod, normalPod, highPod *v1.Pod
		By("Create sleep pod to consume queue")
		sleepPod, err = k8s.InitTestPod(sleepPodConf)
		Ω(err).NotTo(gomega.HaveOccurred())
		sleepPod, err = kubeClient.CreatePod(sleepPod, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kubeClient.WaitForPodRunning(ns, sleepPod.Name, 1*time.Minute)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Submit low priority job")
		lowPod, err = k8s.InitTestPod(lowPodConf)
		Ω(err).NotTo(gomega.HaveOccurred())
		lowJob := k8s.InitTestJob(lowPod.Labels[constants.LabelApplicationID], 1, 1, lowPod)
		lowJob, err = kubeClient.CreateJob(lowJob, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kubeClient.WaitForJobPodsCreated(ns, lowJob.Name, 1, 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Submit normal priority job")
		normalPod, err = k8s.InitTestPod(normalPodConf)
		Ω(err).NotTo(gomega.HaveOccurred())
		normalJob := k8s.InitTestJob(normalPod.Labels[constants.LabelApplicationID], 1, 1, normalPod)
		normalJob, err = kubeClient.CreateJob(normalJob, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kubeClient.WaitForJobPodsCreated(ns, normalJob.Name, 1, 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Submit high priority job")
		highPod, err = k8s.InitTestPod(highPodConf)
		Ω(err).NotTo(gomega.HaveOccurred())
		highJob := k8s.InitTestJob(highPod.Labels[constants.LabelApplicationID], 1, 1, highPod)
		highJob, err = kubeClient.CreateJob(highJob, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kubeClient.WaitForJobPodsCreated(ns, highJob.Name, 1, 30*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Wait for scheduler state to settle")
		time.Sleep(10 * time.Second)

		var lowPods, normalPods, highPods *v1.PodList
		lowPods, err = kubeClient.ListPods(ns, fmt.Sprintf("job-name=%s", lowJob.Name))
		Ω(err).NotTo(gomega.HaveOccurred())
		lowPod = &lowPods.Items[0]

		normalPods, err = kubeClient.ListPods(ns, fmt.Sprintf("job-name=%s", normalJob.Name))
		Ω(err).NotTo(gomega.HaveOccurred())
		normalPod = &normalPods.Items[0]

		highPods, err = kubeClient.ListPods(ns, fmt.Sprintf("job-name=%s", highJob.Name))
		Ω(err).NotTo(gomega.HaveOccurred())
		highPod = &highPods.Items[0]

		By("Ensure no test pods are running")
		ensureNotRunning(ns, lowPod, normalPod, highPod)

		By("Kill sleep pod to make room for test pods")
		err = kubeClient.DeletePod(sleepPod.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Wait for high-priority placeholders terminated")
		err = kubeClient.WaitForPlaceholders(ns, "tg-"+highPodConf.Labels["applicationId"]+"-", 0, 30*time.Second, nil)
		Ω(err).NotTo(HaveOccurred())

		By("Wait for high-priority pod to begin running")
		err = kubeClient.WaitForPodRunning(ns, highPod.Name, 1*time.Minute)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Ensure low and normal priority pods are not running")
		ensureNotRunning(ns, lowPod, normalPod)

		By("Kill high-priority job")
		err = kubeClient.DeleteJob(highJob.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Wait for normal-priority placeholders terminated")
		err = kubeClient.WaitForPlaceholders(ns, "tg-"+normalPodConf.Labels["applicationId"]+"-", 0, 30*time.Second, nil)
		Ω(err).NotTo(HaveOccurred())

		By("Wait for normal-priority pod to begin running")
		err = kubeClient.WaitForPodRunning(ns, normalPod.Name, 1*time.Minute)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Ensure low priority pod is not running")
		ensureNotRunning(ns, lowPod)

		By("Kill normal-priority job")
		err = kubeClient.DeleteJob(normalJob.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Wait for low-priority placeholders terminated")
		err = kubeClient.WaitForPlaceholders(ns, "tg-"+lowPodConf.Labels["applicationId"]+"-", 0, 30*time.Second, nil)
		Ω(err).NotTo(HaveOccurred())

		By("Wait for low-priority pod to begin running")
		err = kubeClient.WaitForPodRunning(ns, lowPod.Name, 1*time.Minute)
		Ω(err).NotTo(gomega.HaveOccurred())

		By("Kill low-priority job")
		err = kubeClient.DeleteJob(lowJob.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{ns})

		// If there is any error test case, we need to delete all pods to make sure it doesn't influence other cases.
		ginkgo.By("Delete all sleep pods")
		err = kubeClient.DeletePods(ns)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pods in namespace %s - reason is %s\n", ns, err.Error())
		}

		By(fmt.Sprintf("Tearing down namespace %s", ns))
		err = kubeClient.TearDownNamespace(ns)
		Ω(err).ShouldNot(HaveOccurred())

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

func createPodConfWithTaskGroup(name, priorityClassName string, taskGroupMinResource map[string]resource.Quantity) k8s.TestPodConfig {
	return k8s.TestPodConfig{
		Name: fmt.Sprintf("test-%s-priority-%s", name, common.RandSeq(5)),
		Labels: map[string]string{
			constants.LabelQueueName:     "root.default",
			constants.LabelApplicationID: fmt.Sprintf("app-%s-%s", name, common.RandSeq(5))},
		Namespace: ns,
		Annotations: &k8s.PodAnnotation{
			TaskGroupName: "group-" + name,
			TaskGroups: []cache.TaskGroup{
				{
					Name:        "group-" + name,
					MinMember:   int32(1),
					MinResource: taskGroupMinResource,
				},
			},
		},
		Resources:         rr,
		PriorityClassName: priorityClassName,
		RestartPolicy:     v1.RestartPolicyNever,
	}
}
