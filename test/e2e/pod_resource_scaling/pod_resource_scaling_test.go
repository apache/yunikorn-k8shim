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

package pod_resource_scaling

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var err error
var ns string
var oldConfigMap = new(v1.ConfigMap)
var suiteName string

var _ = BeforeEach(func() {
	// Skip if K8s version < 1.32
	k8sVer, err := kClient.GetKubernetesVersion()
	Ω(err).NotTo(HaveOccurred())
	if k8sVer.Major < "1" || (k8sVer.Major == "1" && k8sVer.Minor < "32") {
		ginkgo.Skip("InPlacePodVerticalScaling requires K8s 1.32+")
	}

	// Create test namespace
	ns = "test-" + common.RandSeq(10)
	_, err = kClient.CreateNamespace(ns, nil)
	Ω(err).NotTo(HaveOccurred())
})

var _ = ginkgo.AfterEach(func() {
	By("Killing all pods")
	err := kClient.DeletePods(ns)
	Ω(err).NotTo(HaveOccurred())
	err = kClient.DeleteNamespace(ns)
	Ω(err).NotTo(HaveOccurred())
})

func verifyYunikornResourceUsage(appID, resourceName string, value int64) {
	err = utils.WaitForCondition(func() bool {
		app, err := restClient.GetAppInfo("default", "root."+ns, appID)
		if err != nil || app == nil {
			fmt.Println(err)
			return false
		}

		if app.Allocations == nil {
			fmt.Println(app)
			return false
		}

		for _, alloc := range app.Allocations {
			resVal, exists := alloc.ResourcePerAlloc[resourceName]
			if !exists {
				return false
			}

			if resVal == value {
				return true
			}
		}

		return false
	}, 30*time.Second, 120*time.Second)
	Ω(err).NotTo(HaveOccurred(), fmt.Sprintf("Pod should be scheduled by YuniKorn with correct resource(%s) allocation", resourceName))
}

var _ = ginkgo.Describe("InPlacePodVerticalScaling", func() {
	ginkgo.It("Pod resources(cpu/memory) resize up", func() {
		// Create pod with initial resources
		sleepPodConfigs := k8s.SleepPodConfig{NS: ns, Time: 600, CPU: 100, Mem: 100, QOSClass: v1.PodQOSGuaranteed}
		pod, err := k8s.InitSleepPod(sleepPodConfigs)
		Ω(err).NotTo(HaveOccurred())

		// Create pod
		pod, err = kClient.CreatePod(pod, ns)
		Ω(err).NotTo(HaveOccurred())

		// Wait for pod running
		err = kClient.WaitForPodRunning(ns, pod.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		// Check if pod is scheduled by YuniKorn and verify CPU allocation is 100m
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "vcore", 100)

		// Get initial pod restart count
		pod, err = kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialRestartCount := pod.Status.ContainerStatuses[0].RestartCount

		pod, err = kClient.ModifyResourceUsage(pod, ns, 200, 100)
		Ω(err).NotTo(HaveOccurred())

		Ω(pod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "vcore", 200)

		pod, err = kClient.ModifyResourceUsage(pod, ns, 200, 200)
		Ω(err).NotTo(HaveOccurred())

		Ω(pod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "memory", 200*1024*1024)
	})

	ginkgo.It("Pod resources(cpu/memory) resize down", func() {
		// Create pod with initial resources
		sleepPodConfigs := k8s.SleepPodConfig{NS: ns, Time: 600, CPU: 200, Mem: 200, QOSClass: v1.PodQOSGuaranteed}
		pod, err := k8s.InitSleepPod(sleepPodConfigs)
		Ω(err).NotTo(HaveOccurred())

		// Create pod
		pod, err = kClient.CreatePod(pod, ns)
		Ω(err).NotTo(HaveOccurred())

		// Wait for pod running
		err = kClient.WaitForPodRunning(ns, pod.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		// Check if pod is scheduled by YuniKorn and verify CPU allocation is 100m
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "vcore", 200)

		// Get initial pod state
		pod, err = kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialStartTime := pod.Status.StartTime
		initialRestartCount := pod.Status.ContainerStatuses[0].RestartCount

		pod, err = kClient.ModifyResourceUsage(pod, ns, 100, 200)
		Ω(err).NotTo(HaveOccurred())

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}
			return currentPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue() == int64(100)
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		Ω(err).NotTo(HaveOccurred())
		Ω(pod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(pod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "vcore", 100)

		pod, err = kClient.ModifyResourceUsage(pod, ns, 100, 100)
		Ω(err).NotTo(HaveOccurred()) // Expect an error as memory cannot be decreased

		Ω(err).NotTo(HaveOccurred())
		Ω(pod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(pod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "memory", 100*1024*1024)
	})

	ginkgo.It("Pod resources(cpu/memory) resize to excessive values should fail", func() {
		// Create pod with initial resources
		sleepPodConfigs := k8s.SleepPodConfig{NS: ns, Time: 600, CPU: 100, Mem: 100, QOSClass: v1.PodQOSGuaranteed}
		pod, err := k8s.InitSleepPod(sleepPodConfigs)
		Ω(err).NotTo(HaveOccurred())

		// Create pod
		_, err = kClient.CreatePod(pod, ns)
		Ω(err).NotTo(HaveOccurred())

		// Wait for pod running
		err = kClient.WaitForPodRunning(ns, pod.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		// Check if pod is scheduled by YuniKorn and verify CPU allocation is 100m
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "vcore", 100)

		// Get initial pod state
		pod, err = kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialStartTime := pod.Status.StartTime
		initialRestartCount := pod.Status.ContainerStatuses[0].RestartCount

		// Patch CPU/Memory to an excessive value
		pod, err = kClient.ModifyResourceUsage(pod, ns, 100000, 100000)
		Ω(err).NotTo(HaveOccurred())

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}
			return currentPod.Status.Resize == v1.PodResizeStatusInfeasible
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		Ω(err).NotTo(HaveOccurred())
		Ω(pod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(pod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")

		// Verify pod resource usage is unchanged after set an excessive value
		verifyYunikornResourceUsage(pod.ObjectMeta.Labels["applicationId"], "vcore", 100)
	})
})
