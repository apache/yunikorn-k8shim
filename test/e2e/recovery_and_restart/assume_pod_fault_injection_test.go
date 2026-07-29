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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = ginkgo.Describe("AssumePodFaultInjection", func() {
	var faultDev string

	ginkgo.BeforeEach(func() {
		faultDev = "fault-dev-" + common.RandSeq(5)

		ginkgo.By("Create test namespace " + faultDev)
		ns, err := kClient.CreateNamespace(faultDev, nil)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Clear fault injection")
		_ = kClient.SetFaultInject(configmanager.YuniKornTestConfig.YkNamespace, false)

		ginkgo.By("Tear down test namespace " + faultDev)
		err := kClient.TearDownNamespace(faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())

		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{faultDev})
	})

	// ---------------------------------------------------------------------------
	// Test 1: transient failures absorbed by the retry loop — no rollback fires
	// ---------------------------------------------------------------------------
	ginkgo.It("Verify_transient_AssumePod_error_retried_without_rollback", func() {
		sleepPodConfig := k8s.SleepPodConfig{Name: "fault-transient", NS: faultDev}
		podObj, err := k8s.InitSleepPod(sleepPodConfig)
		Ω(err).NotTo(gomega.HaveOccurred())
		appID := podObj.Labels["applicationId"]

		ginkgo.By("Enable fault injection (transient: will disable after 3s while retry loop is live)")
		Ω(kClient.SetFaultInject(configmanager.YuniKornTestConfig.YkNamespace, true)).
			NotTo(gomega.HaveOccurred())

		ginkgo.By("Submit pod")
		pod, err := kClient.CreatePod(podObj, faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())
		podName := pod.Name

		ginkgo.By("Wait 3s then disable fault injection — retry loop should still be active")
		time.Sleep(3 * time.Second)
		Ω(kClient.SetFaultInject(configmanager.YuniKornTestConfig.YkNamespace, false)).
			NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for pod to reach Running state")
		err = kClient.WaitForPodBySelectorRunning(faultDev, fmt.Sprintf("applicationId=%s", appID), 120)
		Ω(err).NotTo(gomega.HaveOccurred())

		// --- Assertions ---

		ginkgo.By("Assert no AssumePodFailed event on the pod (rollback path was NOT taken)")
		events, err := kClient.GetEvents(faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())
		for _, e := range events.Items {
			if e.InvolvedObject.Name == podName {
				Ω(e.Reason).NotTo(gomega.Equal("AssumePodFailed"),
					"Expected no AssumePodFailed event but found one")
			}
		}

		ginkgo.By("Assert app state = Running in YuniKorn core")
		err = restClient.WaitForAppStateTransition(configmanager.DefaultPartition, "root."+faultDev, appID, yunikorn.States().Application.Running, 30)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assert 1 allocation, 0 pending asks in app info")
		appInfo, err := restClient.GetAppInfo(configmanager.DefaultPartition, "root."+faultDev, appID)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(appInfo.Allocations).To(gomega.HaveLen(1), "expected exactly 1 allocation")
		Ω(appInfo.Requests).To(gomega.BeEmpty(), "expected no pending asks")

		ginkgo.By("Assert pod's node shows resources allocated in YuniKorn")
		runningPod, err := kClient.GetPod(podName, faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(runningPod.Spec.NodeName).NotTo(gomega.BeEmpty())
		nodes, err := restClient.GetNodes(configmanager.DefaultPartition)
		Ω(err).NotTo(gomega.HaveOccurred())
		nodeFound := false
		for _, n := range *nodes {
			if n.NodeID == runningPod.Spec.NodeName {
				nodeFound = true
				Ω(n.Allocated).NotTo(gomega.BeEmpty(), "expected allocated resources on the node")
			}
		}
		Ω(nodeFound).To(gomega.BeTrue(), "scheduled node not found in YuniKorn node list")
	})

	// ---------------------------------------------------------------------------
	// Test 2: all retry steps fail → rollback fires → re-queued → pod eventually runs
	// ---------------------------------------------------------------------------
	ginkgo.It("Verify_persistent_AssumePod_error_triggers_rollback_then_reschedule", func() {
		sleepPodConfig := k8s.SleepPodConfig{Name: "fault-persistent", NS: faultDev}
		podObj, err := k8s.InitSleepPod(sleepPodConfig)
		Ω(err).NotTo(gomega.HaveOccurred())
		appID := podObj.Labels["applicationId"]

		ginkgo.By("Enable fault injection (persistent: stays on until manually cleared)")
		Ω(kClient.SetFaultInject(configmanager.YuniKornTestConfig.YkNamespace, true)).
			NotTo(gomega.HaveOccurred())

		ginkgo.By("Submit pod")
		pod, err := kClient.CreatePod(podObj, faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())
		podName := pod.Name

		ginkgo.By("Wait for AssumePodFailed Warning event on the pod (proves rollback path was taken)")
		err = kClient.WaitForPodEvent(faultDev, podName, "AssumePodFailed", 60*time.Second)
		Ω(err).NotTo(gomega.HaveOccurred())

		// --- Mid-rollback assertions (injector still armed) ---
		// With persistent injection the core immediately re-schedules after rollback,
		// so the allocation cycles continuously. Only assert on stable state.

		ginkgo.By("Snapshot state dump after first rollback")
		dump, err := restClient.GetFullStateDump()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(dump).NotTo(gomega.BeEmpty())

		ginkgo.By("Assert app is NOT in Failed state (it rolled back, not terminated)")
		appInfo, err := restClient.GetAppInfo(configmanager.DefaultPartition, "root."+faultDev, appID)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(appInfo.State).NotTo(gomega.Equal("Failed"),
			"app should not be Failed after rollback — it should be re-queued or re-scheduled")

		ginkgo.By("Assert pod is still Pending (never bound to the node)")
		pendingPod, err := kClient.GetPod(podName, faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(pendingPod.Status.Phase).To(gomega.Equal(v1.PodPending),
			"pod should still be Pending while fault injection is active")

		// --- Recovery ---

		ginkgo.By("Disable fault injection to allow re-schedule to succeed")
		Ω(kClient.SetFaultInject(configmanager.YuniKornTestConfig.YkNamespace, false)).
			NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait for pod to reach Running state after re-schedule")
		err = kClient.WaitForPodBySelectorRunning(faultDev, fmt.Sprintf("applicationId=%s", appID), 120)
		Ω(err).NotTo(gomega.HaveOccurred())

		// --- Post-recovery assertions ---

		ginkgo.By("Assert app state = Running after re-schedule")
		err = restClient.WaitForAppStateTransition(configmanager.DefaultPartition, "root."+faultDev, appID, yunikorn.States().Application.Running, 30)
		Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Assert 1 allocation, 0 pending asks after successful re-schedule")
		appInfo, err = restClient.GetAppInfo(configmanager.DefaultPartition, "root."+faultDev, appID)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(appInfo.Allocations).To(gomega.HaveLen(1), "expected exactly 1 allocation after re-schedule")
		Ω(appInfo.Requests).To(gomega.BeEmpty(), "expected no pending asks after re-schedule")

		ginkgo.By("Assert pod's node shows resources allocated in YuniKorn")
		runningPod, err := kClient.GetPod(podName, faultDev)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(runningPod.Spec.NodeName).NotTo(gomega.BeEmpty())
		nodes, err := restClient.GetNodes(configmanager.DefaultPartition)
		Ω(err).NotTo(gomega.HaveOccurred())
		nodeFound := false
		for _, n := range *nodes {
			if n.NodeID == runningPod.Spec.NodeName {
				nodeFound = true
				Ω(n.Allocated).NotTo(gomega.BeEmpty())
			}
		}
		Ω(nodeFound).To(gomega.BeTrue(), "scheduled node not found in YuniKorn node list")

		ginkgo.By("Assert scheduler is healthy")
		health, err := restClient.GetHealthCheck()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(health.Healthy).To(gomega.BeTrue(), "scheduler should be healthy after fault injection and recovery")
	})
})
