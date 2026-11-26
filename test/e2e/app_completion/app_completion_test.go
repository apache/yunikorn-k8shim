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

package appcompletion_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	// an app stays in Completing for 30 seconds before it moves to Completed
	completedTimeout     = 60
	numPods              = 50
	pod1SleepSec         = 30
	completingWait       = 29 * time.Second
	pod2SleepSec         = 5
	pod1SucceededTimeout = 120 * time.Second
	pod2PendingTimeout   = 20 * time.Second
	cpuRequest           = 5
	memRequest           = 30
	testScenarioLabelKey = "test-scenario"
	testScenarioLabelVal = "completing-orphan-race"
)

type caseOutcome int

const (
	outcomeScheduled caseOutcome = iota
	outcomeOrphanPending
	outcomeFailed
)

type caseResult struct {
	index    int
	appID    string
	pod2Name string
	outcome  caseOutcome
	err      error
}

var (
	suiteName    string
	kClient      k8s.KubeCtl
	restClient   yunikorn.RClient
	oldConfigMap = new(v1.ConfigMap)
	dev          string
)

var _ = ginkgo.Describe("", func() {
	ginkgo.BeforeEach(func() {
		dev = "dev" + common.RandSeq(5)
		ginkgo.By("create development namespace")
		ns, err := kClient.CreateNamespace(dev, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.It("Verify_Reuse_AppID_After_Completed", func() {
		appID := "app-" + common.RandSeq(5)
		runAndDeletePod(appID, sleepPod("sleep-first", appID))
		verifyAppCompleted(appID)

		ginkgo.By("Submit a new pod with the same application ID")
		verifyPodRuns(appID, sleepPod("sleep-second", appID))
	})

	ginkgo.It("Verify_Reuse_Autogen_AppID_After_Completed", func() {
		// no application ID on the pod, the admission controller generates one per namespace
		appID := utils.GenerateApplicationID(dev, false, "")
		runAndDeletePod(appID, podWithoutAppID("sleep-first"))
		verifyAppCompleted(appID)

		ginkgo.By("Submit a new pod without an application ID to the same namespace")
		verifyPodRuns(appID, podWithoutAppID("sleep-second"))
	})

	ginkgo.It("Verify_Reuse_AppID_While_Completing", func() {
		appID := "app-" + common.RandSeq(5)
		runAndDeletePod(appID, sleepPod("sleep-first", appID))

		ginkgo.By("Wait for the application to be Completing")
		err := restClient.WaitForAppStateTransition(configmanager.DefaultPartition, "root."+dev, appID,
			yunikorn.States().Application.Completing, 20)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Submit a new pod with the same application ID")
		verifyPodRuns(appID, sleepPod("sleep-second", appID))

		ginkgo.By("Verify the application does not complete")
		err = restClient.WaitForCompletedAppStateTransition(configmanager.DefaultPartition, appID,
			yunikorn.States().Application.Completed, completedTimeout)
		Ω(err).To(HaveOccurred())
	})

	ginkgo.It("Verify_Completing_App_No_Orphan_Tasks", func() {
		ginkgo.By(fmt.Sprintf("Running %d concurrent application cases (pod1 sleep=%ds, completing wait=%v)",
			numPods, pod1SleepSec, completingWait))

		results := make([]caseResult, numPods)
		var wg sync.WaitGroup
		wg.Add(numPods)

		for i := 1; i <= numPods; i++ {
			go func(idx int) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()
				results[idx-1] = runSingleCase(dev, idx)
			}(i)
		}

		wg.Wait()

		var scheduledCount int
		var orphanCases []string
		var failedCases []string

		for _, res := range results {
			switch res.outcome {
			case outcomeScheduled:
				scheduledCount++
			case outcomeOrphanPending:
				orphanCases = append(orphanCases, fmt.Sprintf("%s (%s)", res.appID, res.pod2Name))
			case outcomeFailed:
				failedCases = append(failedCases, fmt.Sprintf("%s: %v", res.appID, res.err))
			}
		}

		fmt.Fprintf(ginkgo.GinkgoWriter, "\n========================================\n")
		fmt.Fprintf(ginkgo.GinkgoWriter, "Test Summary\n")
		fmt.Fprintf(ginkgo.GinkgoWriter, "========================================\n")
		fmt.Fprintf(ginkgo.GinkgoWriter, "Total cases run       : %d\n", numPods)
		fmt.Fprintf(ginkgo.GinkgoWriter, "pod2 scheduled normal : %d\n", scheduledCount)
		fmt.Fprintf(ginkgo.GinkgoWriter, "pod2 stuck in Pending : %d\n", len(orphanCases))
		fmt.Fprintf(ginkgo.GinkgoWriter, "========================================\n")

		gomega.Expect(failedCases).To(gomega.BeEmpty(), "Unexpected errors during test cases: %v", failedCases)
		gomega.Expect(orphanCases).To(gomega.BeEmpty(),
			"Applications hit the Completing -> Completed orphan task race: %v", orphanCases)
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)
		ginkgo.By("Tear down namespace: " + dev)
		err = kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())
	})
})

func sleepPod(name string, appID string) *v1.Pod {
	pod, err := k8s.InitSleepPod(k8s.SleepPodConfig{Name: name, NS: dev, AppID: appID})
	Ω(err).NotTo(HaveOccurred())
	return pod
}

func podWithoutAppID(name string) *v1.Pod {
	pod, err := k8s.InitSleepPod(k8s.SleepPodConfig{Name: name, NS: dev})
	Ω(err).NotTo(HaveOccurred())
	delete(pod.Labels, constants.LabelApplicationID)
	return pod
}

// runAndDeletePod runs the only pod of the app and deletes it, which lets the app complete.
func runAndDeletePod(appID string, initPod *v1.Pod) {
	ginkgo.By("Run the first pod of application " + appID)
	verifyPodRuns(appID, initPod)
	ginkgo.By("Delete the pod")
	err := kClient.DeletePod(initPod.Name, dev)
	Ω(err).NotTo(HaveOccurred())
}

// verifyAppCompleted waits for the app in the completed list.
func verifyAppCompleted(appID string) {
	ginkgo.By("Wait for the application in the completed list")
	err := restClient.WaitForCompletedAppStateTransition(configmanager.DefaultPartition, appID,
		yunikorn.States().Application.Completed, completedTimeout)
	Ω(err).NotTo(HaveOccurred())
	appInfo, err := restClient.GetCompletedAppInfo(configmanager.DefaultPartition, appID)
	Ω(err).NotTo(HaveOccurred())
	Ω(appInfo.FinishedTime).NotTo(gomega.BeNil())
}

// verifyPodRuns creates the pod and checks that it runs in a Running app.
func verifyPodRuns(appID string, initPod *v1.Pod) {
	pod, err := kClient.CreatePod(initPod, dev)
	Ω(err).NotTo(HaveOccurred())
	Ω(pod.Labels[constants.LabelApplicationID]).To(gomega.Equal(appID))
	err = kClient.WaitForPodRunning(dev, pod.Name, 60*time.Second)
	Ω(err).NotTo(HaveOccurred())
	err = restClient.WaitForAppStateTransition(configmanager.DefaultPartition, "root."+dev, appID,
		yunikorn.States().Application.Running, 30)
	Ω(err).NotTo(HaveOccurred())
}

func runSingleCase(ns string, index int) caseResult {
	appID := fmt.Sprintf("case-%d", index)
	pod1Name := fmt.Sprintf("pod-%d", index)
	pod2Name := fmt.Sprintf("pod2-%d", index)
	labels := map[string]string{
		"queue":              "root." + ns,
		testScenarioLabelKey: testScenarioLabelVal,
	}

	pod1, err := k8s.InitSleepPod(k8s.SleepPodConfig{
		Name:   pod1Name,
		NS:     ns,
		AppID:  appID,
		Time:   pod1SleepSec,
		CPU:    cpuRequest,
		Mem:    memRequest,
		Labels: labels,
	})
	if err != nil {
		return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeFailed, err: err}
	}

	fmt.Fprintf(ginkgo.GinkgoWriter, "[%s] Creating %s\n", appID, pod1Name)
	_, err = kClient.CreatePod(pod1, ns)
	if err != nil {
		return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeFailed, err: err}
	}

	err = waitForPodSucceeded(ns, pod1Name, pod1SucceededTimeout)
	if err != nil {
		return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeFailed, err: err}
	}

	fmt.Fprintf(ginkgo.GinkgoWriter, "[%s] Pod %s is in Completed (Succeeded) state. Sleeping %v...\n",
		appID, pod1Name, completingWait)
	time.Sleep(completingWait)

	pod2, err := k8s.InitSleepPod(k8s.SleepPodConfig{
		Name:   pod2Name,
		NS:     ns,
		AppID:  appID,
		Time:   pod2SleepSec,
		CPU:    cpuRequest,
		Mem:    memRequest,
		Labels: labels,
	})
	if err != nil {
		return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeFailed, err: err}
	}

	fmt.Fprintf(ginkgo.GinkgoWriter, "[%s] Creating %s\n", appID, pod2Name)
	_, err = kClient.CreatePod(pod2, ns)
	if err != nil {
		return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeFailed, err: err}
	}

	scheduled := waitForPod2Scheduled(ns, appID, pod2Name, pod2PendingTimeout)
	if !scheduled {
		fmt.Fprintf(ginkgo.GinkgoWriter, "[%s] REPRODUCED: %s remained Pending after %v!\n",
			appID, pod2Name, pod2PendingTimeout)
		return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeOrphanPending}
	}

	return caseResult{index: index, appID: appID, pod2Name: pod2Name, outcome: outcomeScheduled}
}

func waitForPodSucceeded(ns, podName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.Background(), time.Second, timeout, false,
		func(ctx context.Context) (bool, error) {
			pod, err := kClient.GetPod(podName, ns)
			if err != nil {
				return false, nil
			}
			switch pod.Status.Phase {
			case v1.PodSucceeded:
				return true, nil
			case v1.PodFailed:
				return false, fmt.Errorf("pod %s failed unexpectedly", podName)
			default:
				return false, nil
			}
		})
}

func waitForPod2Scheduled(ns, appID, pod2Name string, timeout time.Duration) bool {
	err := wait.PollUntilContextTimeout(context.Background(), time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := kClient.GetPod(pod2Name, ns)
			if err != nil {
				return false, nil
			}
			if pod.Spec.NodeName != "" || pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded {
				fmt.Fprintf(ginkgo.GinkgoWriter, "[%s] %s scheduled successfully (phase=%s, node=%s)\n",
					appID, pod2Name, pod.Status.Phase, pod.Spec.NodeName)
				return true, nil
			}
			return false, nil
		})
	return err == nil
}
