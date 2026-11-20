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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

// an app stays in Completing for 30 seconds before it moves to Completed
const completedTimeout = 60

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)
var dev string

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
