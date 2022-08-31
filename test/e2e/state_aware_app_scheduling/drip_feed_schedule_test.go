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

package stateawareappscheduling_test

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("DripFeedSchedule:", func() {

	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var app1 = "app01-" + common.RandSeq(5)
	var app2 = "app02-" + common.RandSeq(5)
	var app3 = "app03-" + common.RandSeq(5)
	var ns = "sleep-" + common.RandSeq(10)
	var testTimeout float64 = 360
	var running = yunikorn.States().Application.Running
	var accepted = yunikorn.States().Application.Accepted
	var starting = yunikorn.States().Application.Starting

	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		By(fmt.Sprintf("Creating namespace: %s for sleep jobs", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	It("Test_State_Aware_App_Sorting", func() {
		By("Submit 3 apps(app01, app02, app03) with one pod each")
		for _, appID := range []string{app1, app2, app3} {
			podName := "pod1-" + common.RandSeq(5)
			sleepPodConf := k8s.SleepPodConfig{Name: podName, NS: ns, AppID: appID}
			initPod, podErr := k8s.InitSleepPod(sleepPodConf)
			Ω(podErr).NotTo(HaveOccurred())
			Ω(err).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			time.Sleep(3 * time.Second) // Buffer time between pod submission
		}

		By(fmt.Sprintf("Get apps from specific queue: %s", ns))
		var appsFromQueue []map[string]interface{}
		// Poll for apps to appear in the queue
		err = wait.PollImmediate(time.Second, time.Duration(60)*time.Second, func() (done bool, err error) {
			appsFromQueue, err = restClient.GetApps("default", "root."+ns)
			if len(appsFromQueue) == 3 {
				return true, nil
			}
			return false, err
		})
		Ω(err).NotTo(HaveOccurred())
		Ω(appsFromQueue).NotTo(BeEmpty())

		/*
			At this point, the apps state will be
			app01 - Starting
			app02 - Accepted
			app03 - Accepted
		*/
		By(fmt.Sprintf("Verify the app %s are in Starting state", app1))
		err = restClient.WaitForAppStateTransition("default", "root."+ns, app1, starting, 60)
		Ω(err).NotTo(HaveOccurred())
		By(fmt.Sprintf("Verify the app %s are in Accepted state", app2))
		err = restClient.WaitForAppStateTransition("default", "root."+ns, app2, accepted, 60)
		Ω(err).NotTo(HaveOccurred())
		By(fmt.Sprintf("Verify the app %s are in Accepted state", app3))
		err = restClient.WaitForAppStateTransition("default", "root."+ns, app3, accepted, 60)
		Ω(err).NotTo(HaveOccurred())

		/*
			Add another pod for app01, and once this pod is allocated, verify app states:
				app01 - Running => pod1, pod2
				app02 - Starting => pod1
				app03 - Accepted => pod1

			Add another pod for app02, and once this pod is allocated, verify app states:
				app01 - Running => pod1, pod2
				app02 - Running => pod1, pod2
				app03 - starting => pod1

			Add another pod for app03, and once this pod is allocated, verify app states:
				app01 - Running => pod1, pod2
				app02 - Running => pod1, pod2
				app03 - Running => pod1, pod2
		*/
		var appStates = map[string][]string{
			app1: {running, starting, accepted},
			app2: {running, running, starting},
			app3: {running, running, running},
		}
		for _, appID := range []string{app1, app2, app3} {
			By(fmt.Sprintf("Add one more pod to the app: %s", appID))
			podName := "pod2-" + common.RandSeq(5)
			sleepPodConf := k8s.SleepPodConfig{Name: podName, NS: ns, AppID: appID}
			initPod, podErr := k8s.InitSleepPod(sleepPodConf)
			Ω(podErr).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			By(fmt.Sprintf("Verify that the app: %s is in running state", appID))
			err = restClient.WaitForAppStateTransition("default", "root."+ns, app1, appStates[appID][0], 60)
			Ω(err).NotTo(HaveOccurred())
			err = restClient.WaitForAppStateTransition("default", "root."+ns, app2, appStates[appID][1], 60)
			Ω(err).NotTo(HaveOccurred())
			err = restClient.WaitForAppStateTransition("default", "root."+ns, app3, appStates[appID][2], 60)
			Ω(err).NotTo(HaveOccurred())
		}

	}, testTimeout)

	AfterEach(func() {
		By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(Equal(""), checks)

		By("Tearing down namespace: " + ns)
		err = kClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
