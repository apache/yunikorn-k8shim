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

package resourcefairness_test

import (
	"fmt"
	"math/rand"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("FairScheduling:", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var ns = "default"

	var maxCPU int64 = 500
	var maxMem int64 = 500

	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
	})

	// Validates waitQueue order of requested app resources, according to fairAppScheduling.
	// Step 1: Deploy 4 apps, which sum to 95% queueCPU / 35% queueMem
	//              -> Resource priority order: 1)CPU. 2)Mem
	// Step 2: Deploy 1 more blocked pod each for apps0-2
	// Step 3: Kill an App3 pod.
	// Step 4: App with least cpu use is allocated pod next. Break tie using mem.
	It("Test_Wait_Queue_Order", func() {
		// Create appIDs
		var apps []string
		for j := 0; j < 4; j++ {
			id := fmt.Sprintf("app%d-%s", j, common.RandSeq(5))
			apps = append(apps, id)
		}

		// Initial allocation to fill ns quota
		appPods := map[string][]string{}
		appAllocs := map[string]map[string]float64{
			apps[0]: {"cpu": 0.3, "mem": 0.1, "pods": 1},
			apps[1]: {"cpu": 0.2, "mem": 0.05, "pods": 1},
			apps[2]: {"cpu": 0.1, "mem": 0.15, "pods": 1},
			apps[3]: {"cpu": 0.4, "mem": 0.05, "pods": 4},
		}
		for appID, req := range appAllocs {
			appPods[appID] = []string{}
			numPods := int(req["pods"])
			By(fmt.Sprintf("[%s] Deploy %d pods", appID, numPods))
			for i := 0; i < numPods; i++ {
				// Calculate individual podRequest by dividing total appAlloc by numPods
				reqCPU := int64(req["cpu"] / req["pods"] * float64(maxCPU))
				reqMem := int64(req["mem"] / req["pods"] * float64(maxMem))
				podName := fmt.Sprintf("%s-pod%d", appID, i)
				appPods[appID] = append(appPods[appID], podName)

				// Deploy pod
				sleepPodConf := k8s.SleepPodConfig{
					Name: podName, NS: ns, AppID: appID, CPU: reqCPU, Mem: reqMem}
				initPod, podErr := k8s.InitSleepPod(sleepPodConf)
				Ω(podErr).NotTo(HaveOccurred())
				_, err = kClient.CreatePod(initPod, ns)
				Ω(err).NotTo(HaveOccurred())
				err = kClient.WaitForPodRunning(ns, podName, 30*time.Second)
				Ω(err).NotTo(HaveOccurred())
			}
		}

		// Submit one blocked pod for each app in random order. Each requests 0.1 qCPU and 0.05 qMem.
		By("Submitting additional blocked pod for each app")
		randOrder := rand.Perm(3)
		for _, i := range randOrder {
			cpuPct, memPct, appID := 0.1, 0.05, apps[i]
			reqCPU, reqMem := int64(cpuPct*float64(maxCPU)), int64(memPct*float64(maxMem))
			podNum := len(appPods[appID])
			podName := fmt.Sprintf("%s-pod%d", appID, podNum)
			appPods[appID] = append(appPods[appID], podName)

			By(fmt.Sprintf("[%s] Submit %s", appID, podName))
			sleepPodConf := k8s.SleepPodConfig{
				Name: podName, NS: ns, AppID: appID, CPU: reqCPU, Mem: reqMem}
			initPod, podErr := k8s.InitSleepPod(sleepPodConf)
			Ω(podErr).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			err = kClient.WaitForPodPending(ns, podName, 10*time.Second)
			Ω(err).NotTo(HaveOccurred())

			// Wait till requests has been added to application
			err := wait.PollImmediate(300*time.Millisecond, time.Duration(30)*time.Second, func() (bool, error) {
				app, err := restClient.GetAppInfo("default", "root."+ns, appID)
				if err != nil {
					return false, nil
				}
				if len(app.Requests) == 1 {
					return true, nil
				}
				return false, nil
			})
			Ω(err).NotTo(HaveOccurred())
		}

		// Log correct app priority and pod wait order
		appOrder := []string{apps[2], apps[1], apps[0]}
		var waitOrder []string
		for _, appID := range appOrder {
			l := len(appPods[appID])
			waitOrder = append(waitOrder, appPods[appID][l-1])
		}

		// Verify wait order by releasing app3 pod. Then check for correct pod running.
		for _, podName := range waitOrder {
			// Delete app3 pod to release resource
			app3Pods, l := appPods[apps[3]], len(appPods[apps[3]])
			pod := app3Pods[l-1]
			appPods[apps[3]] = app3Pods[:l-1]

			By(fmt.Sprintf("Delete %s", pod))
			err := kClient.DeletePod(pod, ns)
			Ω(err).NotTo(HaveOccurred())

			By(fmt.Sprintf("Verify %s is now running", podName))
			err = kClient.WaitForPodRunning(ns, podName, 120*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		testDescription := CurrentGinkgoTestDescription()
		if testDescription.Failed {
			tests.LogTestClusterInfoWrapper(testDescription.TestText, []string{ns})
			tests.LogYunikornContainer(testDescription.TestText)
		}
		By("Deleting all pods: " + ns)
		err := kClient.DeletePods(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
