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
	"regexp"
	"strconv"

	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("FallbackTest:", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var sleepRespPod *v1.Pod
	var ns string
	var appsInfo map[string]interface{}
	var r = regexp.MustCompile(`memory:(\d+) vcore:(\d+)`)

	BeforeEach(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}
		ns = "test-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace", ns))
		ns1, err1 := kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		By(fmt.Sprintf("Deploy the sleep pod to %s namespace", ns))
		sleepPodConf := common.SleepPodConfig{Name: "sleepjob", NS: ns, Time: 600}
		sleepRespPod, err = kClient.CreatePod(common.InitSleepPod(sleepPodConf), ns)
		Ω(err).NotTo(HaveOccurred())
		//Wait for pod to move to running state
	})

	It("Verify_App_In_Starting_State", func() {
		err = kClient.WaitForPodBySelectorRunning(ns,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			10)
		Ω(err).NotTo(HaveOccurred())

		appsInfo, err = restClient.GetAppInfo(sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appsInfo).NotTo(BeNil())
		By(fmt.Sprintf("Verify that the sleep pod is mapped to %s queue", ns))
		Ω(appsInfo["applicationID"]).To(Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		Ω(appsInfo["queueName"]).To(ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
		By("Verify that the job is scheduled by YuniKorn & is in starting state")
		Ω(appsInfo["applicationState"]).To(Equal("Starting"))
		Ω("yunikorn").To(Equal(sleepRespPod.Spec.SchedulerName))
	}, 60)

	It("Verify_App_State_Transition_To_Running_Post_Timeout", func() {
		By("Wait for fallback timeout of 5mins")
		err = restClient.WaitForAppStateTransition(sleepRespPod.ObjectMeta.Labels["applicationId"],
			yunikorn.States().Application.Running,
			360)
		Ω(err).NotTo(HaveOccurred())

		// Get AppInfo again to check the allocations post running state.
		appsInfo, err = restClient.GetAppInfo(sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(appsInfo["allocations"]).NotTo(BeNil())
		allocations, ok := appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
		Ω(ok).Should(BeTrue())
		Ω(allocations["allocationKey"]).NotTo(BeNil())
		Ω(allocations["nodeId"]).NotTo(BeNil())
		Ω(allocations["partition"]).NotTo(BeNil())
		Ω(allocations["uuid"]).NotTo(BeNil())
		Ω(allocations["applicationId"]).To(Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		Ω(allocations["queueName"]).To(ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
		core := strconv.FormatInt(sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue(), 10)
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().String()
		matches := r.FindStringSubmatch(allocations["resource"].(string))
		Ω(matches[1] + "M").To(Equal(mem))
		Ω(matches[2]).To(ContainSubstring(core))
	}, 360)

	AfterEach(func() {
		By("Tearing down namespace: " + ns)
		err := k.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
