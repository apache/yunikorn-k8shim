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

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("FallbackTest:", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var sleepRespPod *v1.Pod
	var ns string
	var appsInfo *dao.ApplicationDAOInfo

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
		sleepPodConf := k8s.SleepPodConfig{Name: "sleepjob", NS: ns, Time: 600}
		initPod, podErr := k8s.InitSleepPod(sleepPodConf)
		Ω(podErr).NotTo(HaveOccurred())
		sleepRespPod, err = kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		// Wait for pod to move to running state
	})

	It("Verify_App_In_Starting_State", func() {
		err = kClient.WaitForPodBySelectorRunning(ns,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			10)
		Ω(err).NotTo(HaveOccurred())

		appsInfo, err = restClient.GetAppInfo("default", "root."+ns, sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appsInfo).NotTo(BeNil())
		By(fmt.Sprintf("Verify that the sleep pod is mapped to %s queue", ns))
		Ω(appsInfo.ApplicationID).To(Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		Ω(appsInfo.QueueName).To(ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
		By("Verify that the job is scheduled by YuniKorn & is in starting state")
		Ω(appsInfo.State).To(Equal("Starting"))
		Ω("yunikorn").To(Equal(sleepRespPod.Spec.SchedulerName))
	}, 60)

	It("Verify_App_State_Transition_To_Running_Post_Timeout", func() {
		By("Wait for fallback timeout of 5mins")
		err = restClient.WaitForAppStateTransition("default", "root."+ns, sleepRespPod.ObjectMeta.Labels["applicationId"],
			yunikorn.States().Application.Running,
			360)
		Ω(err).NotTo(HaveOccurred())

		// Get AppInfo again to check the allocations post running state.
		appsInfo, err = restClient.GetAppInfo("default", "root."+ns, sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(appsInfo.Allocations).NotTo(BeNil())
		Ω(len(appsInfo.Allocations)).NotTo(gomega.BeZero())
		allocation := appsInfo.Allocations[0]
		Ω(allocation).NotTo(gomega.BeNil())
		Ω(allocation.AllocationKey).NotTo(BeNil())
		Ω(allocation.NodeID).NotTo(BeNil())
		Ω(allocation.Partition).NotTo(BeNil())
		Ω(allocation.UUID).NotTo(BeNil())
		Ω(allocation.ApplicationID).To(Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap := allocation.ResourcePerAlloc
		Ω(len(resMap)).NotTo(gomega.BeZero())
		Ω(resMap["memory"]).To(gomega.Equal(mem))
		Ω(resMap["vcore"]).To(gomega.Equal(core))
	}, 360)

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
