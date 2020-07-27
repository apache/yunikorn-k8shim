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

package ci

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers"
)

var _ = ginkgo.Describe("CI: Test for basic scheduling", func() {
	var kClient helpers.KubeCtl
	var restClient helpers.RClient
	var sleepPodDef string
	var err error
	var sleepRespPod *v1.Pod
	var dev = "development"
	var appsInfo map[string]interface{}
	var r = regexp.MustCompile(`memory:(\d+) vcore:(\d+)`)

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client
		sleepPodDef, err = helpers.GetAbsPath("../testdata/sleeppod_template.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		kClient = helpers.KubeCtl{}
		gomega.Ω(kClient.SetClient()).To(gomega.BeNil())
		// Initializing rest client
		restClient = helpers.RClient{}
		ginkgo.By("create development namespace")
		ns1, err := kClient.CreateNamespace(dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

		ginkgo.By("Deploy the sleep pod to the development namespace")
		sleepObj, err := helpers.GetPodObj(sleepPodDef)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		sleepObj.Namespace = dev
		sleepObj.ObjectMeta.Labels["applicationId"] = helpers.GetUUID()
		sleepRespPod, err = kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		//Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			10)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		appsInfo, err = restClient.GetAppInfo(sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
	})

	ginkgo.Context("Verifying the basic scheduling information", func() {
		ginkgo.It("Verify the application & queue information ", func() {
			ginkgo.By("Verify that the sleep pod is mapped to development queue")
			gomega.Ω(appsInfo["applicationID"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
			gomega.Ω(appsInfo["queueName"]).To(gomega.ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
		})

		ginkgo.It("Verify that the job is scheduled & Starting by YuniKorn", func() {
			ginkgo.By("Verify that the job is scheduled & running by YuniKorn")
			gomega.Ω(appsInfo["applicationState"]).To(gomega.Equal("Starting"))
			gomega.Ω("yunikorn").To(gomega.Equal(sleepRespPod.Spec.SchedulerName))
		})

		ginkgo.It("Verify the pod allocation properties", func() {
			ginkgo.By("Verify the pod allocation properties")
			gomega.Ω(appsInfo["allocations"]).NotTo(gomega.BeNil())
			allocations, ok := appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
			gomega.Ω(ok).Should(gomega.BeTrue())
			gomega.Ω(allocations["allocationKey"]).NotTo(gomega.BeNil())
			gomega.Ω(allocations["nodeId"]).NotTo(gomega.BeNil())
			gomega.Ω(allocations["partition"]).NotTo(gomega.BeNil())
			gomega.Ω(allocations["uuid"]).NotTo(gomega.BeNil())
			gomega.Ω(allocations["applicationId"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
			gomega.Ω(allocations["queueName"]).To(gomega.ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
			core := strconv.FormatInt(sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue(), 10)
			mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().String()
			matches := r.FindStringSubmatch(allocations["resource"].(string))
			gomega.Ω(matches[1] + "M").To(gomega.Equal(mem))
			gomega.Ω(matches[2]).To(gomega.ContainSubstring(core))
		})

		ginkgo.AfterSuite(func() {
			ginkgo.By("Deleting pod with name - " + sleepRespPod.Name)
			err := kClient.DeletePod(sleepRespPod.Name, dev)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting development namespaces")
			err = kClient.DeleteNamespace(dev)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		})
	})
})
