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
package CI


import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"regexp"
	"strconv"
	"time"
	"yunikorn-qe/framework/cfg_manager"
	"yunikorn-qe/framework/helpers"
)

var _ = Describe( "CI: Test for basic scheduling", func(){
	var kClient helpers.KubeCtl
	var restClient helpers.RClient
	var sleepPodDef = cfg_manager.GetAbsPath("../testdata/sleeppod_template.yaml")
	var sleepRespPod *v1.Pod
	var dev = "development"
	var appsInfo map[string]interface{}
	var r = regexp.MustCompile("memory:(\\d+) vcore:(\\d+)")

	BeforeSuite(func(){
		// Initializing kubectl client
		kClient = helpers.KubeCtl{}
		kClient.SetClient()
		// Initializing rest client
		restClient = helpers.RClient{}
		By("create development namespace")
		ns1, err := kClient.CreateNamespace(dev)
		Ω(err).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		By("Deploy the sleep pod to the development namespace")
		sleepObj := helpers.GetPodObj(sleepPodDef)
		sleepObj.Namespace = dev
		sleepObj.ObjectMeta.Labels["applicationId"] = helpers.GetUUID()
		sleepRespPod, err = kClient.CreatePod(sleepObj, dev)
		Ω(err).NotTo(HaveOccurred())
		//Adding sleep here for the app info to be reflected.
		time.Sleep(3 * time.Second)

		appsInfo, err = restClient.GetAppInfo(sleepRespPod.ObjectMeta.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appsInfo).NotTo(BeNil())
	})

	Context("Verifying the basic scheduling information", func(){
		It("Verify the application & queue information ", func(){
			By("Verify that the sleep pod is mapped to development queue")
			Ω(appsInfo["applicationID"]).To(Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
			Ω(appsInfo["queueName"]).To(ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
		})

		It("Verify that the job is scheduled & running by YuniKorn", func(){
			By("Verify that the job is scheduled & running by YuniKorn")
			Ω(appsInfo["applicationState"]).To(Equal("Running"))
			Ω("yunikorn").To(Equal(sleepRespPod.Spec.SchedulerName))
		})

		It("Verify the pod allocation properties", func(){
			By("Verify the pod allocation properties")
			Ω(appsInfo["allocations"]).NotTo(BeNil())
			allocations := appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
			Ω(allocations["allocationKey"]).NotTo(BeNil())
			Ω(allocations["nodeId"]).NotTo(BeNil())
			Ω(allocations["partition"]).NotTo(BeNil())
			Ω(allocations["uuid"]).NotTo(BeNil())
			Ω(allocations["applicationId"]).To(Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
			Ω(allocations["queueName"]).To(ContainSubstring(sleepRespPod.ObjectMeta.Namespace))
			core:= strconv.FormatInt(sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue(), 10)
			mem:= sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().String()
			matches := r.FindStringSubmatch(allocations["resource"].(string))
			Ω(matches[1]+"M").To(Equal(mem))
			Ω(matches[2]).To(ContainSubstring(core))
		})

		AfterSuite(func(){
			By("Deleting pod with name - " + sleepRespPod.Name)
			err := kClient.DeletePod(sleepRespPod.Name, dev)
			Ω(err).NotTo(HaveOccurred())

			By("Deleting development namespaces")
			err = kClient.DeleteNamespace(dev)
			Ω(err).NotTo(HaveOccurred())
		})
	})
})
