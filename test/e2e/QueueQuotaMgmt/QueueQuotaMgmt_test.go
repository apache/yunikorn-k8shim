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

package queuequotamgmt_test

import (
	"fmt"
	"math"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func setPodValues(pod *v1.Pod, namespace string, cpu int64, mem int64) {
	pod.Namespace = namespace
	pod.Name = "sleep-" + common.RandSeq(5)
	pod.ObjectMeta.Labels["applicationId"] = common.GetUUID()
	pod.Spec.Containers[0].Command = []string{"sleep", "60"}
	pod.Spec.Containers[0].Resources = v1.ResourceRequirements{
		Requests: v1.ResourceList{
			"cpu":    resource.MustParse(strconv.FormatInt(cpu, 10) + "m"),
			"memory": resource.MustParse(strconv.FormatInt(mem, 10) + "M"),
		},
	}
}

var _ = Describe("", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var sleepPodDef string
	var err error
	var sleepRespPod *v1.Pod
	var maxCPU int64
	var maxMem int64
	var maxCPUAnnotation = "yunikorn.apache.org/namespace.max.cpu"
	var maxMemAnnotation = "yunikorn.apache.org/namespace.max.memory"
	var annotations map[string]string
	var ns string
	var queueInfo map[string]interface{}
	var maxResource yunikorn.ResourceUsage
	var usedResource yunikorn.ResourceUsage
	var usedPerctResource yunikorn.ResourceUsage
	var pods []string

	BeforeEach(func() {
		// Initializing kubectl client
		sleepPodDef, err = common.GetAbsPath("../testdata/sleeppod_template.yaml")
		Ω(err).NotTo(HaveOccurred())
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}
		annotations = map[string]string{}
		pods = []string{}
	})

	/*
		1. Create a namespace with annotations set to max CPU and max Mem
		2. Submit pod with CPU and Mem requests
		3. Verify the CPU and memory usage reported by Yunikorn is accurate.
		4. Submit another pod with resource request that exceeds the limits
		5. Verify the pod is in un-schedulable state
		6. Wait until pod-1 completes and verify that pod-2 is scheduled now.
	*/
	It("Verify_Queue_Quota_Allocation", func() {
		maxMem = 1024
		maxCPU = 1000
		annotations[maxCPUAnnotation] = strconv.FormatInt(maxCPU, 10) + "m"
		annotations[maxMemAnnotation] = strconv.FormatInt(maxMem, 10) + "M"

		var reqCPU int64 = 300
		var reqMem int64 = 300

		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace with maxCPU: %d and maxMem: %d", ns, maxCPU, maxMem))
		ns1, err1 := kClient.CreateNamespace(ns, annotations)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		var iter int64
		for iter = 1; iter <= 3; iter++ {
			var sleepObj, err2 = k8s.GetPodObj(sleepPodDef)
			Ω(err2).NotTo(HaveOccurred())
			setPodValues(sleepObj, ns, reqCPU, reqMem)

			pods = append(pods, sleepObj.Name)
			By(fmt.Sprintf("App-%d: Deploy the sleep app:%s to %s namespace", iter, sleepObj.Name, ns))
			sleepRespPod, err = kClient.CreatePod(sleepObj, ns)
			Ω(err).NotTo(HaveOccurred())
			//Wait for pod to move to starting state
			err = restClient.WaitForAppStateTransition(sleepRespPod.ObjectMeta.Labels["applicationId"],
				yunikorn.States().Application.Starting,
				360)
			Ω(err).NotTo(HaveOccurred())

			// Verify that the resources requested by above sleep pod is accounted for in the queues response
			queueInfo, err = restClient.GetSpecificQueueInfo(ns)
			Ω(err).NotTo(HaveOccurred())
			Ω(queueInfo).NotTo(BeEmpty())
			Ω(queueInfo["queuename"]).Should(Equal(ns))
			Ω(queueInfo["status"]).Should(Equal("Active"))
			Ω(queueInfo["properties"]).Should(BeEmpty())
			var capacities = queueInfo["capacities"].(map[string]interface{})
			Ω(maxResource.ParseResourceUsageString(capacities["maxcapacity"].(string))).NotTo(HaveOccurred())
			Ω(usedResource.ParseResourceUsageString(capacities["usedcapacity"].(string))).NotTo(HaveOccurred())
			Ω(usedPerctResource.ParseResourceUsageString(capacities["absusedcapacity"].(string))).NotTo(HaveOccurred())

			By(fmt.Sprintf("App-%d: Verify max capacity on the queue is accurate", iter))
			Ω(maxResource.GetVCPU()).Should(Equal(strconv.FormatInt(maxCPU, 10)))
			Ω(maxResource.GetMemory()).Should(Equal(strconv.FormatInt(maxMem, 10)))

			By(fmt.Sprintf("App-%d: Verify used capacity on the queue is accurate after 1st pod deployment", iter))
			Ω(usedResource.GetVCPU()).Should(Equal(strconv.FormatInt(reqCPU*iter, 10)))
			Ω(usedResource.GetMemory()).Should(Equal(strconv.FormatInt(reqMem*iter, 10)))

			var perctCPU = strconv.FormatFloat(math.Floor((float64(reqCPU*iter)/float64(maxCPU))*100), 'g', -4, 64)
			var perctMem = strconv.FormatFloat(math.Floor((float64(reqMem*iter)/float64(maxMem))*100), 'g', -4, 64)
			By(fmt.Sprintf("App-%d: Verify used percentage capacity on the queue is accurate after 1st pod deployment", iter))
			Ω(usedPerctResource.GetVCPU()).Should(Equal(perctCPU))
			Ω(usedPerctResource.GetMemory()).Should(Equal(perctMem))
		}

		By("App-4: Submit another app which exceeds the queue quota limitation")
		var sleepObj, err2 = k8s.GetPodObj(sleepPodDef)
		Ω(err2).NotTo(HaveOccurred())
		setPodValues(sleepObj, ns, reqCPU, reqMem)

		By(fmt.Sprintf("App-4: Deploy the sleep app:%s to %s namespace", sleepObj.Name, ns))
		sleepRespPod, err = kClient.CreatePod(sleepObj, ns)
		Ω(err).NotTo(HaveOccurred())
		pods = append(pods, sleepObj.Name)

		By(fmt.Sprintf("App-4: Verify app:%s in accepted state", sleepObj.Name))
		//Wait for pod to move to accepted state
		err = restClient.WaitForAppStateTransition(sleepRespPod.ObjectMeta.Labels["applicationId"],
			yunikorn.States().Application.Accepted,
			120)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Pod-4: Verify pod:%s is in pending state", sleepObj.Name))
		err = kClient.WaitForPodPending(ns, sleepObj.Name, time.Duration(60)*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("App-1: Wait for 1st app:%s to complete, to make enough capacity to run the last app", pods[0]))
		//Wait for pod to move to accepted state
		err = kClient.WaitForPodSucceeded(ns, pods[0], time.Duration(360)*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("App-4: Verify app:%s moved to starting state", sleepObj.Name))
		//Wait for pod to move to started state
		err = restClient.WaitForAppStateTransition(sleepRespPod.ObjectMeta.Labels["applicationId"],
			yunikorn.States().Application.Starting,
			360)
		Ω(err).NotTo(HaveOccurred())

	}, 360)

	It("Verify_NS_Without_Quota", func() {
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace without quota information", ns))
		ns1, err1 := kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	DescribeTable("", func(x string, y string) {
		annotations[maxCPUAnnotation] = x
		annotations[maxMemAnnotation] = y

		var reqCPU int64 = 300
		var reqMem int64 = 300

		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace with empty annotations", ns))
		ns1, err1 := kClient.CreateNamespace(ns, annotations)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		var sleepObj, err2 = k8s.GetPodObj(sleepPodDef)
		Ω(err2).NotTo(HaveOccurred())
		setPodValues(sleepObj, ns, reqCPU, reqMem)

		pods = append(pods, sleepObj.Name)
		By(fmt.Sprintf("Deploy the sleep app:%s to %s namespace", sleepObj.Name, ns))
		sleepRespPod, err = kClient.CreatePod(sleepObj, ns)
		Ω(err).NotTo(HaveOccurred())
		//Wait for pod to move to starting state
		err = restClient.WaitForAppStateTransition(sleepRespPod.ObjectMeta.Labels["applicationId"],
			yunikorn.States().Application.Starting,
			360)
		Ω(err).NotTo(HaveOccurred())
	},
		Entry("Verify_NS_With_Empty_Quota_Annotations", nil, nil),
		Entry("Verify_NS_With_Wrong_Quota_Annotations", "a", "b"),
	)

	// Hierarchical Queues - Quota enforcement
	// For now, these cases are skipped
	PIt("Verify_Child_Max_Resource_Must_Be_Less_Than_Parent_Max", func() {
		//P2 case - addressed later
	})

	PIt("Verify_Sum_Of_All_Child_Resource_Must_Be_Less_Than_Parent_Max", func() {
		//P2 case - addressed later
	})

	AfterEach(func() {
		for _, pod := range pods {
			By("Deleting pod with name - " + pod)
			err2 := kClient.DeletePod(pod, ns)
			Ω(err2).NotTo(HaveOccurred())
		}

		By("Deleting test namespaces")
		err = kClient.DeleteNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
