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

package gangscheduling_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

const (
	groupA = "groupa"
	groupB = "groupb"
	groupC = "groupc"
)

var (
	restClient                yunikorn.RClient
	ns                        string
	nsQueue                   string
	appID                     string
	minResource               map[string]resource.Quantity
	jobNames                  []string
	unsatisfiableNodeSelector = map[string]string{"kubernetes.io/hostname": "unsatisfiable_node"}
)

var _ = Describe("", func() {
	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())

		ns = "ns-" + common.RandSeq(10)
		nsQueue = "root." + ns
		By(fmt.Sprintf("Creating namespace: %s for sleep jobs", ns))
		namespace, err := kClient.CreateNamespace(ns, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(namespace.Status.Phase).To(Equal(v1.NamespaceActive))

		minResource = map[string]resource.Quantity{
			v1.ResourceCPU.String():    resource.MustParse("10m"),
			v1.ResourceMemory.String(): resource.MustParse("20M"),
		}
		jobNames = []string{}
		appID = "appid-" + common.RandSeq(5)
	})

	// Test to verify annotation with task group definition
	// 1. Deploy 1 job with tg definition
	// 2. Poll for 5 placeholders
	// 3. App running
	// 4. Deploy 1 job with 5 real pods of task group
	// 5. Check placeholders deleted
	// 6. Real pods running and app running
	It("Verify_Annotation_TaskGroup_Def", func() {
		// Define gang member template with 5 members, 1 real pod (not part of tg)
		annotations := k8s.PodAnnotation{
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(5), MinResource: minResource},
			},
		}
		createJob(appID, minResource, annotations, 1)

		checkAppStatus(appID, yunikorn.States().Application.Running)

		// Ensure placeholders are created
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		checkPlaceholderData(appDaoInfo, groupA, 5, 0, 0)

		// Deploy job, now with 5 pods part of taskGroup
		By("Deploy second job with 5 real taskGroup pods")
		annotations.TaskGroupName = groupA
		realJob := createJob(appID, minResource, annotations, 5)

		// Check all placeholders deleted.
		By("Wait for all placeholders terminated")
		phTermErr := kClient.WaitForPlaceholders(ns, "tg-"+appID+"-"+groupA+"-", 0, 3*time.Minute, nil)
		Ω(phTermErr).NotTo(gomega.HaveOccurred())

		// Check real gang members now running
		By("Wait for all gang members running")
		jobRunErr := kClient.WaitForJobPods(ns, realJob.Name, int(*realJob.Spec.Parallelism), 3*time.Minute)
		Ω(jobRunErr).NotTo(HaveOccurred())

		checkAppStatus(appID, yunikorn.States().Application.Running)

		// Ensure placeholders are replaced
		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		checkPlaceholderData(appDaoInfo, groupA, 5, 5, 0)
	})

	// Test to verify multiple task group nodes
	// 1. Deploy 1 job with 3 task group's definitions
	// 2. Poll for task group's placeholders
	// 3. Store the nodes of placeholders by task group
	// 4. Deploy 1 job with real pods
	// 5. Nodes distributions of real pods and placeholders should be the same.
	It("Verify_Multiple_TaskGroups_Nodes", func() {
		annotations := k8s.PodAnnotation{
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: minResource},
				{Name: groupB, MinMember: int32(5), MinResource: minResource},
				{Name: groupC, MinMember: int32(7), MinResource: minResource},
			},
		}
		createJob(appID, minResource, annotations, 1)

		checkAppStatus(appID, yunikorn.States().Application.Running)

		// Wait for placeholders to become running
		stateRunning := v1.PodRunning
		By("Wait for all placeholders running")
		phPodPrefix := "tg-" + appID + "-"
		phErr := kClient.WaitForPlaceholders(ns, phPodPrefix, 15, 2*time.Minute, &stateRunning)
		Ω(phErr).NotTo(HaveOccurred())

		// Check placeholder node distribution is same as real pods'
		phPods, phListErr := kClient.ListPlaceholders(ns, phPodPrefix)
		Ω(phListErr).NotTo(HaveOccurred())
		taskGroupNodes := map[string]map[string]int{}
		for _, ph := range phPods {
			tg, ok := ph.Annotations[constants.AnnotationTaskGroupName]
			if !ok {
				continue
			}
			if _, ok = taskGroupNodes[tg]; !ok {
				taskGroupNodes[tg] = map[string]int{}
			}
			if _, ok = taskGroupNodes[tg][ph.Spec.NodeName]; !ok {
				taskGroupNodes[tg][ph.Spec.NodeName] = 0
			}
			taskGroupNodes[tg][ph.Spec.NodeName]++
		}

		// Deploy real pods for each taskGroup
		realJobNames := []string{}
		for _, tg := range annotations.TaskGroups {
			annotations.TaskGroupName = tg.Name
			realJob := createJob(appID, minResource, annotations, tg.MinMember)
			realJobNames = append(realJobNames, realJob.Name)
		}

		By("Wait for all placeholders terminated")
		phTermErr := kClient.WaitForPlaceholders(ns, phPodPrefix, 0, 3*time.Minute, nil)
		Ω(phTermErr).NotTo(HaveOccurred())

		// Check real gang members now running on same node distribution
		By("Verify task group node distribution")
		realPodNodes := map[string]map[string]int{}
		for i, tg := range annotations.TaskGroups {
			jobPods, lstErr := kClient.ListPods(ns, fmt.Sprintf("job-name=%s", realJobNames[i]))
			Ω(lstErr).NotTo(HaveOccurred())
			Ω(len(jobPods.Items)).Should(BeNumerically("==", tg.MinMember))
			realPodNodes[tg.Name] = map[string]int{}
			for _, pod := range jobPods.Items {
				podRunErr := kClient.WaitForPodRunning(ns, pod.Name, time.Minute*5)
				Ω(podRunErr).NotTo(HaveOccurred())
				pod, getErr := kClient.GetPod(pod.Name, ns)
				Ω(getErr).NotTo(HaveOccurred())
				realPodNodes[tg.Name][pod.Spec.NodeName]++
			}
		}
		Ω(realPodNodes).Should(Equal(taskGroupNodes))

		checkAppStatus(appID, yunikorn.States().Application.Running)
	})

	// Test to verify task group with more than min members
	// 1. Deploy 1 job with more taskGroup pods than minMembers
	// 2. Verify all pods running
	It("Verify_TG_with_More_Than_minMembers", func() {
		annotations := k8s.PodAnnotation{
			TaskGroupName: groupA,
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: minResource},
			},
		}
		createJob(appID, minResource, annotations, 6)

		checkAppStatus(appID, yunikorn.States().Application.Running)

		By("Wait for all placeholders terminated")
		phTermErr := kClient.WaitForPlaceholders(ns, "tg-"+appID+"-", 0, 3*time.Minute, nil)
		Ω(phTermErr).NotTo(HaveOccurred())

		// Ensure placeholders are replaced and allocations count is correct
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		checkPlaceholderData(appDaoInfo, groupA, 3, 3, 0)
		Ω(len(appDaoInfo.Allocations)).To(Equal(int(6)), "Allocations count is not correct")
	})

	// Test to verify soft GS style behaviour
	// 1. Submit gang job with 2 gangs
	// a) ganga - 3 placeholders
	// b) gangb - 1 placeholder, unsatisfiable nodeSelector
	// 2. After placeholder timeout, real pods should be scheduled.
	It("Verify_Default_GS_Style", func() {
		pdTimeout := 20
		annotations := k8s.PodAnnotation{
			SchedulingPolicyParams: fmt.Sprintf("%s=%d", constants.SchedulingPolicyTimeoutParam, pdTimeout),
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: minResource},
				{Name: groupB, MinMember: int32(1), MinResource: minResource, NodeSelector: unsatisfiableNodeSelector},
			},
		}
		job := createJob(appID, minResource, annotations, 3)

		// Wait for placeholder timeout
		time.Sleep(time.Duration(pdTimeout) * time.Second)

		By(fmt.Sprintf("[%s] Verify pods are scheduled", appID))
		jobRunErr := kClient.WaitForJobPods(ns, job.Name, int(*job.Spec.Parallelism), 2*time.Minute)
		Ω(jobRunErr).NotTo(HaveOccurred())

		checkAppStatus(appID, yunikorn.States().Application.Running)

		// Ensure placeholders are timed out and allocations count is correct as app started running normal because of 'soft' gang style
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(2), "Placeholder count is not correct")
		checkPlaceholderData(appDaoInfo, groupA, 3, 0, 3)
		checkPlaceholderData(appDaoInfo, groupB, 1, 0, 1)
		Ω(len(appDaoInfo.Allocations)).To(Equal(int(3)), "Allocations count is not correct")
		for _, alloc := range appDaoInfo.Allocations {
			Ω(alloc.Placeholder).To(Equal(false), "Allocation should be non placeholder")
			Ω(alloc.PlaceholderUsed).To(Equal(false), "Allocation should not be replacement of ph")
		}
	})

	// Test to verify Hard GS style behaviour
	// 1. Deploy 1 job with 2 task group's:
	// a) 1 tg with un runnable placeholders
	// b) 1 tg with runnable placeholders
	// 2. Verify appState = Accepted
	// 3. Once ph's are timed out, app should move to failed state
	It("Verify_Hard_GS_Failed_State", func() {
		pdTimeout := 20
		gsStyle := "Hard"
		placeholderTimeoutStr := fmt.Sprintf("%s=%d", constants.SchedulingPolicyTimeoutParam, pdTimeout)
		gsStyleStr := fmt.Sprintf("%s=%s", constants.SchedulingPolicyStyleParam, gsStyle)

		annotations := k8s.PodAnnotation{
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: minResource, NodeSelector: unsatisfiableNodeSelector},
				{Name: groupB, MinMember: int32(3), MinResource: minResource},
			},
			SchedulingPolicyParams: fmt.Sprintf("%s %s", placeholderTimeoutStr, gsStyleStr),
		}
		createJob(appID, minResource, annotations, 1)

		// Wait for placeholder timeout
		time.Sleep(time.Duration(pdTimeout) * time.Second)

		checkCompletedAppStatus(appID, yunikorn.States().Application.Failed)

		// Ensure placeholders are timed out and allocations count is correct as app started running normal because of 'soft' gang style
		appDaoInfo, appDaoInfoErr := restClient.GetCompletedAppInfo(configmanager.DefaultPartition, appID)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(2), "Placeholder count is not correct")
		checkPlaceholderData(appDaoInfo, groupA, 3, 0, 3)
		checkPlaceholderData(appDaoInfo, groupB, 3, 0, 3)
	})

	// Test to verify Gang Apps FIFO order
	// Update namespace with quota of 300m and 300M
	// 1. Deploy appA with 1 pod
	// 2. Deploy appB with gang of 3 pods
	// 3. Deploy appC with 1 pod
	// 4. Delete appA
	// 5. appA = Completing, appB = Running, appC = Accepted
	It("Verify_GangApp_FIFO_Order", func() {
		By(fmt.Sprintf("Update namespace %s with quota cpu 300m and memory 300M", ns))
		namespace, nsErr := kClient.UpdateNamespace(ns, map[string]string{
			constants.NamespaceQuota: "{\"cpu\": \"300m\", \"memory\": \"300M\"}"})
		Ω(nsErr).NotTo(HaveOccurred())
		Ω(namespace.Status.Phase).To(Equal(v1.NamespaceActive))

		appIDA := "app-a-" + common.RandSeq(5)
		appIDB := "app-b-" + common.RandSeq(5)
		appIDC := "app-c-" + common.RandSeq(5)

		minResource[v1.ResourceCPU.String()] = resource.MustParse("100m")
		minResource[v1.ResourceMemory.String()] = resource.MustParse("100M")

		annotationsA := k8s.PodAnnotation{
			TaskGroupName: groupA,
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(0), MinResource: minResource},
			},
		}
		annotationsB := k8s.PodAnnotation{
			TaskGroupName: groupB,
			TaskGroups: []cache.TaskGroup{
				{Name: groupB, MinMember: int32(3), MinResource: minResource},
			},
		}
		annotationsC := k8s.PodAnnotation{
			TaskGroupName: groupC,
			TaskGroups: []cache.TaskGroup{
				{Name: groupC, MinMember: int32(0), MinResource: minResource},
			},
		}
		jobA := createJob(appIDA, minResource, annotationsA, 1)
		time.Sleep(1 * time.Second) // To ensure there is minor gap between applications
		createJob(appIDB, minResource, annotationsB, 3)
		time.Sleep(1 * time.Second) // To ensure there is minor gap between applications
		createJob(appIDC, minResource, annotationsC, 1)

		// AppB should have 2/3 placeholders running
		By("Wait for 2 placeholders running in app " + appIDB)
		statusRunning := v1.PodRunning
		phErr := kClient.WaitForPlaceholders(ns, "tg-"+appIDB+"-", 2, 30*time.Second, &statusRunning)
		Ω(phErr).NotTo(HaveOccurred())

		// Delete appA
		deleteErr := kClient.DeleteJob(jobA.Name, ns)
		Ω(deleteErr).NotTo(HaveOccurred())
		jobNames = jobNames[1:] // remove jobA, so we don't delete again in AfterEach

		// Now, appA=Completing, appB=Running, appC=Accepted
		checkAppStatus(appIDA, yunikorn.States().Application.Completing)
		checkAppStatus(appIDB, yunikorn.States().Application.Running)
		checkAppStatus(appIDC, yunikorn.States().Application.Accepted)

		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appIDA)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.Allocations)).To(Equal(0), "Allocations count is not correct")
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(0), "Placeholder count is not correct")

		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appIDB)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.Allocations)).To(Equal(3), "Allocations count is not correct")
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(3)), "Placeholder count is not correct")

		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appIDC)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.Allocations)).To(Equal(0), "Allocations count is not correct")
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(0), "Placeholder count is not correct")
	})

	// Test validates that lost placeholders resources are decremented by Yunikorn.
	// 1. Submit gang job with 2 gangs
	// a) ganga - 3 placeholders, nodeSelector=nodeA
	// b) gangb - 1 placeholder, unsatisfiable nodeSelector
	// c) 3 real ganga pods
	// 2. Delete all gangA placeholders after all are running
	// 3. Verify no pods from gang-app in nodeA allocations.
	// Verify no pods in app allocations
	// Verify queue used capacity = 0
	// Verify app is failed after app timeout met
	It("Verify_Deleted_Placeholders", func() {
		nodes, err := kClient.GetNodes()
		Ω(err).NotTo(HaveOccurred())
		workerNodes := k8s.GetWorkerNodes(*nodes)
		Ω(len(workerNodes)).NotTo(Equal(0))

		pdTimeout := 60
		annotations := k8s.PodAnnotation{
			SchedulingPolicyParams: fmt.Sprintf("%s=%d", constants.SchedulingPolicyTimeoutParam, pdTimeout),
			TaskGroups: []cache.TaskGroup{
				{
					Name:         groupA,
					MinMember:    int32(3),
					MinResource:  minResource,
					NodeSelector: map[string]string{"kubernetes.io/hostname": workerNodes[0].Name},
				},
				{
					Name:         groupB,
					MinMember:    int32(1),
					MinResource:  minResource,
					NodeSelector: unsatisfiableNodeSelector,
				},
			},
		}
		createJob(appID, minResource, annotations, 3)

		checkAppStatus(appID, yunikorn.States().Application.Accepted)

		// Wait for groupa placeholder pods running
		By(fmt.Sprintf("Wait for %s placeholders running", groupA))
		stateRunning := v1.PodRunning
		runErr := kClient.WaitForPlaceholders(ns, "tg-"+appID+"-"+groupA+"-", 3, 30*time.Second, &stateRunning)
		Ω(runErr).NotTo(HaveOccurred())

		// Delete all groupa placeholder pods
		phPods, listErr := kClient.ListPlaceholders(ns, "tg-"+appID+"-"+groupA+"-")
		Ω(listErr).NotTo(HaveOccurred())
		for _, ph := range phPods {
			By(fmt.Sprintf("Delete placeholder %s", ph.Name))
			deleteErr := kClient.DeletePod(ph.Name, ns)
			Ω(deleteErr).NotTo(HaveOccurred())
		}

		// Wait for Yunikorn allocation removal after K8s deletion
		time.Sleep(5 * time.Second)

		// Verify app allocations correctly decremented
		appInfo, appErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(appErr).NotTo(HaveOccurred())
		Ω(len(appInfo.Allocations)).To(Equal(0), "Placeholder allocation not removed from app")

		// Verify no app allocation in nodeA
		ykNodes, nodeErr := restClient.GetNodes(configmanager.DefaultPartition)
		Ω(nodeErr).NotTo(HaveOccurred())
		for _, node := range *ykNodes {
			for _, alloc := range node.Allocations {
				Ω(alloc.ApplicationID).NotTo(Equal(appID), "Placeholder allocation not removed from node")
			}
		}

		// Verify queue resources = 0
		qInfo, qErr := restClient.GetQueue(configmanager.DefaultPartition, nsQueue)
		Ω(qErr).NotTo(HaveOccurred())
		var usedResource yunikorn.ResourceUsage
		var usedPercentageResource yunikorn.ResourceUsage
		usedResource.ParseResourceUsage(qInfo.AllocatedResource)
		Ω(usedResource.GetResourceValue(siCommon.CPU)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
		Ω(usedResource.GetResourceValue(siCommon.Memory)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
		usedPercentageResource.ParseResourceUsage(qInfo.AbsUsedCapacity)
		Ω(usedPercentageResource.GetResourceValue(siCommon.CPU)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
		Ω(usedPercentageResource.GetResourceValue(siCommon.Memory)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
	})

	// Test to verify completed placeholders cleanup
	// 1. Deploy 1 job with 2 task group's:
	// a) 1 tg with un runnable placeholders
	// b) 1 tg with runnable placeholders
	// 2. Delete job
	// 3. Verify app is completing
	// 4. Verify placeholders deleted
	// 5. Verify app allocation is empty
	It("Verify_Completed_Job_Placeholders_Cleanup", func() {
		annotations := k8s.PodAnnotation{
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: minResource, NodeSelector: unsatisfiableNodeSelector},
				{Name: groupB, MinMember: int32(3), MinResource: minResource},
			},
		}
		job := createJob(appID, minResource, annotations, 1)

		checkAppStatus(appID, yunikorn.States().Application.Accepted)

		By("Wait for groupB placeholders running")
		stateRunning := v1.PodRunning
		runErr := kClient.WaitForPlaceholders(ns, "tg-"+appID+"-"+groupB+"-", 3, 30*time.Second, &stateRunning)
		Ω(runErr).NotTo(HaveOccurred())

		By("List placeholders")
		tgPods, listErr := kClient.ListPlaceholders(ns, "tg-"+appID+"-")
		Ω(listErr).NotTo(HaveOccurred())

		By("Delete job pods")
		deleteErr := kClient.DeleteJob(job.Name, ns)
		Ω(deleteErr).NotTo(HaveOccurred())
		jobNames = []string{} // remove job names to prevent delete job again in AfterEach

		checkAppStatus(appID, yunikorn.States().Application.Completing)

		By("Verify placeholders deleted")
		for _, ph := range tgPods {
			deleteErr = kClient.WaitForPodTerminated(ns, ph.Name, 30*time.Second)
			Ω(deleteErr).NotTo(HaveOccurred(), "Placeholder %s still running", ph)
		}

		By("Verify app allocation is empty")
		appInfo, restErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(restErr).NotTo(HaveOccurred())
		Ω(len(appInfo.Allocations)).To(Equal(0))
	})

	// Test to verify originator deletion will trigger placeholders cleanup
	// 1. Create an originator pod
	// 2. Not set pod ownerreference
	// 3. Delete originator pod to trigger placeholders deletion
	// 4. Verify placeholders deleted
	// 5. Verify app allocation is empty
	It("Verify_OriginatorDeletion_Trigger_Placeholders_Cleanup", func() {
		// case 1: originator pod without ownerreference
		verifyOriginatorDeletionCase(false)
	})

	// Test to verify originator deletion will trigger placeholders cleanup
	// 1. Create an originator pod
	// 2. Set pod ownerreference with ownerreference, take configmap for example
	// 3. Delete originator pod to trigger placeholders deletion
	// 4. Verify placeholders deleted
	// 5. Verify app allocation is empty
	It("Verify_OriginatorDeletionWithOwnerreference_Trigger_Placeholders_Cleanup", func() {
		// case 2: originator pod with ownerreference
		verifyOriginatorDeletionCase(true)
	})

	// Test placeholder with hugepages
	// 1. Deploy 1 job with hugepages-2Mi
	// 2. Verify all pods running
	It("Verify_HugePage", func() {
		hugepageKey := fmt.Sprintf("%s2Mi", v1.ResourceHugePagesPrefix)
		nodes, err := kClient.GetNodes()
		Ω(err).NotTo(HaveOccurred())
		hasHugePages := false
		for _, node := range nodes.Items {
			if v, ok := node.Status.Capacity[v1.ResourceName(hugepageKey)]; ok {
				if v.Value() != 0 {
					hasHugePages = true
					break
				}
			}
		}
		if !hasHugePages {
			ginkgo.Skip("Skip hugepages test as no node has hugepages")
		}

		// add hugepages to request
		minResource[hugepageKey] = resource.MustParse("100Mi")
		annotations := k8s.PodAnnotation{
			TaskGroupName: groupA,
			TaskGroups: []cache.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: minResource},
			},
		}
		job := createJob(appID, minResource, annotations, 3)

		By("Verify all job pods are running")
		jobRunErr := kClient.WaitForJobPods(ns, job.Name, int(*job.Spec.Parallelism), 2*time.Minute)
		Ω(jobRunErr).NotTo(HaveOccurred())

		checkAppStatus(appID, yunikorn.States().Application.Running)

		// Ensure placeholders are replaced and allocations count is correct
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		checkPlaceholderData(appDaoInfo, groupA, 3, 3, 0)
		Ω(len(appDaoInfo.Allocations)).To(Equal(int(3)), "Allocations count is not correct")
		Ω(appDaoInfo.UsedResource[hugepageKey]).To(Equal(int64(314572800)), "Used huge page resource is not correct")
	})

	AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{ns})

		By(fmt.Sprintf("Cleanup jobs: %v", jobNames))
		for _, jobName := range jobNames {
			err := kClient.DeleteJob(jobName, ns)
			Ω(err).NotTo(gomega.HaveOccurred())
		}

		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)

		Ω(err).NotTo(HaveOccurred())
	})

})

func createJob(applicationID string, minResource map[string]resource.Quantity, annotations k8s.PodAnnotation, parallelism int32) (job *batchv1.Job) {
	var (
		err      error
		requests = v1.ResourceList{}
		limits   = v1.ResourceList{}
	)
	for k, v := range minResource {
		key := v1.ResourceName(k)
		requests[key] = v
		if strings.HasPrefix(k, v1.ResourceHugePagesPrefix) {
			limits[key] = v
		}
	}

	podConf := k8s.TestPodConfig{
		Labels: map[string]string{
			"app":           "sleep-" + common.RandSeq(5),
			"applicationId": applicationID,
		},
		Annotations: &annotations,
		Resources: &v1.ResourceRequirements{
			Requests: requests,
			Limits:   limits,
		},
	}
	jobConf := k8s.JobConfig{
		Name:        fmt.Sprintf("gangjob-%s-%s", applicationID, common.RandSeq(5)),
		Namespace:   ns,
		Parallelism: parallelism,
		PodConfig:   podConf,
	}

	job, err = k8s.InitJobConfig(jobConf)
	Ω(err).NotTo(HaveOccurred())

	taskGroupsMap, err := k8s.PodAnnotationToMap(podConf.Annotations)
	Ω(err).NotTo(HaveOccurred())

	By(fmt.Sprintf("[%s] Deploy job %s with task-groups: %+v", applicationID, jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
	job, err = kClient.CreateJob(job, ns)
	Ω(err).NotTo(HaveOccurred())

	err = kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
	Ω(err).NotTo(HaveOccurred())

	jobNames = append(jobNames, job.Name) // for cleanup in afterEach function
	return job
}

func checkAppStatus(applicationID, state string) {
	By(fmt.Sprintf("Verify application %s status is %s", applicationID, state))
	timeoutErr := restClient.WaitForAppStateTransition(configmanager.DefaultPartition, nsQueue, applicationID, state, 120)
	Ω(timeoutErr).NotTo(HaveOccurred())
}

func checkCompletedAppStatus(applicationID, state string) {
	By(fmt.Sprintf("Verify application %s status is %s", applicationID, state))
	timeoutErr := restClient.WaitForCompletedAppStateTransition(configmanager.DefaultPartition, applicationID, state, 120)
	Ω(timeoutErr).NotTo(HaveOccurred())
}

func checkPlaceholderData(appDaoInfo *dao.ApplicationDAOInfo, tgName string, count, replaced, timeout int) {
	verified := false
	for _, placeholderData := range appDaoInfo.PlaceholderData {
		if tgName == placeholderData.TaskGroupName {
			Ω(int(placeholderData.Count)).To(Equal(count), "Placeholder count is not correct")
			Ω(int(placeholderData.Replaced)).To(Equal(replaced), "Placeholder replaced is not correct")
			Ω(int(placeholderData.TimedOut)).To(Equal(timeout), "Placeholder timeout is not correct")
			verified = true
			break
		}
	}
	Ω(verified).To(Equal(true), fmt.Sprintf("Can't find task group %s in app info", tgName))
}

func verifyOriginatorDeletionCase(withOwnerRef bool) {
	podConf := k8s.TestPodConfig{
		Name: "gang-driver-pod" + common.RandSeq(5),
		Labels: map[string]string{
			"app":           "sleep-" + common.RandSeq(5),
			"applicationId": appID,
		},
		Annotations: &k8s.PodAnnotation{
			TaskGroups: []cache.TaskGroup{
				{
					Name:         groupA,
					MinMember:    int32(3),
					MinResource:  minResource,
					NodeSelector: unsatisfiableNodeSelector,
				},
				{
					Name:        groupB,
					MinMember:   int32(3),
					MinResource: minResource,
				},
			},
		},
		Resources: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    minResource["cpu"],
				"memory": minResource["memory"],
			},
		},
	}

	podTest, err := k8s.InitTestPod(podConf)
	Ω(err).NotTo(HaveOccurred())

	if withOwnerRef {
		// create a configmap as ownerreference
		testConfigmap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cm",
				UID:  "test-cm-uid",
			},
			Data: map[string]string{
				"test": "test",
			},
		}
		defer func() {
			deleteErr := kClient.DeleteConfigMap(testConfigmap.Name, ns)
			Ω(deleteErr).NotTo(HaveOccurred())
		}()

		testConfigmap, testErr := kClient.CreateConfigMap(testConfigmap, ns)
		Ω(testErr).NotTo(HaveOccurred())

		podTest.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       testConfigmap.Name,
				UID:        testConfigmap.UID,
			},
		}
	}

	taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
	Ω(annErr).NotTo(HaveOccurred())
	By(fmt.Sprintf("Deploy pod %s with task-groups: %+v", podTest.Name, taskGroupsMap[k8s.TaskGroups]))
	originator, err := kClient.CreatePod(podTest, ns)
	Ω(err).NotTo(HaveOccurred())

	checkAppStatus(appID, yunikorn.States().Application.Accepted)

	By("Wait for groupB placeholders running")
	stateRunning := v1.PodRunning
	runErr := kClient.WaitForPlaceholders(ns, "tg-"+appID+"-"+groupB+"-", 3, 30*time.Second, &stateRunning)
	Ω(runErr).NotTo(HaveOccurred())

	By("List placeholders")
	tgPods, listErr := kClient.ListPlaceholders(ns, "tg-"+appID+"-")
	Ω(listErr).NotTo(HaveOccurred())

	By("Delete originator pod")
	deleteErr := kClient.DeletePod(originator.Name, ns)
	Ω(deleteErr).NotTo(HaveOccurred())

	By("Verify placeholders deleted")
	for _, ph := range tgPods {
		deleteErr = kClient.WaitForPodTerminated(ns, ph.Name, 30*time.Second)
		Ω(deleteErr).NotTo(HaveOccurred(), "Placeholder %s still running", ph)
	}

	By("Verify app allocation is empty")
	appInfo, restErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
	Ω(restErr).NotTo(HaveOccurred())
	Ω(len(appInfo.Allocations)).To(BeNumerically("==", 0))
}
