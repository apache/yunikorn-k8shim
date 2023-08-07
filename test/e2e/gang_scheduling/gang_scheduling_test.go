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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

var _ = Describe("", func() {
	var kClient k8s.KubeCtl //nolint
	var restClient yunikorn.RClient
	var ns string
	groupA := "groupa"
	groupB := "groupb"
	fifoQName := "fifoq"
	defaultPartition := "default"
	var nsQueue string

	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		ns = "ns-" + common.RandSeq(10)
		nsQueue = "root." + ns
		By(fmt.Sprintf("Creating namespace: %s for sleep jobs", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	// Test to verify annotation with task group definition
	// 1. Deploy 1 job with tg definition
	// 2. Poll for 5 placeholders
	// 3. App running
	// 4. Deploy 1 job with 5 real pods of task group
	// 5. Check placeholders deleted
	// 6. Real pods running and app running
	It("Verify_Annotation_TaskGroup_Def", func() {
		taskGroupName := "group-" + common.RandSeq(3)

		// Define gang member template with 5 members, 1 real pod (not part of tg)
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		annotations := k8s.PodAnnotation{
			TaskGroups: []interfaces.TaskGroup{
				{Name: taskGroupName, MinMember: int32(5), MinResource: podResources},
			},
		}
		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &annotations,
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(1),
			PodConfig:   podConf,
		}
		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())

		// Deploy job
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		// After all placeholders reserved + separate pod, appStatus = Running
		By("Verify appStatus = Running")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are created
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(5)), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(0)), "Placeholder replacement count is not correct")

		// Deploy job, now with 5 pods part of taskGroup
		By("Deploy second job with 5 real taskGroup pods")
		podConf.Annotations.TaskGroupName = taskGroupName
		realJobConf := k8s.JobConfig{
			Name:        "gangjob2-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(5),
			PodConfig:   podConf,
		}
		job1, job1Err := k8s.InitJobConfig(realJobConf)
		Ω(job1Err).NotTo(HaveOccurred())
		_, job1Err = kClient.CreateJob(job1, ns)
		Ω(job1Err).NotTo(HaveOccurred())
		createErr = kClient.WaitForJobPodsCreated(ns, job1.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		// Check all placeholders deleted.
		By("Wait for all placeholders terminated")
		tgPlaceHolders := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		for _, phNames := range tgPlaceHolders {
			for _, ph := range phNames {
				phTermErr := kClient.WaitForPodTerminated(ns, ph, 3*time.Minute)
				Ω(phTermErr).NotTo(HaveOccurred())
			}
		}

		// Check real gang members now running
		By("Wait for all gang members running")
		jobRunErr := kClient.WaitForJobPods(ns, realJobConf.Name, int(realJobConf.Parallelism), 3*time.Minute)
		Ω(jobRunErr).NotTo(HaveOccurred())

		By("Verify appStatus = Running")
		timeoutErr = restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are replaced
		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(5)), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(5)), "Placeholder replacement count is not correct")

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeleteJob(job1.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify multiple task group nodes
	// 1. Deploy 1 job with 3 task group's definitions
	// 2. Poll for task group's placeholders
	// 3. Store the nodes of placeholders by task group
	// 4. Deploy 1 job with real pods
	// 5. Nodes distributions of real pods and placeholders should be the same.
	It("Verify_Multiple_TaskGroups_Nodes", func() {
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &k8s.PodAnnotation{
				TaskGroups: []interfaces.TaskGroup{
					{Name: "group-" + common.RandSeq(5), MinMember: int32(3), MinResource: podResources},
					{Name: "group-" + common.RandSeq(5), MinMember: int32(5), MinResource: podResources},
					{Name: "group-" + common.RandSeq(5), MinMember: int32(7), MinResource: podResources},
				},
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(1),
			PodConfig:   podConf,
		}
		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())

		// Deploy job
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		By("Verify appStatus = Running")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			30)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Check placeholder node distribution is same as real pods'
		tgPlaceHolders := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		taskGroupNodes := map[string]map[string]int{}
		for tg, phNames := range tgPlaceHolders {
			taskGroupNodes[tg] = map[string]int{}
			for _, name := range phNames {
				podRunErr := kClient.WaitForPodRunning(ns, name, time.Second*120)
				Ω(podRunErr).NotTo(HaveOccurred())
				ph, phErr := kClient.GetPod(name, ns)
				Ω(phErr).NotTo(HaveOccurred())
				taskGroupNodes[tg][ph.Spec.NodeName]++
			}
		}

		// Deploy real pods for each taskGroup
		for i := 0; i < len(podConf.Annotations.TaskGroups); i++ {
			curTG := podConf.Annotations.TaskGroups[i]
			podConf.Annotations.TaskGroupName = curTG.Name
			realJobConf := k8s.JobConfig{
				Name:        "job-" + curTG.Name,
				Namespace:   ns,
				Parallelism: curTG.MinMember,
				PodConfig:   podConf,
			}
			By(fmt.Sprintf("Deploy pods for taskGroup: %s", curTG.Name))
			job, jobErr = k8s.InitJobConfig(realJobConf)
			Ω(jobErr).NotTo(HaveOccurred())
			_, jobErr = kClient.CreateJob(job, ns)
			Ω(jobErr).NotTo(HaveOccurred())
			createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
			Ω(createErr).NotTo(HaveOccurred())
		}

		By("Wait for all placeholders terminated")
		for _, phNames := range tgPlaceHolders {
			for _, ph := range phNames {
				phTermErr := kClient.WaitForPodTerminated(ns, ph, 3*time.Minute)
				Ω(phTermErr).NotTo(HaveOccurred())
			}
		}

		// Check real gang members now running on same node distribution
		By("Verify task group node distribution")
		realPodNodes := map[string]map[string]int{}
		for _, tg := range podConf.Annotations.TaskGroups {
			jobPods, lstErr := kClient.ListPods(ns, fmt.Sprintf("job-name=%s%s", "job-", tg.Name))
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

		By("Verify appStatus = Running")
		timeoutErr = restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			10)
		Ω(timeoutErr).NotTo(HaveOccurred())

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify task group with more than min members
	// 1. Deploy 1 job with more taskGroup pods than minMembers
	// 2. Verify all pods running
	It("Verify_TG_with_More_Than_minMembers", func() {
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		tgName := "group-" + common.RandSeq(5)
		annotations := k8s.PodAnnotation{
			TaskGroupName: tgName,
			TaskGroups: []interfaces.TaskGroup{
				{Name: tgName, MinMember: int32(3), MinResource: podResources},
			},
		}
		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &annotations,
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: 2 * annotations.TaskGroups[0].MinMember,
			PodConfig:   podConf,
		}

		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		By("Verify all job pods are running")
		jobRunErr := kClient.WaitForJobPods(ns, jobConf.Name, int(jobConf.Parallelism), 2*time.Minute)
		Ω(jobRunErr).NotTo(HaveOccurred())

		By("Verify appStatus = Running")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are replaced and allocations count is correct
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(3)), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(3)), "Placeholder replacement count is not correct")
		Ω(len(appDaoInfo.Allocations)).To(Equal(int(6)), "Allocations count is not correct")

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify soft GS style behaviour
	// 1. Submit gang job with 2 gangs
	// a) ganga - 3 placeholders
	// b) gangb - 1 placeholder, unsatisfiable nodeSelector
	// 2. After placeholder timeout, real pods should be scheduled.
	It("Verify_Default_GS_Style", func() {
		appID := "appid-" + common.RandSeq(5)
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}

		pdTimeout := 20
		placeholderTimeoutStr := fmt.Sprintf("%s=%d", "placeholderTimeoutInSeconds", pdTimeout)
		annotations := k8s.PodAnnotation{
			SchedulingPolicyParams: placeholderTimeoutStr,
			TaskGroups: []interfaces.TaskGroup{
				{
					Name:        groupA,
					MinMember:   int32(3),
					MinResource: podResources,
				},
				{
					Name:         groupB,
					MinMember:    int32(1),
					MinResource:  podResources,
					NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable_node"},
				},
			},
		}

		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": appID,
			},
			Annotations: &annotations,
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: 3,
			PodConfig:   podConf,
		}

		// Create gang job
		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("[%s] Deploy job %s with task-groups: %+v",
			appID, jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		// Wait for placeholder timeout
		time.Sleep(time.Duration(pdTimeout) * time.Second)

		By(fmt.Sprintf("[%s] Verify pods are scheduled", appID))
		jobRunErr := kClient.WaitForJobPods(ns, jobConf.Name, int(jobConf.Parallelism), 2*time.Minute)
		Ω(jobRunErr).NotTo(HaveOccurred())

		By("Verify appStatus = Running")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			10)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are timed out and allocations count is correct as app started running normal because of 'soft' gang style
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(2), "Placeholder count is not correct")
		if appDaoInfo.PlaceholderData[0].TaskGroupName == groupA {
			Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(3)), "Placeholder count is not correct")
			Ω(int(appDaoInfo.PlaceholderData[0].TimedOut)).To(Equal(int(3)), "Placeholder timed out is not correct")
			Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(0)), "Placeholder replacement count is not correct")
		} else if appDaoInfo.PlaceholderData[1].TaskGroupName == groupB {
			Ω(int(appDaoInfo.PlaceholderData[1].Count)).To(Equal(int(0)), "Placeholder count is not correct")
		}
		Ω(len(appDaoInfo.Allocations)).To(Equal(int(3)), "Allocations count is not correct")
		Ω(appDaoInfo.Allocations[0].Placeholder).To(Equal(false), "Allocation should be non placeholder")
		Ω(appDaoInfo.Allocations[0].PlaceholderUsed).To(Equal(false), "Allocation should not be replacement of ph")
		Ω(appDaoInfo.Allocations[1].Placeholder).To(Equal(false), "Allocation should be non placeholder")
		Ω(appDaoInfo.Allocations[1].PlaceholderUsed).To(Equal(false), "Allocation should not be replacement of ph")
		Ω(appDaoInfo.Allocations[2].Placeholder).To(Equal(false), "Allocation should be non placeholder")
		Ω(appDaoInfo.Allocations[2].PlaceholderUsed).To(Equal(false), "Allocation should not be replacement of ph")

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify soft GS style behaviour
	// 1. Deploy 1 job with 2 task group's:
	// a. 1 tg with un runnable placeholders
	// b. 1 tg with runnable placeholders
	// 2. Verify appState = Accepted
	// 3. Once ph's are timed out, app should move to failed state
	It("Verify_Hard_GS_Failed_State", func() {
		pdTimeout := 20
		gsStyle := "Hard"
		placeholderTimeoutStr := fmt.Sprintf("%s=%d", "placeholderTimeoutInSeconds", pdTimeout)
		gsStyleStr := fmt.Sprintf("%s=%s", "gangSchedulingStyle", gsStyle)
		groupA = groupA + "-" + common.RandSeq(5)
		groupB = groupB + "-" + common.RandSeq(5)

		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &k8s.PodAnnotation{
				TaskGroups: []interfaces.TaskGroup{
					{
						Name:         groupA,
						MinMember:    int32(3),
						MinResource:  podResources,
						NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable_node"},
					},
					{
						Name:        groupB,
						MinMember:   int32(3),
						MinResource: podResources,
					},
				},
				SchedulingPolicyParams: fmt.Sprintf("%s %s", placeholderTimeoutStr, gsStyleStr),
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(1),
			PodConfig:   podConf,
		}
		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		// Wait for placeholder timeout
		time.Sleep(time.Duration(pdTimeout) * time.Second)

		By("Verify appStatus = Failing")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"],
			yunikorn.States().Application.Failing,
			30)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are timed out and allocations count is correct as app started running normal because of 'soft' gang style
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(2), "Placeholder count is not correct")
		if appDaoInfo.PlaceholderData[0].TaskGroupName == groupB {
			Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(3)), "Placeholder count is not correct")
			Ω(int(appDaoInfo.PlaceholderData[0].TimedOut)).To(Equal(int(3)), "Placeholder timed out is not correct")
			Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(0)), "Placeholder replacement count is not correct")
		}
		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify Gang Apps FIFO order
	// Create FIFO queue with quota of 300m and 300M
	// 1. Deploy appA with 1 pod
	// 2. Deploy appB with gang of 3 pods
	// 3. Deploy appC with 1 pod
	// 4. Delete appA
	// 5. appA = Completing, appB = Running, appC = Accepted
	It("Verify_GangApp_FIFO_Order", func() {
		By(fmt.Sprintf("Creating namespace: %s", fifoQName))
		fifoQ, nsErr := kClient.CreateNamespace(fifoQName, map[string]string{
			constants.NamespaceQuota: "{\"cpu\": \"300m\", \"memory\": \"300M\"}"})
		Ω(nsErr).NotTo(HaveOccurred())
		Ω(fifoQ.Status.Phase).To(Equal(v1.NamespaceActive))
		defer func() { Ω(kClient.DeleteNamespace(fifoQName)).NotTo(HaveOccurred()) }()

		// Create appIDs
		var apps []string
		for j := 0; j < 3; j++ {
			id := fmt.Sprintf("app%d-%s", j, common.RandSeq(5))
			apps = append(apps, id)
		}

		// Initial allocation to fill ns quota
		appAllocs := map[string]map[string]int{
			apps[0]: {"pods": 1, "minMembers": 0},
			apps[1]: {"pods": 3, "minMembers": 3},
			apps[2]: {"pods": 1, "minMembers": 0},
		}
		// Expected appState progression
		appStates := map[string][]string{
			apps[0]: {"Completing"},
			apps[1]: {"Running"},
			apps[2]: {"Accepted"},
		}

		// Base pod conf
		taskGroupName := groupA + "-" + common.RandSeq(5)
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("100m"),
			"memory": resource.MustParse("100M"),
		}
		podConf := k8s.TestPodConfig{
			Annotations: &k8s.PodAnnotation{
				TaskGroupName: taskGroupName,
				TaskGroups: []interfaces.TaskGroup{
					{Name: taskGroupName, MinResource: podResources},
				},
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}

		// Deploy 3 apps in that order
		for _, appID := range apps {
			req := appAllocs[appID]
			podConf.Annotations.TaskGroups[0].MinMember = int32(req["minMembers"])
			podConf.Labels = map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": appID,
			}
			jobConf := k8s.JobConfig{
				Name:        appID,
				Namespace:   fifoQName,
				Parallelism: int32(req["pods"]),
				PodConfig:   podConf,
			}

			By(fmt.Sprintf("[%s] Deploy %d pods", appID, req["pods"]))
			job, jobErr := k8s.InitJobConfig(jobConf)
			Ω(jobErr).NotTo(HaveOccurred())
			taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
			Ω(annErr).NotTo(HaveOccurred())
			By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
			_, jobErr = kClient.CreateJob(job, fifoQName)
			Ω(jobErr).NotTo(HaveOccurred())
			createErr := kClient.WaitForJobPodsCreated(fifoQName, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
			Ω(createErr).NotTo(HaveOccurred())

			// To ensure there is minor gap between applications
			time.Sleep(1 * time.Second)
		}

		// App1 should have 2/3 placeholders running
		podConf.Annotations.TaskGroups[0].MinMember = int32(appAllocs[apps[1]]["minMembers"])
		app1Phs := yunikorn.GetPlaceholderNames(podConf.Annotations, apps[1])
		numRunningPhs := 0
		for _, placeholders := range app1Phs {
			for _, ph := range placeholders {
				runErr := kClient.WaitForPodRunning(fifoQName, ph, 30*time.Second)
				if runErr == nil {
					numRunningPhs++
				}
			}
		}
		Ω(numRunningPhs).Should(BeNumerically("==", 2))

		// Delete app0
		deleteErr := kClient.DeleteJob(apps[0], fifoQName)
		Ω(deleteErr).NotTo(HaveOccurred())

		// Now, app0=Completed, app1=Running, app2=Accepted
		for appID, states := range appStates {
			By(fmt.Sprintf("[%s] Verify appStatus = %s", appID, states[0]))
			timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, "root."+fifoQName, appID, states[0], 120)
			Ω(timeoutErr).NotTo(HaveOccurred())
		}

		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo(defaultPartition, "root."+fifoQName, apps[0])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.Allocations)).To(Equal(0), "Allocations count is not correct")
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(0), "Placeholder count is not correct")

		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo(defaultPartition, "root."+fifoQName, apps[1])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.Allocations)).To(Equal(3), "Allocations count is not correct")
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(3)), "Placeholder count is not correct")

		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo(defaultPartition, "root."+fifoQName, apps[2])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.Allocations)).To(Equal(0), "Allocations count is not correct")
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(0), "Placeholder count is not correct")

		deleteErr = kClient.DeleteJob(apps[1], fifoQName)
		Ω(deleteErr).NotTo(HaveOccurred())

		deleteErr = kClient.DeleteJob(apps[2], fifoQName)
		Ω(deleteErr).NotTo(HaveOccurred())
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

		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}

		pdTimeout := 60
		placeholderTimeoutStr := fmt.Sprintf("%s=%d", "placeholderTimeoutInSeconds", pdTimeout)
		annotations := k8s.PodAnnotation{
			SchedulingPolicyParams: placeholderTimeoutStr,
			TaskGroups: []interfaces.TaskGroup{
				{
					Name:         groupA,
					MinMember:    int32(3),
					MinResource:  podResources,
					NodeSelector: map[string]string{"kubernetes.io/hostname": workerNodes[0].Name},
				},
				{
					Name:         groupB,
					MinMember:    int32(1),
					MinResource:  podResources,
					NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable"},
				},
			},
		}

		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &annotations,
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: 3,
			PodConfig:   podConf,
		}

		// Create gang job
		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("[%s] Deploy job %s with task-groups: %+v", podConf.Labels["applicationId"], jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		By(fmt.Sprintf("[%s] Verify appStatus = Accepted", podConf.Labels["applicationId"]))
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"], yunikorn.States().Application.Accepted, 120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Wait for groupa placeholder pods running
		phNames := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		for _, ph := range phNames[groupA] {
			runErr := kClient.WaitForPodRunning(ns, ph, 30*time.Second)
			Ω(runErr).NotTo(HaveOccurred())
		}

		// Delete all groupa placeholder pods
		for i, ph := range phNames[groupA] {
			By(fmt.Sprintf("Iteration-%d: Delete placeholder %s", i, ph))
			deleteErr := kClient.DeletePod(ph, ns)
			Ω(deleteErr).NotTo(HaveOccurred())
		}

		// Wait for Yunikorn allocation removal after K8s deletion
		time.Sleep(5 * time.Second)

		// Verify app allocations correctly decremented
		appInfo, appErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(appErr).NotTo(HaveOccurred())
		Ω(len(appInfo.Allocations)).To(Equal(0), "Placeholder allocation not removed from app")

		// Verify no app allocation in nodeA
		ykNodes, nodeErr := restClient.GetNodes(defaultPartition)
		Ω(nodeErr).NotTo(HaveOccurred())
		for _, node := range *ykNodes {
			for _, alloc := range node.Allocations {
				Ω(alloc.ApplicationID).NotTo(Equal(podConf.Labels["applicationId"]), "Placeholder allocation not removed from node")
			}
		}

		// Verify queue resources = 0
		qInfo, qErr := restClient.GetQueue(defaultPartition, nsQueue)
		Ω(qErr).NotTo(HaveOccurred())
		var usedResource yunikorn.ResourceUsage
		var usedPercentageResource yunikorn.ResourceUsage
		usedResource.ParseResourceUsage(qInfo.AllocatedResource)
		Ω(usedResource.GetResourceValue(siCommon.CPU)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
		Ω(usedResource.GetResourceValue(siCommon.Memory)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
		usedPercentageResource.ParseResourceUsage(qInfo.AbsUsedCapacity)
		Ω(usedPercentageResource.GetResourceValue(siCommon.CPU)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")
		Ω(usedPercentageResource.GetResourceValue(siCommon.Memory)).Should(Equal(int64(0)), "Placeholder allocation not removed from queue")

		err = kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	// Test to verify completed placeholders cleanup
	// 1. Deploy 1 job with 2 task group's:
	// a. 1 tg with un runnable placeholders
	// b. 1 tg with runnable placeholders
	// 2. Delete job
	// 3. Verify app is completing
	// 4. Verify placeholders deleted
	// 5. Verify app allocation is empty
	It("Verify_Completed_Job_Placeholders_Cleanup", func() {
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &k8s.PodAnnotation{
				TaskGroups: []interfaces.TaskGroup{
					{
						Name:         groupA + "-" + common.RandSeq(5),
						MinMember:    int32(3),
						MinResource:  podResources,
						NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable"},
					},
					{
						Name:        groupB + "-" + common.RandSeq(5),
						MinMember:   int32(3),
						MinResource: podResources,
					},
				},
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(1),
			PodConfig:   podConf,
		}
		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		By("Verify appState = Accepted")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"], yunikorn.States().Application.Accepted, 10)
		Ω(timeoutErr).NotTo(HaveOccurred())

		By("Wait for placeholders running")
		phNames := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		tgBNames := phNames[podConf.Annotations.TaskGroups[1].Name]
		for _, ph := range tgBNames {
			runErr := kClient.WaitForPodRunning(ns, ph, 30*time.Second)
			Ω(runErr).NotTo(HaveOccurred())
		}

		By("Delete job pods")
		deleteErr := kClient.DeleteJob(jobConf.Name, ns)
		Ω(deleteErr).NotTo(HaveOccurred())
		timeoutErr = restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"], yunikorn.States().Application.Completing, 30)
		Ω(timeoutErr).NotTo(HaveOccurred())

		By("Verify placeholders deleted")
		for _, placeholders := range phNames {
			for _, ph := range placeholders {
				deleteErr = kClient.WaitForPodTerminated(ns, ph, 30*time.Second)
				Ω(deleteErr).NotTo(HaveOccurred(), "Placeholder %s still running", ph)
			}
		}

		By("Verify app allocation is empty")
		appInfo, restErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(restErr).NotTo(HaveOccurred())
		Ω(len(appInfo.Allocations)).To(BeNumerically("==", 0))
	})

	// Test to verify originator deletion will trigger placeholders cleanup
	// 1. Create an originator pod
	// 2. Not set pod ownerreference
	// 3. Delete originator pod to trigger placeholders deletion
	// 4. Verify placeholders deleted
	// 5. Verify app allocation is empty
	It("Verify_OriginatorDeletion_Trigger_Placeholders_Cleanup", func() {
		// case 1: originator pod without ownerreference
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		podConf := k8s.TestPodConfig{
			Name: "gang-driver-pod" + common.RandSeq(5),
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &k8s.PodAnnotation{
				TaskGroups: []interfaces.TaskGroup{
					{
						Name:         groupA + "-" + common.RandSeq(5),
						MinMember:    int32(3),
						MinResource:  podResources,
						NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable"},
					},
					{
						Name:        groupB + "-" + common.RandSeq(5),
						MinMember:   int32(3),
						MinResource: podResources,
					},
				},
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}

		podTest, err := k8s.InitTestPod(podConf)
		Ω(err).NotTo(HaveOccurred())
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy pod %s with task-groups: %+v", podTest.Name, taskGroupsMap[k8s.TaskGroups]))
		originator, err := kClient.CreatePod(podTest, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verify appState = Accepted")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"], yunikorn.States().Application.Accepted, 20)
		Ω(timeoutErr).NotTo(HaveOccurred())

		By("Wait for placeholders running")
		phNames := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		tgBNames := phNames[podConf.Annotations.TaskGroups[1].Name]
		for _, ph := range tgBNames {
			runErr := kClient.WaitForPodRunning(ns, ph, 30*time.Second)
			Ω(runErr).NotTo(HaveOccurred())
		}

		By("Delete originator pod")
		deleteErr := kClient.DeletePod(originator.Name, ns)
		Ω(deleteErr).NotTo(HaveOccurred())

		By("Verify placeholders deleted")
		for _, placeholders := range phNames {
			for _, ph := range placeholders {
				deleteErr = kClient.WaitForPodTerminated(ns, ph, 30*time.Second)
				Ω(deleteErr).NotTo(HaveOccurred(), "Placeholder %s still running", ph)
			}
		}

		By("Verify app allocation is empty")
		appInfo, restErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(restErr).NotTo(HaveOccurred())
		Ω(len(appInfo.Allocations)).To(BeNumerically("==", 0))

	})

	// Test to verify originator deletion will trigger placeholders cleanup
	// 1. Create an originator pod
	// 2. Set pod ownerreference with ownerreference, take configmap for example
	// 3. Delete originator pod to trigger placeholders deletion
	// 4. Verify placeholders deleted
	// 5. Verify app allocation is empty
	It("Verify_OriginatorDeletionWithOwnerreference_Trigger_Placeholders_Cleanup", func() {
		// case 2: originator pod with ownerreference
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		podConf := k8s.TestPodConfig{
			Name: "gang-driver-pod" + common.RandSeq(5),
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &k8s.PodAnnotation{
				TaskGroups: []interfaces.TaskGroup{
					{
						Name:         groupA + "-" + common.RandSeq(5),
						MinMember:    int32(3),
						MinResource:  podResources,
						NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable"},
					},
					{
						Name:        groupB + "-" + common.RandSeq(5),
						MinMember:   int32(3),
						MinResource: podResources,
					},
				},
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    podResources["cpu"],
					"memory": podResources["memory"],
				},
			},
		}

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
			err := kClient.DeleteConfigMap(testConfigmap.Name, ns)
			Ω(err).NotTo(HaveOccurred())
		}()

		testConfigmap, err := kClient.CreateConfigMap(testConfigmap, ns)
		Ω(err).NotTo(HaveOccurred())

		podTest, err := k8s.InitTestPod(podConf)
		Ω(err).NotTo(HaveOccurred())

		podTest.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       testConfigmap.Name,
				UID:        testConfigmap.UID,
			},
		}

		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy pod %s with task-groups: %+v", podTest.Name, taskGroupsMap[k8s.TaskGroups]))
		originator, err := kClient.CreatePod(podTest, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verify appState = Accepted")
		timeoutErr := restClient.WaitForAppStateTransition(defaultPartition, nsQueue, podConf.Labels["applicationId"], yunikorn.States().Application.Accepted, 20)
		Ω(timeoutErr).NotTo(HaveOccurred())

		By("Wait for placeholders running")
		phNames := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		tgBNames := phNames[podConf.Annotations.TaskGroups[1].Name]
		for _, ph := range tgBNames {
			runErr := kClient.WaitForPodRunning(ns, ph, 30*time.Second)
			Ω(runErr).NotTo(HaveOccurred())
		}

		By("Delete originator pod")
		deleteErr := kClient.DeletePod(originator.Name, ns)
		Ω(deleteErr).NotTo(HaveOccurred())

		By("Verify placeholders deleted")
		for _, placeholders := range phNames {
			for _, ph := range placeholders {
				deleteErr = kClient.WaitForPodTerminated(ns, ph, 30*time.Second)
				Ω(deleteErr).NotTo(HaveOccurred(), "Placeholder %s still running", ph)
			}
		}

		By("Verify app allocation is empty")
		appInfo, restErr := restClient.GetAppInfo(defaultPartition, nsQueue, podConf.Labels["applicationId"])
		Ω(restErr).NotTo(HaveOccurred())
		Ω(len(appInfo.Allocations)).To(BeNumerically("==", 0))
	})

	ginkgo.DescribeTable("", func(annotations k8s.PodAnnotation) {
		podConf := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-" + common.RandSeq(5),
			},
			Annotations: &annotations,
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("10m"),
					"memory": resource.MustParse("10M"),
				},
			},
		}
		jobConf := k8s.JobConfig{
			Name:        "gangjob-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(2),
			PodConfig:   podConf,
		}

		job, jobErr := k8s.InitJobConfig(jobConf)
		Ω(jobErr).NotTo(HaveOccurred())

		// Deploy job
		taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
		Ω(annErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Deploy job %s with task-groups: %+v", jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
		_, jobErr = kClient.CreateJob(job, ns)
		Ω(jobErr).NotTo(HaveOccurred())
		createErr := kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
		Ω(createErr).NotTo(HaveOccurred())

		// Validate placeholders deleted.
		tgPlaceHolders := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		for _, phNames := range tgPlaceHolders {
			for _, name := range phNames {
				phErr := kClient.WaitForPodTerminated(ns, name, time.Minute)
				Ω(phErr).NotTo(HaveOccurred())
			}
		}

		// Validate incorrect task-group definition ignored
		timeoutErr := kClient.WaitForJobPods(ns, jobConf.Name, int(jobConf.Parallelism), 30*time.Second)
		Ω(timeoutErr).NotTo(HaveOccurred())
		appPods, getErr := kClient.ListPods(ns, fmt.Sprintf("applicationId=%s", podConf.Labels["applicationId"]))
		Ω(getErr).NotTo(HaveOccurred())
		Ω(len(appPods.Items)).To(BeNumerically("==", jobConf.Parallelism))
	},
		ginkgo.Entry("Verify_TG_With_Duplicate_Group", k8s.PodAnnotation{
			TaskGroups: []interfaces.TaskGroup{
				{
					Name:      "groupdup",
					MinMember: int32(3),
					MinResource: map[string]resource.Quantity{
						"cpu":    resource.MustParse("10m"),
						"memory": resource.MustParse("10M"),
					},
				},
				{
					Name:      "groupdup",
					MinMember: int32(5),
					MinResource: map[string]resource.Quantity{
						"cpu":    resource.MustParse("10m"),
						"memory": resource.MustParse("10M"),
					},
				},
				{
					Name:      groupA,
					MinMember: int32(7),
					MinResource: map[string]resource.Quantity{
						"cpu":    resource.MustParse("10m"),
						"memory": resource.MustParse("10M"),
					},
				},
			},
		}),
		ginkgo.Entry("Verify_TG_With_Invalid_Chars", k8s.PodAnnotation{
			TaskGroups: []interfaces.TaskGroup{
				{
					Name:      "GROUPCAPS",
					MinMember: int32(3),
					MinResource: map[string]resource.Quantity{
						"cpu":    resource.MustParse("10m"),
						"memory": resource.MustParse("10M"),
					},
				},
			},
		}),
		ginkgo.Entry("Verify_TG_With_Invalid_MinMember", k8s.PodAnnotation{
			TaskGroups: []interfaces.TaskGroup{
				{
					Name:      groupA,
					MinMember: int32(-1),
					MinResource: map[string]resource.Quantity{
						"cpu":    resource.MustParse("10m"),
						"memory": resource.MustParse("10M"),
					},
				},
			},
		}),
	)
	AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)

		Ω(err).NotTo(HaveOccurred())
	})

})
