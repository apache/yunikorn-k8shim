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
	"github.com/onsi/gomega"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("", func() {
	var kClient k8s.KubeCtl //nolint
	var restClient yunikorn.RClient
	var ns string

	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("Creating namespace: %s for sleep jobs", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	It("Verify_Annotation_TaskGroup_Def", func() {
		/*
			1. Deploy 1 job with tg definition
			3. Poll for 5 placeholders
			4. App running
			5. Deploy 1 job with 5 real pods of taskgroup
			6. Check placeholders deleted
			7. Real pods running and app running
		*/
		taskGroupName := "group-" + common.RandSeq(3)

		// Define gang member template with 5 members, 1 real pod (not part of tg)
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		annotations := k8s.PodAnnotation{
			TaskGroups: []v1alpha1.TaskGroup{
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
		timeoutErr := restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are created
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo("default", "root."+ns, podConf.Labels["applicationId"])
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
				phTermErr := kClient.WaitForPodTerminated(ns, ph, time.Minute)
				Ω(phTermErr).NotTo(HaveOccurred())
			}
		}

		// Check real gang members now running
		By("Wait for all gang members running")
		jobRunErr := kClient.WaitForJobPods(ns, realJobConf.Name, int(realJobConf.Parallelism), 30*time.Second)
		Ω(jobRunErr).NotTo(HaveOccurred())

		By("Verify appStatus = Running")
		timeoutErr = restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are replaced
		appDaoInfo, appDaoInfoErr = restClient.GetAppInfo("default", "root."+ns, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(5)), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(5)), "Placeholder replacement count is not correct")

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeleteJob(job1.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	It("Verify_Multiple_TaskGroups_Nodes", func() {
		/*
			1. Deploy 1 job with 3 tg's definitions
			2. Poll for tg's placeholders
			3. Store the nodes of placeholders by taskgroup
			4. Deploy 1 job with real pods
			5. Nodes distributions of real pods and placeholders should be the same.
		*/
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
				TaskGroups: []v1alpha1.TaskGroup{
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
		timeoutErr := restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			30)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Check placeholder node distribution is same as real pods'
		tgPlaceHolders := yunikorn.GetPlaceholderNames(podConf.Annotations, podConf.Labels["applicationId"])
		taskGroupNodes := map[string]map[string]int{}
		for tg, phNames := range tgPlaceHolders {
			taskGroupNodes[tg] = map[string]int{}
			for _, name := range phNames {
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
				phTermErr := kClient.WaitForPodTerminated(ns, ph, time.Minute)
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
				podRunErr := kClient.WaitForPodRunning(ns, pod.Name, time.Minute)
				Ω(podRunErr).NotTo(HaveOccurred())
				pod, getErr := kClient.GetPod(pod.Name, ns)
				Ω(getErr).NotTo(HaveOccurred())
				realPodNodes[tg.Name][pod.Spec.NodeName]++
			}
		}
		Ω(realPodNodes).Should(Equal(taskGroupNodes))

		By("Verify appStatus = Running")
		timeoutErr = restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			10)
		Ω(timeoutErr).NotTo(HaveOccurred())

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	It("Verify_TG_with_More_Than_minMembers", func() {
		/*
			1. Deploy 1 job with more taskGroup pods than minMembers
			2. Verify all pods running
		*/
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}
		tgName := "group-" + common.RandSeq(5)
		annotations := k8s.PodAnnotation{
			TaskGroupName: tgName,
			TaskGroups: []v1alpha1.TaskGroup{
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
		timeoutErr := restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			120)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are replaced and allocations count is correct
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo("default", "root."+ns, podConf.Labels["applicationId"])
		Ω(appDaoInfoErr).NotTo(HaveOccurred())
		Ω(len(appDaoInfo.PlaceholderData)).To(Equal(1), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Count)).To(Equal(int(3)), "Placeholder count is not correct")
		Ω(int(appDaoInfo.PlaceholderData[0].Replaced)).To(Equal(int(3)), "Placeholder replacement count is not correct")
		Ω(len(appDaoInfo.Allocations)).To(Equal(int(6)), "Allocations count is not correct")

		err := kClient.DeleteJob(job.Name, ns)
		Ω(err).NotTo(gomega.HaveOccurred())
	})

	It("Verify_Default_GS_Style", func() {
		/*
			Test validates that default GS style is soft.
			1. Submit gang job with 2 gangs
					a) ganga - 3 placeholders
					b) gangb - 1 placeholder, unsatisfiable nodeSelector
			2. After placeholder timeout, real pods should be scheduled.
		*/
		appID := "appid-" + common.RandSeq(5)
		groupA := "groupa"
		groupB := "groupb"
		podResources := map[string]resource.Quantity{
			"cpu":    resource.MustParse("10m"),
			"memory": resource.MustParse("10M"),
		}

		pdTimeout := 20
		placeholderTimeoutStr := fmt.Sprintf("%s=%d", "placeholderTimeoutInSeconds", pdTimeout)
		annotations := k8s.PodAnnotation{
			SchedulingPolicyParams: placeholderTimeoutStr,
			TaskGroups: []v1alpha1.TaskGroup{
				{Name: groupA, MinMember: int32(3), MinResource: podResources},
				{Name: groupB, MinMember: int32(1), MinResource: podResources,
					NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable_node"}},
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
		timeoutErr := restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Running,
			10)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are timed out and allocations count is correct as app started running normal because of 'soft' gang style
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo("default", "root."+ns, podConf.Labels["applicationId"])
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

	It("Verify_Hard_GS_Failed_State", func() {
		/*
			1. Deploy 1 job with 2 tg's:
				a. 1 tg with unrunnable placeholders
				b. 1 tg with runnable placeholders
			2. Verify appState = Accepted
			3. Once ph's are timed out, app should move to failed state
		*/
		pdTimeout := 20
		gsStyle := "Hard"
		placeholderTimeoutStr := fmt.Sprintf("%s=%d", "placeholderTimeoutInSeconds", pdTimeout)
		gsStyleStr := fmt.Sprintf("%s=%s", "gangSchedulingStyle", gsStyle)
		groupA := "groupa-" + common.RandSeq(5)
		groupB := "groupb-" + common.RandSeq(5)

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
				TaskGroups: []v1alpha1.TaskGroup{
					{Name: groupA, MinMember: int32(3), MinResource: podResources,
						NodeSelector: map[string]string{"kubernetes.io/hostname": "unsatisfiable_node"}},
					{Name: groupB, MinMember: int32(3), MinResource: podResources},
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
		timeoutErr := restClient.WaitForAppStateTransition("default", "root."+ns, podConf.Labels["applicationId"],
			yunikorn.States().Application.Failing,
			30)
		Ω(timeoutErr).NotTo(HaveOccurred())

		// Ensure placeholders are timed out and allocations count is correct as app started running normal because of 'soft' gang style
		appDaoInfo, appDaoInfoErr := restClient.GetAppInfo("default", "root."+ns, podConf.Labels["applicationId"])
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

	AfterEach(func() {
		testDescription := CurrentGinkgoTestDescription()
		if testDescription.Failed {
			tests.LogTestClusterInfoWrapper(testDescription.TestText, []string{ns})
		}
		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)

		Ω(err).NotTo(HaveOccurred())
	})

})
