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

package user_group_limit_test

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	amCommon "github.com/apache/yunikorn-k8shim/pkg/admission/common"
	amconf "github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type TestType int

const (
	largeMem    = 100
	mediumMem   = 50
	smallMem    = 30
	sleepPodMem = 99
	user1       = "user1"
	user2       = "user2"
	group1      = "group1"
	group2      = "group2"

	userTestType TestType = iota
	groupTestType
)

var (
	kClient               k8s.KubeCtl
	restClient            yunikorn.RClient
	ns                    *v1.Namespace
	dev                   = "dev" + common.RandSeq(5)
	oldConfigMap          = new(v1.ConfigMap)
	annotation            = "ann-" + common.RandSeq(10)
	admissionCustomConfig = map[string]string{
		"log.core.scheduler.ugm.level":   "debug",
		amconf.AMAccessControlBypassAuth: constants.True,
	}
)

var _ = ginkgo.BeforeSuite(func() {
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("create development namespace")
	ns, err = kClient.CreateNamespace(dev, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
	ginkgo.By("Tearing down namespace: " + ns.Name)
	err = kClient.TearDownNamespace(ns.Name)
	Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.Describe("UserGroupLimit", func() {
	ginkgo.It("Verify_maxresources_with_a_specific_user_limit", func() {
		ginkgo.By("Update config")
		annotation = "ann-" + common.RandSeq(10)
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", annotation, admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, "default", "root", configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, "root.sandbox1", true, "because memory usage is less than maxresources")
		deploySleepPod(usergroup1, "root.sandbox1", false, "because final memory usage is more than maxresources")
		checkUsage(userTestType, user1, "root.sandbox1", []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		usergroup1Sandbox2Pod1 := deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")
		usergroup1Sandbox2Pod2 := deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")
		checkUsage(userTestType, user1, "root.sandbox2", []*v1.Pod{usergroup1Sandbox2Pod1, usergroup1Sandbox2Pod2})

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		usergroup2Sandbox1Pod2 := deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		checkUsage(userTestType, user2, "root.sandbox1", []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2})
	})

	ginkgo.It("Verify_maxapplications_with_a_specific_user_limit", func() {
		ginkgo.By("Update config")
		annotation = "ann-" + common.RandSeq(10)
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", annotation, admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, "default", "root", configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, "root.sandbox1", true, "because application count is less than maxapplications")
		deploySleepPod(usergroup1, "root.sandbox1", false, "because final application count is more than maxapplications")
		checkUsage(userTestType, user1, "root.sandbox1", []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		usergroup1Sandbox2Pod1 := deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")
		usergroup1Sandbox2Pod2 := deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")
		checkUsage(userTestType, user1, "root.sandbox2", []*v1.Pod{usergroup1Sandbox2Pod1, usergroup1Sandbox2Pod2})

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		usergroup2Sandbox1Pod2 := deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		checkUsage(userTestType, user2, "root.sandbox1", []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2})
	})

	ginkgo.It("Verify_maxresources_with_a_specific_group_limit", func() {
		ginkgo.By("Update config")
		annotation = "ann-" + common.RandSeq(10)
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", annotation, admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, "default", "root", configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, "root.sandbox1", true, "because memory usage is less than maxresources")
		_ = deploySleepPod(usergroup1, "root.sandbox1", false, "because final memory usage is more than maxresources")
		checkUsage(groupTestType, group1, "root.sandbox1", []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")
		deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
	})

	ginkgo.It("Verify_maxapplications_with_a_specific_group_limit", func() {
		ginkgo.By("Update config")
		annotation = "ann-" + common.RandSeq(10)
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", annotation, admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, "default", "root", configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, "root.sandbox1", true, "because application count is less than maxapplications")
		_ = deploySleepPod(usergroup1, "root.sandbox1", false, "because final application count is more than maxapplications")
		checkUsage(groupTestType, group1, "root.sandbox1", []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")
		deploySleepPod(usergroup1, "root.sandbox2", true, "because there is no limit in root.sandbox2")

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		deploySleepPod(usergroup2, "root.sandbox1", true, fmt.Sprintf("because there is no limit for %s", usergroup2))
	})

	ginkgo.It("Verify_maxresources_with_user_limit_lower_than_group_limit", func() {
		ginkgo.By("Update config")
		annotation = "ann-" + common.RandSeq(10)
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", annotation, admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, "default", "root", configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, "root.sandbox1", true, "because memory usage is less than maxresources")
		deploySleepPod(usergroup1, "root.sandbox1", false, "because final memory usage is more than maxresources in user limit")
		checkUsage(userTestType, user1, "root.sandbox1", []*v1.Pod{usergroup1Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxresources_with_group_limit_lower_than_user_limit", func() {
		ginkgo.By("Update config")
		annotation = "ann-" + common.RandSeq(10)
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", annotation, admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, "default", "root", configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, "root.sandbox1", true, "because memory usage is less than maxresources")
		_ = deploySleepPod(usergroup1, "root.sandbox1", false, "because final memory usage is more than maxresources in group limit")
		checkUsage(userTestType, user1, "root.sandbox1", []*v1.Pod{usergroup1Sandbox1Pod1})
	})

	ginkgo.AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns.Name})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		// Delete all sleep pods
		ginkgo.By("Delete all sleep pods")
		err := kClient.DeletePods(ns.Name)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pods in namespace %s - reason is %s\n", ns.Name, err.Error())
		}

		// reset config
		ginkgo.By("Restoring YuniKorn configuration")
		yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
	})
})

func deploySleepPod(usergroup *si.UserGroupInformation, queuePath string, expectedRunning bool, reason string) *v1.Pod {
	usergroupJsonBytes, err := json.Marshal(usergroup)
	Ω(err).NotTo(gomega.HaveOccurred())

	sleepPodConfig := k8s.SleepPodConfig{NS: dev, Mem: smallMem, Labels: map[string]string{constants.LabelQueueName: queuePath}}
	sleepPodObj, err := k8s.InitSleepPod(sleepPodConfig)
	Ω(err).NotTo(gomega.HaveOccurred())
	sleepPodObj.Annotations[amCommon.UserInfoAnnotation] = string(usergroupJsonBytes)

	ginkgo.By(fmt.Sprintf("%s deploys the sleep pod %s to queue %s", usergroup, sleepPodObj.Name, queuePath))
	sleepPod, err := kClient.CreatePod(sleepPodObj, dev)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())

	if expectedRunning {
		ginkgo.By(fmt.Sprintf("The sleep pod %s can be scheduled %s", sleepPod.Name, reason))
		err = kClient.WaitForPodRunning(dev, sleepPod.Name, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	} else {
		ginkgo.By(fmt.Sprintf("The sleep pod %s can't be scheduled %s", sleepPod.Name, reason))
		// Since Pending is the initial state of PodPhase, sleep for 5 seconds, then check whether the pod is still in Pending state.
		time.Sleep(5 * time.Second)
		err = kClient.WaitForPodPending(sleepPod.Namespace, sleepPod.Name, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	}
	return sleepPod
}

func checkUsage(testType TestType, name string, queuePath string, expectedRunningPods []*v1.Pod) {
	var rootQueueResourceUsageDAO *dao.ResourceUsageDAOInfo
	if testType == userTestType {
		ginkgo.By(fmt.Sprintf("Check user resource usage for %s in queue %s", name, queuePath))
		userUsageDAOInfo, err := restClient.GetUserUsage(constants.DefaultPartition, name)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(userUsageDAOInfo).NotTo(gomega.BeNil())

		rootQueueResourceUsageDAO = userUsageDAOInfo.Queues
	} else if testType == groupTestType {
		ginkgo.By(fmt.Sprintf("Check group resource usage for %s in queue %s", name, queuePath))
		groupUsageDAOInfo, err := restClient.GetGroupUsage(constants.DefaultPartition, name)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(groupUsageDAOInfo).NotTo(gomega.BeNil())

		rootQueueResourceUsageDAO = groupUsageDAOInfo.Queues
	}
	Ω(rootQueueResourceUsageDAO).NotTo(gomega.BeNil())

	var resourceUsageDAO *dao.ResourceUsageDAOInfo
	for _, queue := range rootQueueResourceUsageDAO.Children {
		if queue.QueuePath == queuePath {
			resourceUsageDAO = queue
			break
		}
	}
	Ω(resourceUsageDAO).NotTo(gomega.BeNil())

	appIDs := make([]interface{}, 0, len(expectedRunningPods))
	for _, pod := range expectedRunningPods {
		appIDs = append(appIDs, pod.Labels[constants.LabelApplicationID])
	}
	Ω(resourceUsageDAO.ResourceUsage).NotTo(gomega.BeNil())
	Ω(resourceUsageDAO.ResourceUsage.Resources["pods"]).To(gomega.Equal(resources.Quantity(len(expectedRunningPods))))
	Ω(resourceUsageDAO.RunningApplications).To(gomega.ConsistOf(appIDs...))
}
