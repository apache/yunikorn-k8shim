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
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	amCommon "github.com/apache/yunikorn-k8shim/pkg/admission/common"
	amconf "github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type TestType int

const (
	largeMem      = 100
	mediumMem     = 50
	smallMem      = 30
	sleepPodMem   = 99
	user1         = "user1"
	user2         = "user2"
	group1        = "group1"
	group2        = "group2"
	sandboxQueue1 = "root.sandbox1"
	sandboxQueue2 = "root.sandbox2"

	userTestType TestType = iota
	groupTestType
)

var (
	suiteName             string
	kClient               k8s.KubeCtl
	restClient            yunikorn.RClient
	oldConfigMap          = new(v1.ConfigMap)
	admissionCustomConfig = map[string]string{
		"log.core.scheduler.ugm.level":   "debug",
		amconf.AMAccessControlBypassAuth: constants.True,
	}
)

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
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
})

var _ = ginkgo.BeforeEach(func() {
	dev = "dev" + common.RandSeq(5)
	ginkgo.By("create development namespace")
	ns, err := kClient.CreateNamespace(dev, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
	ginkgo.By("Tearing down namespace: " + dev)
	err = kClient.TearDownNamespace(dev)
	Ω(err).NotTo(HaveOccurred())
})

var _ = ginkgo.Describe("UserGroupLimit", func() {
	ginkgo.It("Verify_maxresources_with_a_specific_user_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than maxresources")
		deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than maxresources")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		usergroup1Sandbox2Pod1 := deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")
		usergroup1Sandbox2Pod2 := deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")
		checkUsage(userTestType, user1, sandboxQueue2, []*v1.Pod{usergroup1Sandbox2Pod1, usergroup1Sandbox2Pod2})

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		usergroup2Sandbox1Pod2 := deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2})
	})

	ginkgo.It("Verify_maxapplications_with_a_specific_user_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because application count is less than maxapplications")
		deploySleepPod(usergroup1, sandboxQueue1, false, "because final application count is more than maxapplications")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		usergroup1Sandbox2Pod1 := deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")
		usergroup1Sandbox2Pod2 := deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")
		checkUsage(userTestType, user1, sandboxQueue2, []*v1.Pod{usergroup1Sandbox2Pod1, usergroup1Sandbox2Pod2})

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		usergroup2Sandbox1Pod2 := deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2})
	})

	ginkgo.It("Verify_maxresources_with_a_specific_group_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than maxresources")
		_ = deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than maxresources")
		checkUsage(groupTestType, group1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")
		deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
	})

	ginkgo.It("Verify_maxapplications_with_a_specific_group_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because application count is less than maxapplications")
		_ = deploySleepPod(usergroup1, sandboxQueue1, false, "because final application count is more than maxapplications")
		checkUsage(groupTestType, group1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup1 can deploy 2 sleep pods to root.sandbox2
		deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")
		deploySleepPod(usergroup1, sandboxQueue2, true, "because there is no limit in root.sandbox2")

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
		deploySleepPod(usergroup2, sandboxQueue1, true, fmt.Sprintf("because there is no limit for %s", usergroup2))
	})

	ginkgo.It("Verify_maxresources_with_user_limit_lower_than_group_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than maxresources")
		deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than maxresources in user limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxresources_with_group_limit_lower_than_user_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than maxresources")
		_ = deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than maxresources in group limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxresources_with_a_wildcard_user_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
							Limit:           "wildcard user entry",
							Users:           []string{"*"},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				})
			})
		})

		// usergroup1 can deploy 2 sleep pods to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is less than user entry limit")
		usergroup1Sandbox1Pod2 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is equal to user entry limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1, usergroup1Sandbox1Pod2})

		// usergroup2 can deploy 1 sleep pod to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, "because usage is less than wildcard user entry limit")

		// usergroup2 can't deploy the second sleep pod to root.sandbox1
		deploySleepPod(usergroup2, sandboxQueue1, false, "because final memory usage is more than wildcard maxresources")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxapplications_with_a_wildcard_user_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
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
							Limit:           "wildcard user entry",
							Users:           []string{"*"},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				})
			})
		})

		// usergroup1 can deploy 2 sleep pods to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is less than user entry limit")
		usergroup1Sandbox1Pod2 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is equal to user entry limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1, usergroup1Sandbox1Pod2})

		// usergroup2 can deploy 1 sleep pod to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, "because usage is less than wildcard user entry limit")

		// usergroup2 can't deploy the second sleep pod to root.sandbox1
		deploySleepPod(usergroup2, sandboxQueue1, false, "because final application count is more than wildcard maxapplications")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxresources_with_a_wildcard_group_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
						{
							Limit:           "wildcard group entry",
							Groups:          []string{"*"},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				})
			})
		})

		// usergroup1 can deploy 2 sleep pods to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is less than user entry limit")
		usergroup1Sandbox1Pod2 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is equal to user entry limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1, usergroup1Sandbox1Pod2})

		// usergroup2 can deploy 1 sleep pod to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, "because usage is less than wildcard user entry limit")

		// usergroup2 can't deploy the second sleep pod to root.sandbox1
		deploySleepPod(usergroup2, sandboxQueue1, false, "because final memory usage is more than wildcard maxresources")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxapplications_with_a_wildcard_group_limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
						{
							Limit:           "wildcard group entry",
							Groups:          []string{"*"},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				})
			})
		})

		// usergroup1 can deploy 2 sleep pods to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is less than group entry limit")
		usergroup1Sandbox1Pod2 := deploySleepPod(usergroup1, sandboxQueue1, true, "because usage is equal to group entry limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1, usergroup1Sandbox1Pod2})

		// usergroup2 can deploy 1 sleep pod to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, "because usage is less than wildcard group entry limit")

		// usergroup2 can't deploy the second sleep pod to root.sandbox1
		deploySleepPod(usergroup2, sandboxQueue1, false, "because final application count is more than wildcard maxapplications")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxresources_with_a_valid_user_name_and_specific_user_limit", func() {
		ginkgo.By("Update config")
		validUser := "user_Test-a_b_#_c_#_d_/_e@dom:ain.com"
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{validUser},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: validUser, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than maxresources")
		deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than maxresources")
		checkUsage(userTestType, url.QueryEscape(validUser), sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})
	})

	ginkgo.It("Verify_maxresources_with_a_valid_group_name_and_specific_group_limit", func() {
		ginkgo.By("Update config")
		validGroup := "group_Test-a_b_dom:ain.com"
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				if err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{validGroup},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
					},
				}); err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{validGroup}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than maxresources")
		_ = deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than maxresources")
		checkUsage(groupTestType, url.QueryEscape(validGroup), sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})
	})

	ginkgo.It("Verify_Queue_Name_With_Special_Characters", func() {
		ginkgo.By("Create a queue with a name that includes all allowed special characters")
		queueName := "root_test22-a_b_#_c_#_d_/_e@dom:ain"
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       queueName,
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", 200)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}
			return nil
		})
		ginkgo.By("Fetch the queue information using the REST API")
		queueInfo, err := restClient.GetQueue("default", "root."+url.QueryEscape(queueName), false)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(queueInfo.QueueName).To(gomega.Equal("root." + queueName))
	})

	ginkgo.It("Verify_Queue_Name_With_Disallowed_Special_Characters", func() {
		ginkgo.By("Attempt to create a queue with a name that includes disallowed special characters")
		invalidConfig := `
			partitions:
  				- name: default
				  placementrules:
                       - name: tag
                         value: namespace
                         create: true
                  queues:
					   - name: root_test22-a_b_#_c_#_d__e@dom:ain$
						 submitacl: '*'
`
		queueName := "root_test22-a_b_#_c_#_d__e@dom:ain$"
		invalidConfigData := map[string]string{"queues.yaml": invalidConfig}
		invalidConfigMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: invalidConfigData,
		}
		_, invalidConfigErr := kClient.UpdateConfigMap(invalidConfigMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(invalidConfigErr).Should(gomega.HaveOccurred())
		ginkgo.By("Verify that the queue was not created")
		_, err := restClient.GetQueue("default", "root."+url.QueryEscape(queueName), false)
		gomega.Ω(err).To(gomega.HaveOccurred()) // Expect an error
		queueName2 := "root_test22"
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       queueName2,
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", 200)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}
			return nil
		})
	})

	ginkgo.It("Verify user limit and wildcard user limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil
				err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
						{
							Limit:           "wildcard user entry",
							Users:           []string{"*"},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				})
				if err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})

		// usergroup1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}

		// usergroup1 can't deploy the second sleep pod to root.sandbox1
		usergroup1Sandbox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because memory usage is less than user entry limit")
		_ = deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than user entry limit")
		checkUsage(userTestType, user1, sandboxQueue1, []*v1.Pod{usergroup1Sandbox1Pod1})

		// usergroup2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, "because there is no limit for usergroup2")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1})

		// usergroup2 can deploy the second sleep pod to root.sandbox1
		usergroup2 = &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod2 := deploySleepPod(usergroup2, sandboxQueue1, true, "because there is no limit for usergroup2")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2})

		// usergroup2 can't deploy the third sleep pod to root.sandbox1 because of max-application limit
		usergroup2 = &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		usergroup2Sandbox1Pod3 := deploySleepPod(usergroup2, sandboxQueue1, false, "because final memory usage is more than wildcard maxapplications")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2})

		// Update Wildcard user entry limit to 3
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
						{
							Limit:           "wildcard user entry",
							Users:           []string{"*"},
							MaxApplications: 3,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					},
				})
				if err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})
		// usergroup2 can deploy the third sleep pod to root.sandbox1 becuase of max-application limit updated to 3
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2, usergroup2Sandbox1Pod3})
		// usergroup2 can't deploy the fourth sleep pod to root.sandbox1 because of max-application limit
		usergroup2 = &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		_ = deploySleepPod(usergroup2, sandboxQueue1, false, "because final memory usage is more than wildcard maxapplications")
		checkUsage(userTestType, user2, sandboxQueue1, []*v1.Pod{usergroup2Sandbox1Pod1, usergroup2Sandbox1Pod2, usergroup2Sandbox1Pod3})

	})

	ginkgo.It("Verify group limit and wildcard group limit", func() {
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
						{
							Limit:           "wildcard group entry",
							Groups:          []string{"*"},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					}})
				if err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})
		// group1 can deploy the first sleep pod to root.sandbox1
		usergroup1 := &si.UserGroupInformation{User: user1, Groups: []string{group1}}
		group1Sandvox1Pod1 := deploySleepPod(usergroup1, sandboxQueue1, true, "because there is no limit for group1")
		checkUsage(groupTestType, group1, sandboxQueue1, []*v1.Pod{group1Sandvox1Pod1})

		// group1 can't deploy the second sleep pod to root.sandbox1
		usergroup1 = &si.UserGroupInformation{User: user1, Groups: []string{group1}}
		_ = deploySleepPod(usergroup1, sandboxQueue1, false, "because final memory usage is more than group entry limit")
		checkUsage(groupTestType, group1, sandboxQueue1, []*v1.Pod{group1Sandvox1Pod1})

		// group2 can deploy 2 sleep pods to root.sandbox1
		usergroup2 := &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		group2Sandbox1Pod1 := deploySleepPod(usergroup2, sandboxQueue1, true, "because there is no limit for group2")
		checkUsageWildcardGroups(groupTestType, group2, sandboxQueue1, []*v1.Pod{group2Sandbox1Pod1})

		// group2 can deploy the second sleep pod to root.sandbox1
		usergroup2 = &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		group2Sandbox1Pod2 := deploySleepPod(usergroup2, sandboxQueue1, true, "because there is no limit for group2")
		checkUsageWildcardGroups(groupTestType, group2, sandboxQueue1, []*v1.Pod{group2Sandbox1Pod1, group2Sandbox1Pod2})

		// group2 can't deploy the third sleep pod to root.sandbox1 because of max-application limit
		usergroup2 = &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		group2Sandbox1Pod3 := deploySleepPod(usergroup2, sandboxQueue1, false, "because final memory usage is more than wildcard maxapplications")
		checkUsageWildcardGroups(groupTestType, group2, sandboxQueue1, []*v1.Pod{group2Sandbox1Pod1, group2Sandbox1Pod2})
		// Update Wildcard group entry limit to 3
		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil

				err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "sandbox1",
					Limits: []configs.Limit{
						{
							Limit:           "group entry",
							Groups:          []string{group1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
						{
							Limit:           "wildcard group entry",
							Groups:          []string{"*"},
							MaxApplications: 3,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					}})
				if err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})
		// group2 can deploy the third sleep pod to root.sandbox1 becuase of max-application limit updated to 3
		checkUsageWildcardGroups(groupTestType, group2, sandboxQueue1, []*v1.Pod{group2Sandbox1Pod1, group2Sandbox1Pod2, group2Sandbox1Pod3})
		// group2 can't deploy the fourth sleep pod to root.sandbox1 because of max-application limit
		usergroup2 = &si.UserGroupInformation{User: user2, Groups: []string{group2}}
		_ = deploySleepPod(usergroup2, sandboxQueue1, false, "because final memory usage is more than wildcard maxapplications")
		checkUsageWildcardGroups(groupTestType, group2, sandboxQueue1, []*v1.Pod{group2Sandbox1Pod1, group2Sandbox1Pod2, group2Sandbox1Pod3})
	})

	ginkgo.It("Verify User info for the non kube admin user", func() {
		var clientset *kubernetes.Clientset
		var namespace = "default"
		var serviceAccountName = "test-user-sa"
		var podName = "test-pod"
		var secretName = "test-user-sa-token" // #nosec G101

		ginkgo.By("Update config")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		admissionCustomConfig = map[string]string{
			"log.core.scheduler.ugm.level":   "debug",
			amconf.AMAccessControlBypassAuth: constants.False,
		}
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			yunikorn.UpdateCustomConfigMapWrapperWithMap(oldConfigMap, "", admissionCustomConfig, func(sc *configs.SchedulerConfig) error {
				// remove placement rules so we can control queue
				sc.Partitions[0].PlacementRules = nil
				err := common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{
					Name: "default",
					Limits: []configs.Limit{
						{
							Limit:           "user entry",
							Users:           []string{user1},
							MaxApplications: 1,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", mediumMem),
							},
						},
						{
							Limit:           "user2 entry",
							Users:           []string{user2},
							MaxApplications: 2,
							MaxResources: map[string]string{
								siCommon.Memory: fmt.Sprintf("%dM", largeMem),
							},
						},
					}})
				if err != nil {
					return err
				}
				return common.AddQueue(sc, constants.DefaultPartition, constants.RootQueue, configs.QueueConfig{Name: "sandbox2"})
			})
		})
		defer func() {
			// cleanup
			ginkgo.By("Cleaning up resources...")
			err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{})
			gomega.Ω(err).NotTo(HaveOccurred())
			err = clientset.CoreV1().ServiceAccounts(namespace).Delete(context.TODO(), serviceAccountName, metav1.DeleteOptions{})
			gomega.Ω(err).NotTo(HaveOccurred())
			err = kClient.DeleteClusterRole("pod-creator-role")
			gomega.Ω(err).NotTo(HaveOccurred())
			err = kClient.DeleteClusterRoleBindings("pod-creator-role-binding")
			gomega.Ω(err).NotTo(HaveOccurred())
		}()
		// Create Service Account
		ginkgo.By("Creating Service Account...")
		sa, err := kClient.CreateServiceAccount(serviceAccountName, namespace)
		gomega.Ω(err).NotTo(HaveOccurred())
		// Create a ClusterRole with necessary permissions
		ginkgo.By("Creating ClusterRole...")
		clusterRole := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-creator-role",
			},
			Rules: []rbacv1.PolicyRule{
				{
					APIGroups: []string{""},
					Resources: []string{"pods", "serviceaccounts", "test-user-sa"},
					Verbs:     []string{"create", "get", "list", "watch", "delete"},
				},
			},
		}
		_, err = kClient.CreateClusterRole(clusterRole)
		gomega.Ω(err).NotTo(HaveOccurred())
		// Create a ClusterRoleBinding to bind the ClusterRole to the service account
		ginkgo.By("Creating ClusterRoleBinding...")
		clusterRoleBinding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-creator-role-binding",
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "pod-creator-role",
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      sa.Name,
					Namespace: namespace,
				},
			},
		}
		_, err = kClient.CreateClusterRoleBinding(clusterRoleBinding.ObjectMeta.Name, clusterRoleBinding.RoleRef.Name, clusterRoleBinding.Subjects[0].Namespace, clusterRoleBinding.Subjects[0].Name)
		gomega.Ω(err).NotTo(HaveOccurred())
		// Create a Secret for the Service Account
		ginkgo.By("Creating Secret for the Service Account...")
		// create a object of v1.Secret
		secret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: secretName,
				Annotations: map[string]string{
					"kubernetes.io/service-account.name": serviceAccountName,
				},
			},
			Type: v1.SecretTypeServiceAccountToken,
		}
		_, err = kClient.CreateSecret(secret, namespace)
		gomega.Ω(err).NotTo(HaveOccurred())
		// Get the token value from the Secret
		ginkgo.By("Getting the token value from the Secret...")
		userTokenValue, err := kClient.GetSecretValue(namespace, secretName, "token")
		gomega.Ω(err).NotTo(HaveOccurred())
		// use deep copy not to hardcode the kubeconfig
		config, err := kClient.GetKubeConfig()
		gomega.Ω(err).NotTo(HaveOccurred())
		config.BearerToken = userTokenValue
		newConf := rest.CopyConfig(config) // copy existing config
		// Use token-based authentication instead of client certificates
		newConf.CAFile = ""
		newConf.CertFile = ""
		newConf.KeyFile = ""
		newConf.BearerToken = userTokenValue
		kubeconfigPath := filepath.Join(os.TempDir(), "test-user-config")
		err = k8s.WriteConfigToFile(newConf, kubeconfigPath)
		gomega.Ω(err).NotTo(HaveOccurred())
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		gomega.Ω(err).NotTo(HaveOccurred())
		clientset, err = kubernetes.NewForConfig(config)
		gomega.Ω(err).NotTo(HaveOccurred())
		ginkgo.By("Creating Pod...")
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
				Annotations: map[string]string{
					"created-by": fmt.Sprintf("system:serviceaccount:%s:%s", namespace, serviceAccountName),
					"user-token": userTokenValue, // Log the token in the annotation
				},
				Labels: map[string]string{"applicationId": "test-app"},
			},
			Spec: v1.PodSpec{
				ServiceAccountName: serviceAccountName,
				Containers: []v1.Container{
					{
						Name:  "nginx",
						Image: "nginx",
						Ports: []v1.ContainerPort{{ContainerPort: 80}},
					},
				},
			},
		}
		_, err = clientset.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		gomega.Ω(err).NotTo(HaveOccurred())
		createdPod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		gomega.Ω(err).NotTo(HaveOccurred())
		ginkgo.By("Verifying User Info...")
		userInfo, err := GetUserInfoFromPodAnnotation(createdPod)
		gomega.Ω(err).NotTo(HaveOccurred())
		// user info should contain the substring "system:serviceaccount:default:test-user-sa"
		gomega.Ω(strings.Contains(fmt.Sprintf("%v", userInfo), "system:serviceaccount:default:test-user-sa")).To(gomega.BeTrue())
		queueName2 := "root_22"
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			var err error
			if err = common.AddQueue(sc, "default", "root", configs.QueueConfig{
				Name:       queueName2,
				Resources:  configs.Resources{Guaranteed: map[string]string{"memory": fmt.Sprintf("%dM", 200)}},
				Properties: map[string]string{"preemption.delay": "1s"},
			}); err != nil {
				return err
			}
			return nil
		})
	})
	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
		ginkgo.By("Tearing down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())
		// reset config
		ginkgo.By("Restoring YuniKorn configuration")
		yunikorn.RestoreConfigMapWrapper(oldConfigMap)
	})
})

func GetUserInfoFromPodAnnotation(pod *v1.Pod) (*si.UserGroupInformation, error) {
	userInfo, ok := pod.Annotations[amCommon.UserInfoAnnotation]
	if !ok {
		return nil, fmt.Errorf("user info not found in pod annotation")
	}
	var userInfoObj si.UserGroupInformation
	err := json.Unmarshal([]byte(userInfo), &userInfoObj)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal user info from pod annotation")
	}
	return &userInfoObj, nil
}

func deploySleepPod(usergroup *si.UserGroupInformation, queuePath string, expectedRunning bool, reason string) *v1.Pod {
	usergroupJsonBytes, err := json.Marshal(usergroup)
	Ω(err).NotTo(HaveOccurred())

	sleepPodConfig := k8s.SleepPodConfig{NS: dev, Mem: smallMem, Labels: map[string]string{constants.LabelQueueName: queuePath}}
	sleepPodObj, err := k8s.InitSleepPod(sleepPodConfig)
	Ω(err).NotTo(HaveOccurred())
	sleepPodObj.Annotations[amCommon.UserInfoAnnotation] = string(usergroupJsonBytes)

	ginkgo.By(fmt.Sprintf("%s deploys the sleep pod %s to queue %s", usergroup, sleepPodObj.Name, queuePath))
	sleepPod, err := kClient.CreatePod(sleepPodObj, dev)
	gomega.Ω(err).NotTo(HaveOccurred())

	if expectedRunning {
		ginkgo.By(fmt.Sprintf("The sleep pod %s can be scheduled %s", sleepPod.Name, reason))
		err = kClient.WaitForPodRunning(dev, sleepPod.Name, 60*time.Second)
		gomega.Ω(err).NotTo(HaveOccurred())
	} else {
		ginkgo.By(fmt.Sprintf("The sleep pod %s can't be scheduled %s", sleepPod.Name, reason))
		// Since Pending is the initial state of PodPhase, sleep for 5 seconds, then check whether the pod is still in Pending state.
		time.Sleep(5 * time.Second)
		err = kClient.WaitForPodPending(sleepPod.Namespace, sleepPod.Name, 60*time.Second)
		gomega.Ω(err).NotTo(HaveOccurred())
	}
	return sleepPod
}

func checkUsage(testType TestType, name string, queuePath string, expectedRunningPods []*v1.Pod) {
	var rootQueueResourceUsageDAO *dao.ResourceUsageDAOInfo
	if testType == userTestType {
		ginkgo.By(fmt.Sprintf("Check user resource usage for %s in queue %s", name, queuePath))
		userUsageDAOInfo, err := restClient.GetUserUsage(constants.DefaultPartition, name)
		Ω(err).NotTo(HaveOccurred())
		Ω(userUsageDAOInfo).NotTo(gomega.BeNil())

		rootQueueResourceUsageDAO = userUsageDAOInfo.Queues
	} else if testType == groupTestType {
		ginkgo.By(fmt.Sprintf("Check group resource usage for %s in queue %s", name, queuePath))
		groupUsageDAOInfo, err := restClient.GetGroupUsage(constants.DefaultPartition, name)
		Ω(err).NotTo(HaveOccurred())
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
	Ω(resourceUsageDAO.ResourceUsage["pods"]).To(gomega.Equal(int64(len(expectedRunningPods))))
	Ω(resourceUsageDAO.RunningApplications).To(gomega.ConsistOf(appIDs...))
}

func checkUsageWildcardGroups(testType TestType, name string, queuePath string, expectedRunningPods []*v1.Pod) {
	var rootQueueResourceUsageDAO *dao.ResourceUsageDAOInfo
	if testType == groupTestType {
		ginkgo.By(fmt.Sprintf("Check group resource usage for %s in queue %s", name, queuePath))
		groupUsageDAOInfo, err := restClient.GetGroupsUsage(constants.DefaultPartition)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(groupUsageDAOInfo).NotTo(gomega.BeNil())
		for _, groupUsageDAOInfog := range groupUsageDAOInfo {
			if groupUsageDAOInfog.GroupName == "*" {
				rootQueueResourceUsageDAO = groupUsageDAOInfog.Queues
			}
		}
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
	Ω(resourceUsageDAO.ResourceUsage["pods"]).To(gomega.Equal(int64(len(expectedRunningPods))))
	Ω(resourceUsageDAO.RunningApplications).To(gomega.ConsistOf(appIDs...))
}
