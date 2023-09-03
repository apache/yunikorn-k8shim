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

package user_quota_tracing_test

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

const (
	NANESPACE_LENGTH  = 10
	WAIT_INTERVAL     = 60
	DEFAULT_PARTITION = "default"
)

var _ = Describe("QuotaTracking: Two leaf queus for two groups", func() {
	var ns string
	BeforeEach(func() {
		ns = "ns-" + common.RandSeq(NANESPACE_LENGTH)
		By(fmt.Sprintf("Create namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	It("User qauta trace with 3 users and 2 groups", func() {
		groups := []string{"group1", "group2"}
		users := []string{"user1", "user2", "user3"}
		queuePath := []string{"root", "root.group1_resources", "root.group2_resources"}
		zeroPod := map[string]int64{siCommon.CPU: 0, siCommon.Memory: 0}
		onePod := map[string]int64{siCommon.CPU: 100, siCommon.Memory: 50}
		twoPods := map[string]int64{siCommon.CPU: 200, siCommon.Memory: 100}
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			queuesConfigs := []struct {
				partition, parentQueue, QueueName string
			}{
				{DEFAULT_PARTITION, queuePath[0], "group1_resources"},
				{DEFAULT_PARTITION, queuePath[0], "group2_resources"},
			}
			for _, queueConfig := range queuesConfigs {
				By(fmt.Sprintf("Add child queue %s to the parent queue %s", queueConfig.QueueName, queueConfig.parentQueue))
				config := configs.QueueConfig{
					Name: queueConfig.QueueName,
				}
				if err := common.AddQueue(sc, queueConfig.partition, queueConfig.parentQueue, config); err != nil {
					return err
				}
			}
			return nil
		})

		/*
		*  groups: group1, group2
		*  users: user1, user2, user3
		*  user1 -> group1
		*  user2 -> group2
		*  user3 -> group1, group2
		 */
		configs := []struct {
			AppID, Queue, UserName string
			GroupsNames            []string
		}{
			{"teacher-app-01", queuePath[1], users[0], []string{groups[0]}},
			{"students-app-01", queuePath[2], users[1], []string{groups[1]}},
			{"assistant-app-01", queuePath[2], users[2], []string{groups[1]}},
			{"assistant-app-02", queuePath[1], users[2], []string{groups[0]}},
		}
		for _, config := range configs {
			By(fmt.Sprintf("Deploy the sleep app %s to the %s namespace", config.AppID, ns))
			sleepObj, podErr := k8s.InitSleepPod(k8s.SleepPodConfig{
				AppID:  config.AppID,
				NS:     ns,
				Labels: map[string]string{"queue": config.Queue},
			})
			sleepObj.ObjectMeta.Annotations["yunikorn.apache.org/user.info"] = fmt.Sprintf("{username:%s, groups:{%s}}", config.UserName, config.GroupsNames)
			Ω(podErr).NotTo(HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, ns)
			Ω(podErr).NotTo(HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(ns,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]), WAIT_INTERVAL)
			Ω(podErr).NotTo(HaveOccurred())
		}

		type Queue struct {
			RunningApplications []string
			Resources           map[string]int64
		}
		type GroupQuota struct {
			RunningApplications []string
			Queues              map[string]Queue
		}
		type GroupsQuota struct {
			NumberOfGroups int
			Groups         map[string]GroupQuota
		}
		expectedGroups := GroupsQuota{
			2,
			map[string]GroupQuota{
				groups[0]: {
					[]string{configs[0].AppID, configs[3].AppID},
					map[string]Queue{
						queuePath[0]: {[]string{configs[0].AppID, configs[3].AppID}, twoPods},
						queuePath[1]: {[]string{configs[0].AppID, configs[3].AppID}, twoPods},
						queuePath[2]: {[]string{}, zeroPod},
					},
				},
				groups[1]: {
					[]string{configs[1].AppID, configs[2].AppID},
					map[string]Queue{
						queuePath[0]: {[]string{configs[1].AppID, configs[2].AppID}, twoPods},
						queuePath[1]: {[]string{}, zeroPod},
						queuePath[2]: {[]string{configs[1].AppID, configs[2].AppID}, twoPods},
					},
				},
			},
		}
		restClient := yunikorn.RClient{}
		var usedResource yunikorn.ResourceUsage
		for group, quota := range expectedGroups.Groups {
			By(fmt.Sprintf("GroupTracker: Check group resource usage of %s in each queue", group))
			groupUsage, getGroupErr := restClient.GetGroupResourceUsage(DEFAULT_PARTITION, group)
			Ω(getGroupErr).NotTo(HaveOccurred())
			Ω(groupUsage.Applications).To(Equal(quota.RunningApplications), "running application IDs are not expected")
			for queuePath, queueQuota := range quota.Queues {
				queue, getQueueErr := yunikorn.GetQueueResourceUsage(groupUsage.Queues, queuePath)
				Ω(getQueueErr).NotTo(HaveOccurred())
				Ω(queue.RunningApplications).To(Equal(queueQuota.RunningApplications))
				usedResource.ParseResourceUsage(yunikorn.ParseResource(queue.ResourceUsage))
				Ω(usedResource).To(Equal(queueQuota.Resources))
			}
		}

		By("GroupsTracker: Check total number of groups")
		groupsUsage, err := restClient.GetGroupsResourceUsage(DEFAULT_PARTITION)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(groupsUsage)).To(Equal(expectedGroups.NumberOfGroups), "Total number of groups is not correct")
		for group, quota := range expectedGroups.Groups {
			By(fmt.Sprintf("GroupTracker: Check group resource usage of %s in each queue", group))
			groupUsage, getGroupErr := yunikorn.GetGroupUsageFromGroupsUsage(groupsUsage, group)
			Ω(getGroupErr).NotTo(HaveOccurred())
			Ω(groupUsage.Applications).To(Equal(quota.RunningApplications), "running application IDs are not expected")
			for queuePath, queueQuota := range quota.Queues {
				queue, getQueueErr := yunikorn.GetQueueResourceUsage(groupUsage.Queues, queuePath)
				Ω(getQueueErr).NotTo(HaveOccurred())
				Ω(queue.RunningApplications).To(Equal(queueQuota.RunningApplications))
				usedResource.ParseResourceUsage(yunikorn.ParseResource(queue.ResourceUsage))
				Ω(usedResource).To(Equal(queueQuota.Resources))
			}
		}

		type UserQuota struct {
			AppBelongingGroup map[string]string
			Queues            map[string]Queue
		}
		expectedUsers := struct {
			NumberOfUsers int
			Users         map[string]UserQuota
		}{
			3,
			map[string]UserQuota{
				users[0]: {
					map[string]string{configs[0].AppID: groups[0]},
					map[string]Queue{
						queuePath[0]: {[]string{configs[0].AppID}, onePod},
						queuePath[1]: {[]string{configs[0].AppID}, onePod},
						queuePath[2]: {[]string{}, zeroPod},
					},
				},
				users[1]: {
					map[string]string{configs[1].AppID: groups[1]},
					map[string]Queue{
						queuePath[0]: {[]string{configs[1].AppID}, onePod},
						queuePath[1]: {[]string{configs[1].AppID}, onePod},
						queuePath[2]: {[]string{}, zeroPod},
					},
				},
				users[2]: {
					map[string]string{configs[2].AppID: groups[1], configs[3].AppID: groups[0]},
					map[string]Queue{
						queuePath[0]: {[]string{configs[2].AppID, configs[3].AppID}, twoPods},
						queuePath[1]: {[]string{configs[3].AppID}, onePod},
						queuePath[2]: {[]string{configs[2].AppID}, onePod},
					},
				},
			},
		}
		for user, quota := range expectedUsers.Users {
			By(fmt.Sprintf("UserTracker: Check user resource usage of %s in each queue", user))
			userUsage, getUserErr := restClient.GetUserResourceUsage(DEFAULT_PARTITION, user)
			Ω(getUserErr).NotTo(HaveOccurred())
			Ω(userUsage).To(Equal(quota.AppBelongingGroup))
			for queuePath, queueQuota := range quota.Queues {
				queue, getQueueErr := yunikorn.GetQueueResourceUsage(userUsage.Queues, queuePath)
				Ω(getQueueErr).NotTo(HaveOccurred())
				Ω(queue.RunningApplications).To(Equal(queueQuota.RunningApplications))
				usedResource.ParseResourceUsage(yunikorn.ParseResource(queue.ResourceUsage))
				Ω(usedResource).To(Equal(queueQuota.Resources))
			}
		}

		By("UsersTrackers: Check total number of users")
		usersUsage, err := restClient.GetUsersResourceUsage(DEFAULT_PARTITION)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(usersUsage)).To(Equal(expectedUsers.NumberOfUsers), "Total number of users is not correct")
		for user, quota := range expectedUsers.Users {
			By(fmt.Sprintf("UsersTrackers: Check user resource usage of %s in each queue", user))
			userUsage, getUserErr := yunikorn.GetUserUsageFromUsersUsage(usersUsage, user)
			Ω(getUserErr).NotTo(HaveOccurred())
			Ω(userUsage).To(Equal(quota.AppBelongingGroup))
			for queuePath, queueQuota := range quota.Queues {
				queue, getQueueErr := yunikorn.GetQueueResourceUsage(userUsage.Queues, queuePath)
				Ω(getQueueErr).NotTo(HaveOccurred())
				Ω(queue.RunningApplications).To(Equal(queueQuota.RunningApplications))
				usedResource.ParseResourceUsage(yunikorn.ParseResource(queue.ResourceUsage))
				Ω(usedResource).To(Equal(queueQuota.Resources))
			}
		}
	})

	AfterEach(func() {
		testDescription := CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		By("Tear down namespace: " + ns)
		err := kClient.DeleteNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
