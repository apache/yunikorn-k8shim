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
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
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

var _ = Describe("QuotaTracking: Two leaf queues for two groups", func() {
	var ns string
	BeforeEach(func() {
		ns = "ns-" + common.RandSeq(NANESPACE_LENGTH)
		By(fmt.Sprintf("Create namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	It("User quota trace with 3 users and 2 groups", func() {
		zeroPod := map[string]int64{siCommon.CPU: 0, siCommon.Memory: 0}
		onePod := map[string]int64{siCommon.CPU: 100, siCommon.Memory: 50}
		twoPods := map[string]int64{siCommon.CPU: 200, siCommon.Memory: 100}
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			queuesConfigs := []struct {
				partition, parentQueue, QueueName string
			}{
				{DEFAULT_PARTITION, "root", "group1_resources"},
				{DEFAULT_PARTITION, "root", "group2_resources"},
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
		deploySleepPod("user1-app-01", "root.group1_resources", "user1", []string{"group1"}, ns)
		deploySleepPod("user2-app-01", "root.group2_resources", "user2", []string{"group2"}, ns)
		deploySleepPod("user3-app-01", "root.group2_resources", "user3", []string{"group2"}, ns)
		deploySleepPod("user3-app-02", "root.group1_resources", "user3", []string{"group1"}, ns)

		expectedGroups := ExpectedGroupsQuota{
			2,
			map[string]ExpectedGroupQuota{
				"group1": {
					[]string{"user1-app-01", "user3-app-02"},
					map[string]ExpectedQueue{
						"root": {[]string{"user1-app-01", "user3-app-02"}, twoPods},
						"root.group1_resources": {[]string{"user1-app-01", "user3-app-02"}, twoPods},
						"root.group2_resources": {[]string{}, zeroPod},
					},
				},
				"group2": {
					[]string{"user2-app-01", "user3-app-01"},
					map[string]ExpectedQueue{
						"root": {[]string{"user2-app-01", "user3-app-01"}, twoPods},
						"root.group1_resources": {[]string{}, zeroPod},
						"root.group2_resources": {[]string{"user2-app-01", "user3-app-01"}, twoPods},
					},
				},
			},
		}
		checkGroupUsage(true, nil, expectedGroups.Groups)
		checkGroupsUsage(expectedGroups)

		expectedUsers := ExpectedUsersQuota{
			3,
			map[string]ExpectedUserQuota{
				"user1": {
					map[string]string{"user1-app-01": "group1"},
					map[string]ExpectedQueue{
						"root": {[]string{"user1-app-01"}, onePod},
						"root.group1_resources": {[]string{"user1-app-01"}, onePod},
						"root.group2_resources": {[]string{}, zeroPod},
					},
				},
				"user2": {
					map[string]string{"user2-app-01": "group2"},
					map[string]ExpectedQueue{
						"root": {[]string{"user2-app-01"}, onePod},
						"root.group1_resources": {[]string{"user2-app-01"}, onePod},
						"root.group2_resources": {[]string{}, zeroPod},
					},
				},
				"user3": {
					map[string]string{"user3-app-01": "group2", "user3-app-02": "group1"},
					map[string]ExpectedQueue{
						"root": {[]string{"user3-app-01", "user3-app-02"}, twoPods},
						"root.group1_resources": {[]string{"user3-app-02"}, onePod},
						"root.group2_resources": {[]string{"user3-app-01"}, onePod},
					},
				},
			},
		}
		checkUserUsage(true, nil, expectedUsers.Users)
		checkUsersUsage(expectedUsers)
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

func deploySleepPod(appID, queue, userName string, groupsName []string, ns string) {
	By(fmt.Sprintf("Deploy the sleep app %s to the %s namespace", appID, ns))
			sleepObj, podErr := k8s.InitSleepPod(k8s.SleepPodConfig{
				AppID:  appID,
				NS:     ns,
				Labels: map[string]string{"queue": queue},
			})
			sleepObj.ObjectMeta.Annotations["yunikorn.apache.org/user.info"] = fmt.Sprintf("{username:%s, groups:{%s}}", userName, groupsName)
			Ω(podErr).NotTo(HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, ns)
			Ω(podErr).NotTo(HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(ns,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]), WAIT_INTERVAL)
			Ω(podErr).NotTo(HaveOccurred())
}

type ExpectedQueue struct {
	RunningApplications []string
	Resources           map[string]int64
}

func checkQueue(queues *dao.ResourceUsageDAOInfo, expectedQueues map[string]ExpectedQueue) {
	var usedResource yunikorn.ResourceUsage
	for queuePath, expectedQueue := range expectedQueues {
		queue, getQueueErr := yunikorn.GetQueueResourceUsage(queues, queuePath)
		Ω(getQueueErr).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal(expectedQueue.RunningApplications))
		usedResource.ParseResourceUsage(yunikorn.ParseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(expectedQueue.Resources))
	}
}

type ExpectedGroupQuota struct {
	RunningApplications []string
	Queues              map[string]ExpectedQueue
}

func checkGroupUsage(FromREST bool, groupsUsage []*dao.GroupResourceUsageDAOInfo, expectedGroupUsage map[string]ExpectedGroupQuota) {
	var groupUsage *dao.GroupResourceUsageDAOInfo
	var err error
	for groupName, expectedGroup := range expectedGroupUsage {
		By(fmt.Sprintf("GroupUsage: Check group resource usage of %s in each queue", groupName))
		if FromREST {
			groupUsage, err = RestClient.GetGroupUsage(DEFAULT_PARTITION, groupName)
			Ω(err).NotTo(HaveOccurred())
		} else {
			groupUsage, err = yunikorn.GetGroupUsageFromGroupsUsage(groupsUsage, groupName)
			Ω(err).NotTo(HaveOccurred())
		}
		Ω(groupUsage.Applications).To(Equal(expectedGroup.RunningApplications), "running application IDs are not expected")
		checkQueue(groupUsage.Queues, expectedGroup.Queues)
	}
}

type ExpectedGroupsQuota struct {
	NumberOfGroups int
	Groups         map[string]ExpectedGroupQuota
}

func checkGroupsUsage(expectedGroups ExpectedGroupsQuota) {
	By("GroupsUsage: Check total number of groups")
	groupsUsage, err := RestClient.GetGroupsUsage(DEFAULT_PARTITION)
	Ω(err).NotTo(HaveOccurred())
	Ω(len(groupsUsage)).To(Equal(expectedGroups.NumberOfGroups), "Total number of groups is not correct")
	checkGroupUsage(false, groupsUsage, expectedGroups.Groups)
}

type ExpectedUserQuota struct {
	AppBelongingGroup map[string]string
	Queues            map[string]ExpectedQueue
}

func checkUserUsage(FromREST bool, usersUsage []*dao.UserResourceUsageDAOInfo, expectedUserUsage map[string]ExpectedUserQuota) {
	var userUsage *dao.UserResourceUsageDAOInfo
	var err error
	for userName, expectedUserUsage := range expectedUserUsage {
		By(fmt.Sprintf("UserUsage: Check group resource usage of %s in each queue", userName))
		if FromREST {
			userUsage, err = RestClient.GetUserUsage(DEFAULT_PARTITION, userName)
			Ω(err).NotTo(HaveOccurred())
		} else {
			userUsage, err = yunikorn.GetUserUsageFromUsersUsage(usersUsage, userName)
			Ω(err).NotTo(HaveOccurred())
		}
		Ω(err).NotTo(HaveOccurred())
		Ω(userUsage).To(Equal(expectedUserUsage.AppBelongingGroup))
		checkQueue(userUsage.Queues, expectedUserUsage.Queues)
	}
}

type ExpectedUsersQuota struct {
	NumberOfUsers int
	Users map[string]ExpectedUserQuota
}

func checkUsersUsage(expectedUsersUsage ExpectedUsersQuota) {
	By("UsersUsage: Check total number of users")
	usersUsage, err := RestClient.GetUsersUsage(DEFAULT_PARTITION)
	Ω(err).NotTo(HaveOccurred())
	Ω(len(usersUsage)).To(Equal(expectedUsersUsage.NumberOfUsers), "Total number of users is not correct")
	checkUserUsage(false, usersUsage, expectedUsersUsage.Users)
}