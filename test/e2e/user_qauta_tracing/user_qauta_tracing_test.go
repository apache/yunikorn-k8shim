package user_qauta_tracing_test

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
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

var _ = Describe("UserQuataTracing: Two leaf queus for two groups", func() {
	var ns string
	BeforeEach(func() {
		ns = "ns-" + common.RandSeq(NANESPACE_LENGTH)
		By(fmt.Sprintf("Create namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	It("User qauta trace with 3 users and 2 groups", func() {
		groups := []string{"staff", "stduent"}
		users := []string{"teacher", "student", "assistant"}
		queuePath := []string{"root", "root.staff_resources", "root.students_resources"}
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			queuesConfigs := []struct {
				partition, parentQueue, QueueName string
			}{
				{DEFAULT_PARTITION, queuePath[0], "students_resources"},
				{DEFAULT_PARTITION, queuePath[0], "staff_resources"},
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
				AppID:      config.AppID,
				NS:         ns,
				UserName:   config.UserName,
				GroupNames: config.GroupsNames,
				Labels:     map[string]string{"queue": config.Queue},
			})
			Ω(podErr).NotTo(HaveOccurred())
			sleepRespPod, podErr := kClient.CreatePod(sleepObj, ns)
			Ω(podErr).NotTo(HaveOccurred())

			// Wait for pod to move to running state
			podErr = kClient.WaitForPodBySelectorRunning(ns,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]), WAIT_INTERVAL)
			Ω(podErr).NotTo(HaveOccurred())
		}
		restClient := yunikorn.RClient{}
		var usedResource yunikorn.ResourceUsage
		By("Check total number of users and groups")
		usersUsage, err := restClient.GetUsersResourceUsage(DEFAULT_PARTITION)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(usersUsage)).To(Equal(3), "Total number of users is not correct")
		groupsUsage, err := restClient.GetGroupsResourceUsage(DEFAULT_PARTITION)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(groupsUsage)).To(Equal(2), "Total number of groups is not correct")

		By("Check user resource usage of assistant in each queue")
		userUsage, err := restClient.GetUserResourceUsage(DEFAULT_PARTITION, users[2])
		Ω(err).NotTo(HaveOccurred())
		Ω(userUsage).To(Equal(map[string]string{configs[2].AppID: groups[1], configs[3].AppID: groups[0]}))
		queue, err := restClient.GetQueueFromUserResourceUsage(usersUsage, queuePath[0], users[2])
		Ω(err).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal([]string{configs[2].AppID, configs[3].AppID}))
		usedResource.ParseResourceUsage(parseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(map[string]int64{siCommon.CPU: 200, siCommon.Memory: 100}))
		queue, err = restClient.GetQueueFromUserResourceUsage(usersUsage, queuePath[1], users[2])
		Ω(err).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal([]string{configs[3].AppID}))
		usedResource.ParseResourceUsage(parseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(map[string]int64{siCommon.CPU: 100, siCommon.Memory: 50}))
		queue, err = restClient.GetQueueFromUserResourceUsage(usersUsage, queuePath[2], users[2])
		Ω(err).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal([]string{configs[2].AppID}))
		usedResource.ParseResourceUsage(parseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(map[string]int64{siCommon.CPU: 100, siCommon.Memory: 50}))

		By("Check group resource usage of staff in each queue")
		groupUsage, err := restClient.GetGroupResourceUsage(DEFAULT_PARTITION, groups[0])
		Ω(err).NotTo(HaveOccurred())
		Ω(groupUsage.Applications).To(Equal([]string{configs[0].AppID, configs[3].AppID}), "running application IDs are not expected")
		queue, err = restClient.GetQueueFromGroupResourceUsage(groupsUsage, queuePath[0], groups[0])
		Ω(err).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal([]string{configs[0].AppID, configs[3].AppID}))
		usedResource.ParseResourceUsage(parseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(map[string]int64{siCommon.CPU: 200, siCommon.Memory: 100}))
		queue, err = restClient.GetQueueFromGroupResourceUsage(groupsUsage, queuePath[1], groups[0])
		Ω(err).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal([]string{configs[0].AppID, configs[3].AppID}))
		usedResource.ParseResourceUsage(parseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(map[string]int64{siCommon.CPU: 200, siCommon.Memory: 100}))
		queue, err = restClient.GetQueueFromGroupResourceUsage(groupsUsage, queuePath[2], groups[0])
		Ω(err).NotTo(HaveOccurred())
		Ω(queue.RunningApplications).To(Equal([]string{}))
		usedResource.ParseResourceUsage(parseResource(queue.ResourceUsage))
		Ω(usedResource).To(Equal(map[string]int64{siCommon.CPU: 0, siCommon.Memory: 0}))
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

func parseResource(res *resources.Resource) map[string]int64 {
	result := make(map[string]int64)
	for key, value := range res.Resources {
		result[key] = int64(value)
	}
	return result
}
