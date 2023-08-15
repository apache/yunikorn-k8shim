package user_qauta_tracing_test

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	NANESPACE_LENGTH  = 10
	WAIT_INTERVAL     = 60
	DEFAULT_PARTITION = "default"
	ROOT_QUEUE        = "root"
	DOT               = "."
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
		yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", annotation, func(sc *configs.SchedulerConfig) error {
			// remove placement rules so we can control queue
			sc.Partitions[0].PlacementRules = nil
			queuesConfigs := []struct {
				partition, parentQueue, QueueName string
			}{
				{DEFAULT_PARTITION, ROOT_QUEUE, "students_resources"},
				{DEFAULT_PARTITION, ROOT_QUEUE, "staff_resources"},
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
			{"teacher-app-01", strings.Join([]string{ROOT_QUEUE, "staff_resources"}, DOT), "teacher", []string{"staff"}},
			{"students-app-01", strings.Join([]string{ROOT_QUEUE, "students_resources"}, DOT), "student", []string{"students"}},
			{"assistant-app-01", strings.Join([]string{ROOT_QUEUE, "students_resources"}, DOT), "assistant", []string{"students"}},
			{"assistant-app-02", strings.Join([]string{ROOT_QUEUE, "staff_resources"}, DOT), "assistant", []string{"staff"}},
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
		users, err := restClient.GetUsersResourceUsage(DEFAULT_PARTITION)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(users)).To(Equal(3), "Total number of users is not correct")
		groups, err := restClient.GetGroupsResourceUsage(DEFAULT_PARTITION)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(groups)).To(Equal(2), "Total number of groups is not correct")
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
