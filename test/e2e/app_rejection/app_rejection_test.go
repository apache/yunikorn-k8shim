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

package apprejection_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	// queues under root, see the suite setup
	openQueue          = "open"       // anyone can submit
	restrictedQueue    = "restricted" // only other-user can submit
	gangQueue          = "gang"       // fifo leaf with a max
	gangQueueMaxVcore  = "1"
	gangQueueMaxMemory = "1Gi"

	appFailedEventReason = "ApplicationFailed"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)
var dev string
var appID string

var _ = ginkgo.Describe("", func() {
	ginkgo.BeforeEach(func() {
		dev = "dev" + common.RandSeq(5)
		appID = "app-" + common.RandSeq(5)
		ginkgo.By("create development namespace")
		ns, err := kClient.CreateNamespace(dev, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.It("Verify_App_Rejected_No_Placement_Rule_Match", func() {
		ginkgo.By("Submit a pod without a queue: root." + dev + " does not exist and there is no root.default")
		pod := createSleepPod("sleep-no-queue", "")
		appInfo := verifyAppRejected(pod, "")
		Ω(appInfo.RejectedMessage).To(gomega.ContainSubstring("no placement rule matched"))
	})

	ginkgo.It("Verify_App_Rejected_Submit_ACL", func() {
		ginkgo.By("Submit a pod to root." + restrictedQueue + " which the user cannot submit to")
		pod := createSleepPod("sleep-acl", "root."+restrictedQueue)
		// an ACL denial shows up as a placement failure
		appInfo := verifyAppRejected(pod, "")
		Ω(appInfo.RejectedMessage).To(gomega.ContainSubstring("no placement rule matched"))
	})

	ginkgo.It("Verify_Gang_App_Rejected_Over_Queue_Max", func() {
		// 3 x 500m = 1500m, more than the 1 vcore max
		taskGroups := []cache.TaskGroup{{
			Name:      "groupa",
			MinMember: 3,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("100M"),
			},
		}}
		ginkgo.By("Submit a gang pod whose task group request is larger than the queue max")
		pod := createGangPod("gang-over-max", "root."+gangQueue, taskGroups)
		appInfo := verifyAppRejected(pod, "root."+gangQueue)
		Ω(appInfo.RejectedMessage).To(gomega.ContainSubstring("larger than max queue allocation"))

		ginkgo.By("Verify no placeholder was created")
		placeholders, err := kClient.ListPlaceholders(dev, "tg-"+appID+"-")
		Ω(err).NotTo(HaveOccurred())
		Ω(placeholders).To(gomega.BeEmpty())
	})

	ginkgo.It("Verify_Resubmit_Same_AppID_After_Rejection", func() {
		ginkgo.By("Submit a pod that gets rejected")
		pod := createSleepPod("sleep-rejected", "root."+restrictedQueue)
		rejected := verifyAppRejected(pod, "")

		ginkgo.By("Resubmit the same application ID to root." + openQueue)
		pod = createSleepPod("sleep-resubmitted", "root."+openQueue)
		err := kClient.WaitForPodRunning(dev, pod.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())
		err = restClient.WaitForAppStateTransition(configmanager.DefaultPartition, "root."+openQueue, appID,
			yunikorn.States().Application.Running, 60)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Verify the old entry is still in the rejected list")
		rejectedApp, err := restClient.GetRejectedAppInfo(configmanager.DefaultPartition, appID)
		Ω(err).NotTo(HaveOccurred())
		Ω(rejectedApp.State).To(gomega.Equal(yunikorn.States().Application.Rejected))
		Ω(rejectedApp.RejectedMessage).To(gomega.Equal(rejected.RejectedMessage))
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)
		ginkgo.By("Tear down namespace: " + dev)
		err = kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())
	})
})

// createSleepPod creates a sleep pod in the test app. An empty queue means no queue label.
func createSleepPod(name string, queue string) *v1.Pod {
	conf := k8s.SleepPodConfig{Name: name, NS: dev, AppID: appID}
	if queue != "" {
		conf.Labels = map[string]string{constants.LabelQueueName: queue}
	}
	initPod, err := k8s.InitSleepPod(conf)
	Ω(err).NotTo(HaveOccurred())
	pod, err := kClient.CreatePod(initPod, dev)
	Ω(err).NotTo(HaveOccurred())
	return pod
}

// createGangPod creates a pod with task groups. The pod is not part of a task group.
func createGangPod(name string, queue string, taskGroups []cache.TaskGroup) *v1.Pod {
	initPod, err := k8s.InitTestPod(k8s.TestPodConfig{
		Name:      name,
		Namespace: dev,
		Labels: map[string]string{
			constants.LabelApplicationID: appID,
			constants.LabelQueueName:     queue,
		},
		Annotations: &k8s.PodAnnotation{TaskGroups: taskGroups},
		Resources: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("100m"),
				v1.ResourceMemory: resource.MustParse("50M"),
			},
		},
		RestartPolicy: v1.RestartPolicyNever,
	})
	Ω(err).NotTo(HaveOccurred())
	pod, err := kClient.CreatePod(initPod, dev)
	Ω(err).NotTo(HaveOccurred())
	return pod
}

// verifyAppRejected checks the pod, its event and the rejected app list.
func verifyAppRejected(pod *v1.Pod, expectedQueue string) *dao.ApplicationDAOInfo {
	ginkgo.By("Wait for the pod to fail with reason " + constants.ApplicationRejectedFailure)
	err := kClient.WaitForPodFailed(dev, pod.Name, 60*time.Second)
	Ω(err).NotTo(HaveOccurred())
	failedPod, err := kClient.GetPod(pod.Name, dev)
	Ω(err).NotTo(HaveOccurred())
	Ω(failedPod.Status.Reason).To(gomega.Equal(constants.ApplicationRejectedFailure))
	Ω(failedPod.Status.Message).NotTo(gomega.BeEmpty())

	ginkgo.By("Wait for the " + appFailedEventReason + " warning event on the pod")
	err = kClient.WaitForPodEvent(dev, pod.Name, appFailedEventReason, 30*time.Second)
	Ω(err).NotTo(HaveOccurred())
	events, err := kClient.GetEvents(dev)
	Ω(err).NotTo(HaveOccurred())
	for _, event := range events.Items {
		if event.InvolvedObject.Name != pod.Name || event.Reason != appFailedEventReason {
			continue
		}
		Ω(event.Type).To(gomega.Equal(v1.EventTypeWarning))
		// other failures use the same reason, the message tells them apart
		Ω(event.Message).To(gomega.ContainSubstring(constants.ApplicationRejectedFailure + ": " + failedPod.Status.Message))
	}

	ginkgo.By("Wait for the application in the rejected list")
	err = restClient.WaitForRejectedApp(configmanager.DefaultPartition, appID, 30)
	Ω(err).NotTo(HaveOccurred())
	appInfo, err := restClient.GetRejectedAppInfo(configmanager.DefaultPartition, appID)
	Ω(err).NotTo(HaveOccurred())
	Ω(appInfo.State).To(gomega.Equal(yunikorn.States().Application.Rejected))
	Ω(appInfo.QueueName).To(gomega.Equal(expectedQueue))
	Ω(appInfo.RejectedMessage).To(gomega.Equal(failedPod.Status.Message))

	ginkgo.By("Verify the application is not an active application")
	_, err = restClient.GetAppInfo(configmanager.DefaultPartition, "", appID)
	Ω(err).To(gomega.MatchError(gomega.ContainSubstring("404")))
	return appInfo
}
