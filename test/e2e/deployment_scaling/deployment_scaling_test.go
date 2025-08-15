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

package deployment_scaling_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("Deployment Scaling Tests", func() {
	var ns string
	var dev string

	BeforeEach(func() {
		ns = "deployment-scaling-" + common.RandSeq(10)
		dev = "dev-" + common.RandSeq(5)

		By(fmt.Sprintf("Creating namespace: %s", ns))
		nsObj, err := kClient.CreateNamespace(ns, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(nsObj.Status.Phase).To(Equal(v1.NamespaceActive))

		By(fmt.Sprintf("Creating development namespace: %s", dev))
		devObj, err := kClient.CreateNamespace(dev, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(devObj.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	// Test deployment scaling behavior with YuniKorn scheduler: scale 2->3->2 with queue verification
	It("Verify_Pod_Scaling_With_YuniKorn_Scheduler", func() {
		By("Creating initial deployment with 2 replicas")
		appName := "scaling-app-" + common.RandSeq(5)

		// Create a deployment with resource requests to enable scaling
		deployment := createTestDeployment(appName, ns, 2, map[string]string{
			"cpu":    "100m",
			"memory": "128Mi",
		})

		_, err := kClient.CreateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for initial 2 pods to be created and running")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 120)
		Ω(err).NotTo(HaveOccurred())

		// Verify we have exactly 2 running pods
		err = kClient.WaitForPodCountBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 2, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Scaling deployment up from 2 to 3 replicas")
		threeReplicas := int32(3)
		deployment.Spec.Replicas = &threeReplicas
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying 3 pods are running and scheduled by YuniKorn")
		err = kClient.WaitForPodCountBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 3, 90*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying new pod is scheduled in the proper queue")
		podsAfterScaleUp, err := kClient.GetPods(ns)
		Ω(err).NotTo(HaveOccurred())

		runningPods := 0
		for i := range podsAfterScaleUp.Items {
			pod := &podsAfterScaleUp.Items[i]
			if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
				Ω(pod.Spec.SchedulerName).To(Equal("yunikorn"))
				runningPods++
			}
		}
		Ω(runningPods).To(Equal(3))

		// Verify YuniKorn application is in running state
		appInfo, err := restClient.GetAppInfo("default", "root."+ns, deployment.Spec.Template.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appInfo).NotTo(BeNil())
		Ω(appInfo.State).To(Equal("Running"))
		Ω(len(appInfo.Allocations)).To(BeNumerically(">=", 3))

		By("Scaling deployment down from 3 to 2 replicas")
		twoReplicas := int32(2)
		deployment.Spec.Replicas = &twoReplicas
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scale down to exactly 2 running pods")
		err = kClient.WaitForPodCountBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 2, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying remaining pods are healthy and scheduled by YuniKorn")
		finalPods, finalErr := kClient.GetPods(ns)
		Ω(finalErr).NotTo(gomega.HaveOccurred())

		finalRunningPods := 0
		for i := range finalPods.Items {
			pod := &finalPods.Items[i]
			if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
				Ω(pod.Spec.SchedulerName).To(Equal("yunikorn"))
				finalRunningPods++
			}
		}
		Ω(finalRunningPods).To(Equal(2))
	})

	// Test preemption during deployment scaling operations
	It("Verify_Preemption_During_Deployment_Scaling_Operations", func() {
		By("Testing preemption behavior when deployment scaling operations are in progress")

		// Create high-priority and low-priority deployments
		highPriorityApp := "high-priority-app-" + common.RandSeq(5)
		lowPriorityApp := "low-priority-app-" + common.RandSeq(5)

		// Create low-priority deployment first (consumes resources)
		lowPriorityDeployment := createTestDeployment(lowPriorityApp, ns, 3, map[string]string{
			"cpu":    "50m",
			"memory": "64Mi",
		})
		// Add queue and priority annotations
		lowPriorityDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":    "root.low-priority",
			"yunikorn.apache.org/priority": "0",
		}

		_, err := kClient.CreateDeployment(lowPriorityDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for low-priority pods to be running")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", lowPriorityApp), 90)
		Ω(err).NotTo(HaveOccurred())

		By("Creating high-priority deployment that should trigger preemption")
		highPriorityDeployment := createTestDeployment(highPriorityApp, ns, 2, map[string]string{
			"cpu":    "100m",
			"memory": "128Mi",
		})
		// Add queue and priority annotations
		highPriorityDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":           "root.production",
			"yunikorn.apache.org/priority-class":  "high-priority",
			"yunikorn.apache.org/priority-policy": "30",
		}

		_, err = kClient.CreateDeployment(highPriorityDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Scaling high-priority deployment to 4 replicas to trigger more preemption")
		highReplicas := int32(4)
		highPriorityDeployment.Spec.Replicas = &highReplicas
		_, err = kClient.UpdateDeployment(highPriorityDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying high-priority pods scale up")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", highPriorityApp), 90)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying preemption effects on low-priority pods")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			highPriorityRunning := 0
			lowPriorityRunning := 0
			for i := range pods.Items {
				pod := &pods.Items[i]
				if pod.Status.Phase == v1.PodRunning {
					if strings.Contains(pod.Name, highPriorityApp) {
						highPriorityRunning++
					} else if strings.Contains(pod.Name, lowPriorityApp) {
						lowPriorityRunning++
					}
				}
			}
			return highPriorityRunning == 4 && lowPriorityRunning <= 3
		}, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{ns, dev})

		By("Checking YuniKorn's health after test")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(Equal(""), checks)

		By("Tearing down namespaces: " + ns + ", " + dev)
		err = kClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())

		err = kClient.TearDownNamespace(dev)
		Ω(err).NotTo(HaveOccurred())
	})
})

// Helper function to create a test deployment
func createTestDeployment(appName, namespace string, replicas int32, resources map[string]string) *appsv1.Deployment {
	gracePeriod := int64(5)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: namespace,
			Labels: map[string]string{
				"app": appName,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": appName,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":           appName,
						"applicationId": appName + "-" + common.RandSeq(5),
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "test-container",
							Image: "registry.k8s.io/pause:3.7",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse(resources["cpu"]),
									"memory": resource.MustParse(resources["memory"]),
								},
							},
						},
					},
					RestartPolicy:                 v1.RestartPolicyAlways,
					TerminationGracePeriodSeconds: &gracePeriod,
				},
			},
		},
	}
}
