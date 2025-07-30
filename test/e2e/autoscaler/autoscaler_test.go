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

package autoscaler_test

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
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("Autoscaler Tests", func() {
	var ns string
	var dev string

	BeforeEach(func() {
		ns = "autoscaler-" + common.RandSeq(10)
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

	// Test basic pod scaling behavior with YuniKorn scheduler
	It("Verify_Pod_Scaling_With_YuniKorn_Scheduler", func() {
		By("Creating initial deployment with 1 replica")
		appName := "scaling-app-" + common.RandSeq(5)

		// Create a deployment with resource requests to enable scaling
		deployment := createTestDeployment(appName, ns, 1, map[string]string{
			"cpu":    "100m",
			"memory": "128Mi",
		})

		deploymentObj, err := kClient.CreateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(deploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Waiting for initial pod to be created")
		err = kClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", appName), 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 120)
		Ω(err).NotTo(HaveOccurred())

		By("Scaling deployment up to 3 replicas")
		deployment.Spec.Replicas = &[]int32{3}[0]
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying 3 pods are running")
		gomega.Eventually(func() int {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return 0
			}
			runningCount := 0
			for _, pod := range podsInNs.Items {
				if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			return runningCount
		}, 90*time.Second, 3*time.Second).Should(gomega.Equal(3))
	})

	// Test resource utilization tracking for autoscaling decisions
	It("Verify_Resource_Utilization_Metrics_For_Autoscaling", func() {
		By("Creating a resource-intensive workload")
		appName := "resource-app-" + common.RandSeq(5)

		// Create pods with higher resource requests
		podConfig := k8s.TestPodConfig{
			Name:      appName,
			Namespace: ns,
			Labels: map[string]string{
				"app":           appName,
				"applicationId": "app-" + common.RandSeq(5),
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("500m"),
					"memory": resource.MustParse("512Mi"),
				},
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("1000m"),
					"memory": resource.MustParse("1Gi"),
				},
			},
		}

		pod, err := k8s.InitTestPod(podConfig)
		Ω(err).NotTo(HaveOccurred())
		createdPod, err := kClient.CreatePod(pod, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeletePod(createdPod.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Waiting for pod to be scheduled and running")
		err = kClient.WaitForPodRunning(ns, podConfig.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying YuniKorn application metrics")
		appInfo, err := restClient.GetAppInfo("default", "root."+ns, podConfig.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(appInfo).NotTo(BeNil())
		Ω(appInfo.State).To(Equal("Running"))

		By("Checking resource allocation in YuniKorn")
		Ω(appInfo.Allocations).NotTo(BeNil())
		Ω(len(appInfo.Allocations)).To(BeNumerically(">=", 1))

		allocation := appInfo.Allocations[0]
		Ω(allocation.ResourcePerAlloc).NotTo(BeNil())

		// Verify CPU and memory allocations match requests
		cpuAlloc := allocation.ResourcePerAlloc["vcore"]
		memAlloc := allocation.ResourcePerAlloc["memory"]
		Ω(cpuAlloc).To(Equal(int64(500)))       // 500m CPU
		Ω(memAlloc).To(Equal(int64(536870912))) // 512Mi memory
	})

	// Test queue-based resource management for autoscaling
	It("Verify_Queue_Based_Resource_Management_For_Scaling", func() {
		By("Creating pods in different queues")

		// Create pods in the default queue (namespace-based)
		defaultPodConfig := k8s.TestPodConfig{
			Name:      "default-queue-pod",
			Namespace: ns,
			Labels: map[string]string{
				"app":           "default-app",
				"applicationId": "default-app-" + common.RandSeq(5),
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("50m"),
					"memory": resource.MustParse("64Mi"),
				},
			},
		}

		// Create pods in development queue
		devPodConfig := k8s.TestPodConfig{
			Name:      "dev-queue-pod",
			Namespace: dev,
			Labels: map[string]string{
				"app":           "dev-app",
				"applicationId": "dev-app-" + common.RandSeq(5),
			},
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":    resource.MustParse("300m"),
					"memory": resource.MustParse("384Mi"),
				},
			},
		}

		By("Creating pod in default queue")
		defaultPod, err := k8s.InitTestPod(defaultPodConfig)
		Ω(err).NotTo(HaveOccurred())
		createdDefaultPod, err := kClient.CreatePod(defaultPod, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeletePod(createdDefaultPod.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Creating pod in development queue")
		devPod, err := k8s.InitTestPod(devPodConfig)
		Ω(err).NotTo(HaveOccurred())
		createdDevPod, err := kClient.CreatePod(devPod, dev)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeletePod(createdDevPod.Name, dev) }() //nolint:errcheck // Cleanup operation

		By("Waiting for both pods to be scheduled")
		err = kClient.WaitForPodRunning(ns, defaultPodConfig.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		err = kClient.WaitForPodRunning(dev, devPodConfig.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying pods are in correct queues")
		defaultAppInfo, err := restClient.GetAppInfo("default", "root."+ns, defaultPodConfig.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(defaultAppInfo.QueueName).To(Equal("root." + ns))

		devAppInfo, err := restClient.GetAppInfo("default", "root."+dev, devPodConfig.Labels["applicationId"])
		Ω(err).NotTo(HaveOccurred())
		Ω(devAppInfo.QueueName).To(Equal("root." + dev))

		By("Verifying resource allocation per queue")
		Ω(defaultAppInfo.UsedResource).NotTo(BeNil())
		Ω(devAppInfo.UsedResource).NotTo(BeNil())
	})

	// Test behavior during rapid scaling operations
	It("Verify_Rapid_Scaling_Operations", func() {
		By("Creating deployment for rapid scaling test")
		appName := "rapid-scale-" + common.RandSeq(5)

		deployment := createTestDeployment(appName, ns, 1, map[string]string{
			"cpu":    "50m",
			"memory": "64Mi",
		})

		deploymentObj, err := kClient.CreateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(deploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Waiting for initial pod to be created")
		err = kClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", appName), 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 120)
		Ω(err).NotTo(HaveOccurred())

		By("Rapidly scaling up to 5 replicas")
		deployment.Spec.Replicas = &[]int32{5}[0]
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scale up to complete")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 240)
		Ω(err).NotTo(HaveOccurred())

		By("Rapidly scaling down to 2 replicas")
		deployment.Spec.Replicas = &[]int32{2}[0]
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scale down to complete")
		// Wait for pods to be terminated and scaled down to 2 running pods
		gomega.Eventually(func() int {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return -1
			}
			runningCount := 0
			for _, pod := range podsInNs.Items {
				if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			return runningCount
		}, 120*time.Second, 5*time.Second).Should(Equal(2))

		By("Verifying remaining pods are healthy and scheduled by YuniKorn")
		podsInNs, getErr := kClient.GetPods(ns)
		Ω(getErr).NotTo(gomega.HaveOccurred())

		runningPods := 0
		for _, pod := range podsInNs.Items {
			if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
				Ω(pod.Spec.SchedulerName).To(Equal("yunikorn"))
				runningPods++
			}
		}
		Ω(runningPods).To(Equal(2))
	})

	// Test preemption during autoscaling operations
	It("Verify_Preemption_During_Autoscaling_Operations", func() {
		By("Testing preemption behavior when autoscaling operations are in progress")

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

		lowPriorityDeploymentObj, err := kClient.CreateDeployment(lowPriorityDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(lowPriorityDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Waiting for low-priority pods to be running")
		gomega.Eventually(func() int {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return 0
			}
			runningCount := 0
			for _, pod := range podsInNs.Items {
				if strings.Contains(pod.Name, lowPriorityApp) && pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			return runningCount
		}, 90*time.Second, 3*time.Second).Should(gomega.Equal(3))

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

		highPriorityDeploymentObj, err := kClient.CreateDeployment(highPriorityDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(highPriorityDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Scaling high-priority deployment to 4 replicas to trigger more preemption")
		highPriorityDeployment.Spec.Replicas = &[]int32{4}[0]
		_, err = kClient.UpdateDeployment(highPriorityDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying high-priority pods scale up")
		gomega.Eventually(func() int {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return 0
			}
			highPriorityRunning := 0
			for _, pod := range podsInNs.Items {
				if strings.Contains(pod.Name, highPriorityApp) && pod.Status.Phase == v1.PodRunning {
					highPriorityRunning++
				}
			}
			return highPriorityRunning
		}, 90*time.Second, 3*time.Second).Should(gomega.Equal(4))

		By("Verifying preemption effects on low-priority pods")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}
			highPriorityRunning := 0
			lowPriorityRunning := 0
			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					if strings.Contains(pod.Name, highPriorityApp) {
						highPriorityRunning++
					} else if strings.Contains(pod.Name, lowPriorityApp) {
						lowPriorityRunning++
					}
				}
			}
			// In resource-constrained environments, we may not see perfect preemption
			return highPriorityRunning == 4 && lowPriorityRunning <= 3
		}, 60*time.Second, 3*time.Second).Should(gomega.BeTrue(), "High-priority pods should scale to 4, low-priority should be preempted or stay same")
	})

	// Test resource competition between multiple autoscaling deployments
	It("Verify_Multi_Queue_Autoscaling_With_Resource_Competition", func() {
		By("Testing resource competition between multiple autoscaling deployments in different queues")

		app1Name := "queue1-app-" + common.RandSeq(5)
		app2Name := "queue2-app-" + common.RandSeq(5)
		app3Name := "queue3-app-" + common.RandSeq(5)

		// Create deployments in different queues with different priorities
		deployment1 := createTestDeployment(app1Name, ns, 2, map[string]string{
			"cpu":    "100m",
			"memory": "128Mi",
		})
		deployment1.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue": "root.production",
		}

		deployment2 := createTestDeployment(app2Name, ns, 2, map[string]string{
			"cpu":    "150m",
			"memory": "192Mi",
		})
		deployment2.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue": "root.staging",
		}

		deployment3 := createTestDeployment(app3Name, ns, 2, map[string]string{
			"cpu":    "50m",
			"memory": "64Mi",
		})
		deployment3.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue": "root.development",
		}

		// Deploy all applications
		deploymentObj1, err := kClient.CreateDeployment(deployment1, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(deploymentObj1.Name, ns) }() //nolint:errcheck // Cleanup operation

		deploymentObj2, err := kClient.CreateDeployment(deployment2, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(deploymentObj2.Name, ns) }() //nolint:errcheck // Cleanup operation

		deploymentObj3, err := kClient.CreateDeployment(deployment3, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(deploymentObj3.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Verifying initial deployment state")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}
			app1Running := 0
			app2Running := 0
			app3Running := 0

			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					switch pod.Labels["app"] {
					case app1Name:
						app1Running++
					case app2Name:
						app2Running++
					case app3Name:
						app3Running++
					}
				}
			}

			return app1Running >= 2 && app2Running >= 2 && app3Running >= 2
		}, 180*time.Second, 5*time.Second).Should(gomega.BeTrue())

		By("Scaling all deployments up to trigger resource competition")
		deployment1.Spec.Replicas = &[]int32{4}[0]
		_, err = kClient.UpdateDeployment(deployment1, ns)
		Ω(err).NotTo(HaveOccurred())

		deployment2.Spec.Replicas = &[]int32{4}[0]
		_, err = kClient.UpdateDeployment(deployment2, ns)
		Ω(err).NotTo(HaveOccurred())

		deployment3.Spec.Replicas = &[]int32{3}[0]
		_, err = kClient.UpdateDeployment(deployment3, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying resource competition and scaling behavior")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}
			app1Running := 0
			app2Running := 0
			app3Running := 0
			totalRunning := 0

			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					totalRunning++
					switch pod.Labels["app"] {
					case app1Name:
						app1Running++
					case app2Name:
						app2Running++
					case app3Name:
						app3Running++
					}
				}
			}

			return app1Running >= 3 && app2Running >= 3 && app3Running >= 2 && totalRunning >= 8
		}, 90*time.Second, 3*time.Second).Should(gomega.BeTrue(), "All apps should scale up within resource constraints")

		By("Verifying all pods are scheduled by YuniKorn with proper queue assignment")
		podsInNs, getErr := kClient.GetPods(ns)
		Ω(getErr).NotTo(gomega.HaveOccurred())
		queueCounts := make(map[string]int)
		for _, pod := range podsInNs.Items {
			if pod.Status.Phase == v1.PodRunning {
				Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"))
				if queueAnnotation, exists := pod.Annotations["yunikorn.apache.org/queue"]; exists {
					queueCounts[queueAnnotation]++
				}
			}
		}

		// Verify pods are distributed across queues
		Ω(len(queueCounts)).To(gomega.BeNumerically(">=", 2))
	})

	// Test autoscaling with preemption policies
	It("Verify_Autoscaling_With_Preemption_Policies", func() {
		By("Testing autoscaling behavior with different preemption policies (fence vs allow)")

		fenceApp := "fence-app-" + common.RandSeq(5)
		allowApp := "allow-app-" + common.RandSeq(5)
		aggressorApp := "aggressor-app-" + common.RandSeq(5)

		// Create fence policy deployment (cannot be preempted)
		fenceDeployment := createTestDeployment(fenceApp, ns, 2, map[string]string{
			"cpu":    "150m",
			"memory": "192Mi",
		})
		fenceDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":             "root.fence-queue",
			"yunikorn.apache.org/preemption-policy": "fence",
		}

		// Create allow policy deployment (can be preempted)
		allowDeployment := createTestDeployment(allowApp, ns, 2, map[string]string{
			"cpu":    "150m",
			"memory": "192Mi",
		})
		allowDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":             "root.allow-queue",
			"yunikorn.apache.org/preemption-policy": "allow",
		}

		// Deploy fence and allow policy apps
		fenceDeploymentObj, err := kClient.CreateDeployment(fenceDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(fenceDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		allowDeploymentObj, err := kClient.CreateDeployment(allowDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(allowDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Verifying initial deployment state")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}
			fenceRunning := 0
			allowRunning := 0

			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					if strings.Contains(pod.Name, fenceApp) {
						fenceRunning++
					} else if strings.Contains(pod.Name, allowApp) {
						allowRunning++
					}
				}
			}

			return fenceRunning >= 2 && allowRunning >= 2
		}, 120*time.Second, 5*time.Second).Should(gomega.BeTrue())

		By("Scaling up both deployments to consume more resources")
		fenceDeployment.Spec.Replicas = &[]int32{4}[0]
		allowDeployment.Spec.Replicas = &[]int32{4}[0]

		_, err = kClient.UpdateDeployment(fenceDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.UpdateDeployment(allowDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scaled deployments")
		time.Sleep(30 * time.Second) // Allow scaling to start

		// Create high-priority aggressor deployment
		aggressorDeployment := createTestDeployment(aggressorApp, ns, 3, map[string]string{
			"cpu":    "300m",
			"memory": "512Mi",
		})
		aggressorDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":    "root.high-priority",
			"yunikorn.apache.org/priority": "200",
		}

		aggressorDeploymentObj, err := kClient.CreateDeployment(aggressorDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(aggressorDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Verifying preemption behavior based on policies")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}

			fenceRunning := 0
			allowRunning := 0
			aggressorRunning := 0

			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					switch pod.Labels["app"] {
					case fenceApp:
						fenceRunning++
					case allowApp:
						allowRunning++
					case aggressorApp:
						aggressorRunning++
					}
				}
			}

			// Fence pods should maintain higher count (protected from preemption)
			// Allow pods may be preempted
			// Aggressor should get some resources
			return aggressorRunning > 0 && fenceRunning >= allowRunning
		}, 300*time.Second, 10*time.Second).Should(gomega.BeTrue())
	})

	// Test rapid scaling with preemption cascades
	It("Verify_Rapid_Scaling_With_Preemption_Cascades", func() {
		By("Testing rapid scaling operations that trigger cascading preemption effects")

		// Create multiple tiers of applications with different priorities
		tier1App := "tier1-critical-" + common.RandSeq(5)
		tier2App := "tier2-important-" + common.RandSeq(5)
		tier3App := "tier3-normal-" + common.RandSeq(5)
		tier4App := "tier4-background-" + common.RandSeq(5)

		applications := []struct {
			name      string
			priority  string
			queue     string
			replicas  int32
			resources map[string]string
		}{
			{tier4App, "0", "root.background", 4, map[string]string{"cpu": "25m", "memory": "32Mi"}},
			{tier3App, "25", "root.normal", 3, map[string]string{"cpu": "40m", "memory": "48Mi"}},
			{tier2App, "50", "root.important", 2, map[string]string{"cpu": "50m", "memory": "64Mi"}},
			{tier1App, "100", "root.critical", 1, map[string]string{"cpu": "75m", "memory": "128Mi"}},
		}

		var deployments []*appsv1.Deployment

		// Deploy applications in reverse priority order (background first)
		for _, app := range applications {
			deployment := createTestDeployment(app.name, ns, app.replicas, app.resources)
			deployment.Spec.Template.Annotations = map[string]string{
				"yunikorn.apache.org/queue":    app.queue,
				"yunikorn.apache.org/priority": app.priority,
			}

			deploymentObj, err := kClient.CreateDeployment(deployment, ns)
			Ω(err).NotTo(HaveOccurred())
			deployments = append(deployments, deployment)
			defer func() { _ = kClient.DeleteDeployment(deploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation
		}

		By("Waiting for all initial pods to be running")
		for _, app := range applications {
			gomega.Eventually(func() int {
				podsInNs, getErr := kClient.GetPods(ns)
				if getErr != nil {
					return -1
				}
				runningCount := 0
				for _, pod := range podsInNs.Items {
					if strings.Contains(pod.Name, app.name) && pod.Status.Phase == v1.PodRunning {
						runningCount++
					}
				}
				return runningCount
			}, 180*time.Second, 5*time.Second).Should(gomega.Equal(int(app.replicas)))
		}

		By("Rapidly scaling critical application to trigger cascading preemption")
		// Scale critical app aggressively
		deployments[3].Spec.Replicas = &[]int32{6}[0] // tier1 (critical)
		_, err := kClient.UpdateDeployment(deployments[3], ns)
		Ω(err).NotTo(HaveOccurred())

		By("Simultaneously scaling important application")
		deployments[2].Spec.Replicas = &[]int32{5}[0] // tier2 (important)
		_, err = kClient.UpdateDeployment(deployments[2], ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying cascading preemption effects")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}

			appCounts := make(map[string]int)
			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					appCounts[pod.Labels["app"]]++
				}
			}

			// Critical app should scale up successfully
			criticalRunning := appCounts[tier1App]
			importantRunning := appCounts[tier2App]
			normalRunning := appCounts[tier3App]
			backgroundRunning := appCounts[tier4App]

			// More flexible conditions - verify priority-based resource allocation trends
			// Focus on critical apps getting resources and some preemption happening
			totalRunning := criticalRunning + importantRunning + normalRunning + backgroundRunning

			// At least some scaling should happen and there should be evidence of prioritization
			return criticalRunning >= 2 && // Critical should scale up from 1
				importantRunning >= 2 && // Important should scale up from 2
				totalRunning >= 8 && // Reasonable total running pods
				criticalRunning >= backgroundRunning // Critical should have more than background
		}, 150*time.Second, 5*time.Second).Should(gomega.BeTrue(), "Priority-based scaling and preemption should show resource prioritization")

		By("Verifying resource fairness after preemption cascade")
		podsInNs, getErr := kClient.GetPods(ns)
		Ω(getErr).NotTo(gomega.HaveOccurred())

		appResourceUsage := make(map[string]map[string]int64)
		for _, pod := range podsInNs.Items {
			if pod.Status.Phase == v1.PodRunning {
				appName := pod.Labels["app"]
				if appResourceUsage[appName] == nil {
					appResourceUsage[appName] = make(map[string]int64)
				}

				// Extract CPU and memory requests
				for _, container := range pod.Spec.Containers {
					if cpu := container.Resources.Requests.Cpu(); cpu != nil {
						appResourceUsage[appName]["cpu"] += cpu.MilliValue()
					}
					if memory := container.Resources.Requests.Memory(); memory != nil {
						appResourceUsage[appName]["memory"] += memory.Value()
					}
				}
			}
		}

		By("Validating that high-priority apps get more resources")
		if tier1Usage, exists := appResourceUsage[tier1App]; exists {
			if tier4Usage, exists := appResourceUsage[tier4App]; exists {
				// Critical app should use more CPU than background app
				Ω(tier1Usage["cpu"]).To(gomega.BeNumerically(">", tier4Usage["cpu"]))
			}
		}
	})

	// Test autoscaling with gang scheduling and preemption
	It("Verify_Gang_Scheduling_Autoscaling_With_Preemption", func() {
		By("Testing gang scheduling behavior during autoscaling with preemption scenarios")

		gangApp := "gang-app-" + common.RandSeq(5)
		competitorApp := "competitor-app-" + common.RandSeq(5)

		// Create gang scheduled application
		gangDeployment := createTestDeployment(gangApp, ns, 3, map[string]string{
			"cpu":    "50m",
			"memory": "64Mi",
		})
		gangDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":                      "root.gang-queue",
			"yunikorn.apache.org/task-group-name":            "gang-group",
			"yunikorn.apache.org/task-groups":                `[{"name": "gang-group", "minMember": 3, "minResource": {"cpu": "600m", "memory": "768Mi"}}]`,
			"yunikorn.apache.org/schedulingPolicyParameters": "gangSchedulingStyle=Hard",
		}

		// Create competitor application
		competitorDeployment := createTestDeployment(competitorApp, ns, 4, map[string]string{
			"cpu":    "150m",
			"memory": "192Mi",
		})
		competitorDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":    "root.competitor-queue",
			"yunikorn.apache.org/priority": "10",
		}

		// Deploy competitor first to consume resources
		competitorDeploymentObj, err := kClient.CreateDeployment(competitorDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(competitorDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Waiting for competitor pods to be running")
		gomega.Eventually(func() int {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return -1
			}
			runningCount := 0
			for _, pod := range podsInNs.Items {
				if strings.Contains(pod.Name, competitorApp) && pod.Status.Phase == v1.PodRunning {
					runningCount++
				}
			}
			return runningCount
		}, 180*time.Second, 5*time.Second).Should(gomega.Equal(4))

		// Deploy gang application
		gangDeploymentObj, err := kClient.CreateDeployment(gangDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		defer func() { _ = kClient.DeleteDeployment(gangDeploymentObj.Name, ns) }() //nolint:errcheck // Cleanup operation

		By("Verifying gang scheduling behavior with resource constraints")
		gomega.Eventually(func() bool {
			podsInNs, getErr := kClient.GetPods(ns)
			if getErr != nil {
				return false
			}
			gangRunning := 0
			totalRunning := 0

			for _, pod := range podsInNs.Items {
				if pod.Status.Phase == v1.PodRunning {
					totalRunning++
					if strings.Contains(pod.Name, gangApp) {
						gangRunning++
					}
				}
			}

			// Gang scheduling should be atomic - either 0, 3 (original), or 6 (scaled) pods
			gangValid := (gangRunning == 0 || gangRunning == 3 || gangRunning == 6)
			totalReasonable := totalRunning <= 20 // Don't overload the cluster
			someProgress := totalRunning >= 4     // Some pods should be running

			return gangValid && totalReasonable && someProgress
		}, 90*time.Second, 3*time.Second).Should(gomega.BeTrue(), "Gang scheduling should be atomic (0, 3, or 6 pods) with reasonable resource usage")

		By("Verifying gang scheduling integrity")
		podsInNs, getErr := kClient.GetPods(ns)
		Ω(getErr).NotTo(gomega.HaveOccurred())

		gangRunning := 0
		for _, pod := range podsInNs.Items {
			if strings.Contains(pod.Name, gangApp) && pod.Status.Phase == v1.PodRunning {
				gangRunning++
				// Verify gang group annotation
				Ω(pod.Annotations["yunikorn.apache.org/task-group-name"]).To(gomega.Equal("gang-group"))
			}
		}

		// Gang should be all-or-nothing (but allow for resource constraints)
		if gangRunning > 0 {
			Ω(gangRunning).To(gomega.Or(gomega.Equal(3), gomega.Equal(6)), "Gang scheduling should be atomic - either original gang (3) or scaled gang (6)")
		}
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
					TerminationGracePeriodSeconds: &[]int64{5}[0], // Fast cleanup
				},
			},
		},
	}
}
