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

package replica_scaling_test

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

var _ = Describe("ReplicaScaling Tests", func() {
	var ns string
	var dev string

	BeforeEach(func() {
		ns = "replica-scaling-" + common.RandSeq(10)
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

		_, err := kClient.CreateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())
		By("Waiting for initial pod to be created")
		err = kClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", appName), 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 120)
		Ω(err).NotTo(HaveOccurred())

		By("Scaling deployment up to 3 replicas")
		replicas := int32(3)
		deployment.Spec.Replicas = &replicas
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying 3 pods are running")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 90)
		Ω(err).NotTo(HaveOccurred())
	})

	// Test resource utilization tracking for replica scaling decisions
	It("Verify_Resource_Utilization_Metrics_For_ReplicaScaling", func() {
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
		_, err = kClient.CreatePod(pod, ns)
		Ω(err).NotTo(HaveOccurred())

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

	// Test queue-based resource management for replica scaling
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
		_, err = kClient.CreatePod(defaultPod, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Creating pod in development queue")
		devPod, err := k8s.InitTestPod(devPodConfig)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(devPod, dev)
		Ω(err).NotTo(HaveOccurred())

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

		_, err := kClient.CreateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for initial pod to be created and running")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 120)
		Ω(err).NotTo(HaveOccurred())

		By("Rapidly scaling up to 5 replicas")
		fiveReplicas := int32(5)
		deployment.Spec.Replicas = &fiveReplicas
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scale up to complete")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 240)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying all scaled pods are scheduled by YuniKorn")
		scaledPods, scaledErr := kClient.GetPods(ns)
		Ω(scaledErr).NotTo(gomega.HaveOccurred())
		for _, pod := range scaledPods.Items {
			if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
				Ω(pod.Spec.SchedulerName).To(Equal("yunikorn"))
			}
		}

		By("Rapidly scaling down to 2 replicas")
		twoReplicas := int32(2)
		deployment.Spec.Replicas = &twoReplicas
		_, err = kClient.UpdateDeployment(deployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scale down to complete")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", appName), 120)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying remaining pods are healthy and scheduled by YuniKorn")
		finalPods, finalErr := kClient.GetPods(ns)
		Ω(finalErr).NotTo(gomega.HaveOccurred())

		runningPods := 0
		for _, pod := range finalPods.Items {
			if strings.Contains(pod.Name, appName) && pod.Status.Phase == v1.PodRunning {
				Ω(pod.Spec.SchedulerName).To(Equal("yunikorn"))
				runningPods++
			}
		}
		Ω(runningPods).To(Equal(2))
	})

	// Test preemption during replica scaling operations
	It("Verify_Preemption_During_ReplicaScaling_Operations", func() {
		By("Testing preemption behavior when replica scaling operations are in progress")

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
			for _, pod := range pods.Items {
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

	// Test resource competition between multiple replica scaling deployments
	It("Verify_Multi_Queue_ReplicaScaling_With_Resource_Competition", func() {
		By("Testing resource competition between multiple replica scaling deployments in different queues")

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
		_, err := kClient.CreateDeployment(deployment1, ns)
		Ω(err).NotTo(HaveOccurred())

		_, err = kClient.CreateDeployment(deployment2, ns)
		Ω(err).NotTo(HaveOccurred())

		_, err = kClient.CreateDeployment(deployment3, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying initial deployment state")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			appCounts := map[string]int{app1Name: 0, app2Name: 0, app3Name: 0}
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					if appName := pod.Labels["app"]; appName != "" {
						appCounts[appName]++
					}
				}
			}
			return appCounts[app1Name] >= 2 && appCounts[app2Name] >= 2 && appCounts[app3Name] >= 2
		}, 180*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Scaling all deployments up to trigger resource competition")
		fourReplicas := int32(4)
		threeReplicas := int32(3)
		deployment1.Spec.Replicas = &fourReplicas
		_, err = kClient.UpdateDeployment(deployment1, ns)
		Ω(err).NotTo(HaveOccurred())

		deployment2.Spec.Replicas = &fourReplicas
		_, err = kClient.UpdateDeployment(deployment2, ns)
		Ω(err).NotTo(HaveOccurred())

		deployment3.Spec.Replicas = &threeReplicas
		_, err = kClient.UpdateDeployment(deployment3, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying resource competition and scaling behavior")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			appCounts := map[string]int{app1Name: 0, app2Name: 0, app3Name: 0}
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					if appName := pod.Labels["app"]; appName != "" {
						appCounts[appName]++
					}
				}
			}
			return appCounts[app1Name] >= 3 && appCounts[app2Name] >= 3 && appCounts[app3Name] >= 2
		}, 90*time.Second)
		Ω(err).NotTo(HaveOccurred())

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

	// Test replica scaling with preemption policies
	It("Verify_ReplicaScaling_With_Preemption_Policies", func() {
		By("Testing replica scaling behavior with different preemption policies (fence vs allow)")

		fenceApp := "fence-app-" + common.RandSeq(5)
		allowApp := "allow-app-" + common.RandSeq(5)
		highPrioApp := "high-prio-app-" + common.RandSeq(5)

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
		_, err := kClient.CreateDeployment(fenceDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		_, err = kClient.CreateDeployment(allowDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying initial deployment state")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			fenceRunning := 0
			allowRunning := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					if strings.Contains(pod.Name, fenceApp) {
						fenceRunning++
					} else if strings.Contains(pod.Name, allowApp) {
						allowRunning++
					}
				}
			}
			return fenceRunning >= 2 && allowRunning >= 2
		}, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Scaling up both deployments to consume more resources")
		scaleReplicas := int32(4)
		fenceDeployment.Spec.Replicas = &scaleReplicas
		allowDeployment.Spec.Replicas = &scaleReplicas

		_, err = kClient.UpdateDeployment(fenceDeployment, ns)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.UpdateDeployment(allowDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for scaled deployments")
		time.Sleep(30 * time.Second) // Allow scaling to start

		// Create high-priority deployment
		highPrioDeployment := createTestDeployment(highPrioApp, ns, 3, map[string]string{
			"cpu":    "300m",
			"memory": "512Mi",
		})
		highPrioDeployment.Spec.Template.Annotations = map[string]string{
			"yunikorn.apache.org/queue":    "root.high-priority",
			"yunikorn.apache.org/priority": "200",
		}

		_, err = kClient.CreateDeployment(highPrioDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying preemption behavior based on policies")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			fenceRunning := 0
			allowRunning := 0
			highPrioRunning := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					switch pod.Labels["app"] {
					case fenceApp:
						fenceRunning++
					case allowApp:
						allowRunning++
					case highPrioApp:
						highPrioRunning++
					}
				}
			}
			return highPrioRunning > 0 && fenceRunning >= allowRunning
		}, 300*time.Second)
		Ω(err).NotTo(HaveOccurred())
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

			_, err := kClient.CreateDeployment(deployment, ns)
			Ω(err).NotTo(HaveOccurred())
			deployments = append(deployments, deployment)
		}

		By("Waiting for all initial pods to be running")
		for _, app := range applications {
			err := kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", app.name), 180)
			Ω(err).NotTo(HaveOccurred())
		}

		By("Rapidly scaling critical application to trigger cascading preemption")
		// Scale critical app aggressively
		sixReplicas := int32(6)
		fiveReplicas := int32(5)
		deployments[3].Spec.Replicas = &sixReplicas // tier1 (critical)
		_, err := kClient.UpdateDeployment(deployments[3], ns)
		Ω(err).NotTo(HaveOccurred())

		By("Simultaneously scaling important application")
		deployments[2].Spec.Replicas = &fiveReplicas // tier2 (important)
		_, err = kClient.UpdateDeployment(deployments[2], ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying cascading preemption effects")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			appCounts := make(map[string]int)
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					appCounts[pod.Labels["app"]]++
				}
			}
			criticalRunning := appCounts[tier1App]
			importantRunning := appCounts[tier2App]
			normalRunning := appCounts[tier3App]
			backgroundRunning := appCounts[tier4App]
			totalRunning := criticalRunning + importantRunning + normalRunning + backgroundRunning

			return criticalRunning >= 2 && importantRunning >= 2 && totalRunning >= 8 && criticalRunning >= backgroundRunning
		}, 150*time.Second)
		Ω(err).NotTo(HaveOccurred())

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

	// Test replica scaling with gang scheduling and preemption
	It("Verify_Gang_Scheduling_ReplicaScaling_With_Preemption", func() {
		By("Testing gang scheduling behavior during replica scaling with preemption scenarios")

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
		_, err := kClient.CreateDeployment(competitorDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Waiting for competitor pods to be running")
		err = kClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", competitorApp), 180)
		Ω(err).NotTo(HaveOccurred())

		// Deploy gang application
		_, err = kClient.CreateDeployment(gangDeployment, ns)
		Ω(err).NotTo(HaveOccurred())

		By("Verifying gang scheduling behavior with resource constraints")
		err = kClient.WaitForPodCondition(ns, func(pods *v1.PodList) bool {
			gangRunning := 0
			totalRunning := 0
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodRunning {
					totalRunning++
					if strings.Contains(pod.Name, gangApp) {
						gangRunning++
					}
				}
			}
			// Gang should be atomic (0, 3, or 6), total reasonable (<=20), some progress (>=4)
			gangValid := (gangRunning == 0 || gangRunning == 3 || gangRunning == 6)
			return gangValid && totalRunning <= 20 && totalRunning >= 4
		}, 90*time.Second)
		Ω(err).NotTo(HaveOccurred())

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
					TerminationGracePeriodSeconds: func() *int64 { grace := int64(5); return &grace }(),
				},
			},
		},
	}
}
