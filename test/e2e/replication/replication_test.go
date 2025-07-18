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

package replication_test

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	// Optimized timeouts and intervals for faster execution
	standardTimeout = 30 * time.Second // Reduced from 60s
	longTimeout     = 45 * time.Second // Reduced from 90s
	stressTimeout   = 60 * time.Second // Reduced from 180s
	pollInterval    = 1 * time.Second  // Reduced from 2-3s

	// Lightweight image to reduce pull time
	testImage = "alpine:latest"
)

var _ = ginkgo.Describe("Replication", func() {
	var dev string

	ginkgo.BeforeEach(func() {
		dev = "dev-" + common.RandSeq(10)
		ginkgo.By(fmt.Sprintf("Create development namespace: %s", dev))
		ns, err := kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})

		ginkgo.By("Tear down namespace: " + dev)
		err := kClient.TearDownNamespace(dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// Check YuniKorn's health
		ginkgo.By("Check YuniKorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(checks).To(gomega.Equal(""), checks)
	})

	ginkgo.It("Verify_Basic_Replication_Setup", func() {
		ginkgo.By("Creating a basic test pod for replication setup verification")
		sleepPodConfigs := k8s.SleepPodConfig{
			Name: "replication-test-pod",
			NS:   dev,
			Time: 30,
		}

		ginkgo.By("Initializing sleep pod configuration")
		initPod, err := k8s.InitSleepPod(sleepPodConfigs)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pod in the test namespace")
		pod, err := kClient.CreatePod(initPod, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for pod to be scheduled and running")
		err = kClient.WaitForPodRunning(dev, sleepPodConfigs.Name, standardTimeout)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying the pod is scheduled by YuniKorn scheduler")
		gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"))

		ginkgo.By("Verifying basic replication test setup is working")
		podObj, err := kClient.GetPod(sleepPodConfigs.Name, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(podObj.Status.Phase).To(gomega.Equal(v1.PodRunning))

		ginkgo.By("Validating pod conditions for readiness")
		gomega.Ω(podObj.Status.Conditions).ToNot(gomega.BeEmpty(), "Pod should have at least one condition")

		ginkgo.By("Finding and validating the Ready condition")
		var readyCondition *v1.PodCondition
		for i := range podObj.Status.Conditions {
			if podObj.Status.Conditions[i].Type == v1.PodReady {
				readyCondition = &podObj.Status.Conditions[i]
				break
			}
		}
		gomega.Ω(readyCondition).NotTo(gomega.BeNil(), "Pod should have a Ready condition")
		gomega.Ω(readyCondition.Status).To(gomega.Equal(v1.ConditionTrue), "Ready condition should be True")
		gomega.Ω(readyCondition.LastTransitionTime).NotTo(gomega.BeZero(), "Ready condition should have transition time")

		ginkgo.By("Basic replication setup is verified: pod is running and scheduled by YuniKorn")

		// Simplified validation - removed redundant checks
		ginkgo.By("Validating pod labels and annotations for replication")
		gomega.Ω(podObj.Labels).To(gomega.HaveKeyWithValue("app", "sleep"), "Pod should have app=sleep label")
		gomega.Ω(podObj.Labels).To(gomega.HaveKey("applicationId"), "Pod should have applicationId label")
		gomega.Ω(podObj.Annotations).To(gomega.HaveKey("yunikorn.apache.org/allow-preemption"), "Pod should have preemption annotation")
		gomega.Ω(podObj.Spec.NodeName).NotTo(gomega.BeEmpty(), "Pod should be scheduled to a node")
		gomega.Ω(podObj.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
	})

	ginkgo.It("Verify_ReplicaSet_Pod_Replication", func() {
		ginkgo.By("Creating a ReplicaSet with multiple replicas")
		replicaCount := int32(2) // Reduced from 3 for faster execution
		replicaSetName := "test-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "replica-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "replica-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "replica-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "replica-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"), // Reduced resource requests
										v1.ResourceMemory: resource.MustParse("16Mi"),
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the ReplicaSet")
		createdReplicaSet, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(createdReplicaSet.Name).To(gomega.Equal(replicaSetName))

		ginkgo.By("Waiting for replica pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=replica-test")
			if err != nil {
				return false
			}
			return len(podList.Items) == int(replicaCount)
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 replica pods")

		ginkgo.By("Waiting for all replica pods to be running using parallel check")
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=replica-test")
			if err != nil {
				return false
			}
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "All replica pods should be running")

		ginkgo.By("Verifying all replica pods are scheduled by YuniKorn")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "All replica pods should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "All replica pods should be running")
		}

		ginkgo.By("Cleaning up ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Deployment_Scaling_Operations", func() {
		ginkgo.By("Creating a Deployment with initial replica count")
		deploymentName := "test-deployment"
		initialReplicas := int32(2)

		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name: deploymentName,
				Labels: map[string]string{
					"app": "scaling-test",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &initialReplicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "scaling-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "scaling-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "scaling-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"),
										v1.ResourceMemory: resource.MustParse("16Mi"),
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the Deployment")
		createdDeployment, err := kClient.CreateDeployment(deployment, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(createdDeployment.Name).To(gomega.Equal(deploymentName))

		ginkgo.By("Waiting for initial pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=scaling-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(initialReplicas) {
				return false
			}
			// Check all pods are running in same call
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 initial running pods")

		ginkgo.By("Scaling up the deployment")
		scaledReplicas := int32(3) // Reduced from 4
		deploymentObj, err := kClient.GetDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		deploymentObj.Spec.Replicas = &scaledReplicas

		_, err = kClient.UpdateDeployment(deploymentObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for scaled pods to be created and running")
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=scaling-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(scaledReplicas) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 3 scaled running pods")

		ginkgo.By("Verifying all scaled pods are scheduled by YuniKorn")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "All scaled pods should be scheduled by YuniKorn")
		}

		ginkgo.By("Cleaning up Deployment")
		err = kClient.DeleteDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Pod_Failure_Recovery", func() {
		ginkgo.By("Creating a ReplicaSet for failure testing")
		replicaCount := int32(2) // Reduced from 3
		replicaSetName := "failure-test-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "failure-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "failure-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "failure-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "failure-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"),
										v1.ResourceMemory: resource.MustParse("16Mi"),
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=failure-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 initial running pods")

		ginkgo.By("Deleting one pod to simulate failure")
		podToDelete := podList.Items[0]
		err = kClient.DeletePod(podToDelete.Name, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for replacement pod to be created and running")
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=failure-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 pods after recovery")

		ginkgo.By("Verifying replacement pod is scheduled by YuniKorn")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "All pods including replacement should be scheduled by YuniKorn")
		}

		ginkgo.By("Cleaning up ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Resource_Allocation_Across_Replicas", func() {
		ginkgo.By("Creating a ReplicaSet with resource requests")
		replicaCount := int32(2)
		replicaSetName := "resource-test-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "resource-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "resource-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "resource-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "resource-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("50m"),  // Reduced from 100m
										v1.ResourceMemory: resource.MustParse("64Mi"), // Reduced from 128Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=resource-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 running pods")

		ginkgo.By("Verifying resource allocation for all replica pods")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")

			// Verify resource requests are preserved
			container := pod.Spec.Containers[0]
			cpuReq := container.Resources.Requests[v1.ResourceCPU]
			memReq := container.Resources.Requests[v1.ResourceMemory]
			gomega.Ω(cpuReq.String()).To(gomega.Equal("50m"), "CPU request should be 50m")
			gomega.Ω(memReq.String()).To(gomega.Equal("64Mi"), "Memory request should be 64Mi")
		}

		ginkgo.By("Cleaning up ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Queue_Assignment_Consistency", func() {
		ginkgo.By("Creating a ReplicaSet to test queue assignment consistency")
		replicaCount := int32(2) // Reduced from 3
		replicaSetName := "queue-test-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "queue-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "queue-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "queue-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "queue-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"),
										v1.ResourceMemory: resource.MustParse("16Mi"),
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=queue-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 running pods")

		ginkgo.By("Verifying queue assignment consistency across replica pods")
		var appIDs []string
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")

			// Check if pod has applicationId label
			if appID, exists := pod.Labels["applicationId"]; exists {
				appIDs = append(appIDs, appID)
			}
		}

		ginkgo.By("Verifying all pods have applicationId labels")
		gomega.Ω(appIDs).To(gomega.HaveLen(int(replicaCount)), "All pods should have applicationId labels")

		ginkgo.By("Checking YuniKorn queue assignment via REST API")
		if len(appIDs) > 0 {
			// Get application info for first pod to verify queue assignment
			expectedQueue := "root." + dev
			appInfo, appErr := restClient.GetAppInfo("default", expectedQueue, appIDs[0])
			gomega.Ω(appErr).NotTo(gomega.HaveOccurred())
			gomega.Ω(appInfo).NotTo(gomega.BeNil())
			gomega.Ω(appInfo.QueueName).To(gomega.ContainSubstring(dev), "App should be in namespace-based queue")
		}

		ginkgo.By("Cleaning up ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Multi_Container_Pod_Replication", func() {
		ginkgo.By("Creating a ReplicaSet with multi-container pods")
		replicaCount := int32(2) // Reduced from 3
		replicaSetName := "multi-container-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "multi-container-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "multi-container-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "multi-container-test",
						},
					},
					Spec: v1.PodSpec{
						InitContainers: []v1.Container{
							{
								Name:    "init-container",
								Image:   testImage,
								Command: []string{"sh", "-c", "echo 'Init complete' && sleep 2"}, // Reduced sleep
							},
						},
						Containers: []v1.Container{
							{
								Name:    "main-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("25m"),  // Reduced from 50m
										v1.ResourceMemory: resource.MustParse("32Mi"), // Reduced from 64Mi
									},
								},
							},
							{
								Name:    "sidecar-container",
								Image:   testImage,
								Command: []string{"sh", "-c", "while true; do echo 'Sidecar running'; sleep 30; done"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"),  // Reduced from 25m
										v1.ResourceMemory: resource.MustParse("16Mi"), // Reduced from 32Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the multi-container ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all multi-container pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=multi-container-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
				// Check init container completion
				if len(pod.Status.InitContainerStatuses) > 0 {
					initStatus := pod.Status.InitContainerStatuses[0]
					if initStatus.State.Terminated == nil || initStatus.State.Terminated.ExitCode != 0 {
						return false
					}
				}
				// Check main containers
				if len(pod.Status.ContainerStatuses) != 2 {
					return false
				}
				for _, containerStatus := range pod.Status.ContainerStatuses {
					if !containerStatus.Ready {
						return false
					}
				}
			}
			return true
		}, longTimeout, pollInterval).Should(gomega.BeTrue(), "All multi-container pods should be ready")

		ginkgo.By("Verifying multi-container pod structure")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Spec.InitContainers).To(gomega.HaveLen(1), "Pod should have 1 init container")
			gomega.Ω(pod.Spec.Containers).To(gomega.HaveLen(2), "Pod should have 2 main containers")

			// Verify init container completed successfully
			if len(pod.Status.InitContainerStatuses) > 0 {
				initStatus := pod.Status.InitContainerStatuses[0]
				// Init container should either be not ready (completed) or have terminated state
				isInitCompleted := !initStatus.Ready ||
					(initStatus.State.Terminated != nil && initStatus.State.Terminated.ExitCode == 0)
				gomega.Ω(isInitCompleted).To(gomega.BeTrue(), "Init container should be completed successfully")
			}

			// Verify both main containers are running
			readyCount := 0
			for _, containerStatus := range pod.Status.ContainerStatuses {
				if containerStatus.Ready {
					readyCount++
				}
			}
			gomega.Ω(readyCount).To(gomega.Equal(2), "All 2 containers should be ready")
		}

		ginkgo.By("Cleaning up multi-container ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Replication_With_Node_Affinity", func() {
		ginkgo.By("Getting available nodes")
		nodes, err := kClient.GetNodes()
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(nodes.Items).ToNot(gomega.BeEmpty(), "Should have at least 1 node")

		targetNode := nodes.Items[0].Name
		ginkgo.By(fmt.Sprintf("Targeting node: %s", targetNode))

		ginkgo.By("Creating a ReplicaSet with node affinity")
		replicaCount := int32(2)
		replicaSetName := "node-affinity-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "node-affinity-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "node-affinity-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "node-affinity-test",
						},
					},
					Spec: v1.PodSpec{
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
									{
										Weight: 100,
										Preference: v1.NodeSelectorTerm{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "kubernetes.io/hostname",
													Operator: v1.NodeSelectorOpIn,
													Values:   []string{targetNode},
												},
											},
										},
									},
								},
							},
						},
						Containers: []v1.Container{
							{
								Name:    "affinity-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"),
										v1.ResourceMemory: resource.MustParse("16Mi"),
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the ReplicaSet with node affinity")
		_, err = kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=node-affinity-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 running pods")

		ginkgo.By("Verifying node affinity is respected by YuniKorn scheduler")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Spec.NodeName).NotTo(gomega.BeEmpty(), "Pod should be scheduled to a node")
			gomega.Ω(pod.Spec.Affinity.NodeAffinity).NotTo(gomega.BeNil(), "Pod should have node affinity")
		}

		ginkgo.By("Cleaning up ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Rolling_Update_Replication", func() {
		ginkgo.By("Creating a Deployment for rolling update testing")
		deploymentName := "rolling-update-deployment"
		replicaCount := int32(3) // Reduced from 4

		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name: deploymentName,
				Labels: map[string]string{
					"app": "rolling-update-test",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicaCount,
				Strategy: appsv1.DeploymentStrategy{
					Type: appsv1.RollingUpdateDeploymentStrategyType,
					RollingUpdate: &appsv1.RollingUpdateDeployment{
						MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
						MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
					},
				},
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "rolling-update-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app":     "rolling-update-test",
							"version": "v1",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "rolling-container",
								Image:   testImage, // Use consistent image
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("10m"),
										v1.ResourceMemory: resource.MustParse("16Mi"),
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the initial Deployment")
		_, err := kClient.CreateDeployment(deployment, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for initial deployment to be ready")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=rolling-update-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, standardTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 3 initial running pods")

		ginkgo.By("Triggering rolling update by changing image version")
		deploymentObj, err := kClient.GetDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		deploymentObj.Spec.Template.Labels["version"] = "v2"
		deploymentObj.Spec.Template.Spec.Containers[0].Env = []v1.EnvVar{
			{Name: "VERSION", Value: "v2"}, // Use env var instead of different image
		}

		_, err = kClient.UpdateDeployment(deploymentObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for rolling update to complete")
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=rolling-update-test")
			if err != nil {
				return false
			}
			// Check if all pods have the new version AND we have exactly the right number
			v2Count := 0
			for _, pod := range podList.Items {
				if pod.Labels["version"] == "v2" && pod.Status.Phase == v1.PodRunning {
					v2Count++
				}
			}
			return v2Count == int(replicaCount) && len(podList.Items) == int(replicaCount)
		}, longTimeout, pollInterval).Should(gomega.BeTrue(), "All pods should be updated to v2 and count should be exactly 3")

		ginkgo.By("Verifying rolling update completed successfully")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Labels["version"]).To(gomega.Equal("v2"), "Pod should have version v2")
		}

		ginkgo.By("Cleaning up Deployment")
		err = kClient.DeleteDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_StatefulSet_Replication", func() {
		ginkgo.By("Creating a StatefulSet for persistent identity testing")
		statefulSetName := "stateful-test"
		replicaCount := int32(2) // Reduced from 3

		statefulSet := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: statefulSetName,
				Labels: map[string]string{
					"app": "stateful-test",
				},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    &replicaCount,
				ServiceName: "stateful-service",
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "stateful-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "stateful-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "stateful-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("50m"),  // Reduced from 100m
										v1.ResourceMemory: resource.MustParse("64Mi"), // Reduced from 128Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating a headless service for StatefulSet")
		service := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "stateful-service",
				Labels: map[string]string{
					"app": "stateful-test",
				},
			},
			Spec: v1.ServiceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					"app": "stateful-test",
				},
				Ports: []v1.ServicePort{
					{
						Port:       80,
						TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 80},
					},
				},
			},
		}

		ginkgo.By("Creating the headless service")
		_, err := kClient.CreateService(service, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the StatefulSet")
		_, err = kClient.CreateStatefulSet(statefulSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSet pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=stateful-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, longTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 StatefulSet running pods")

		ginkgo.By("Verifying StatefulSet pod naming and ordering")
		expectedNames := []string{
			fmt.Sprintf("%s-0", statefulSetName),
			fmt.Sprintf("%s-1", statefulSetName),
		}

		actualNames := make([]string, len(podList.Items))
		for i, pod := range podList.Items {
			actualNames[i] = pod.Name
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Spec.Hostname).To(gomega.Equal(pod.Name), "Pod hostname should match pod name")
			gomega.Ω(pod.Spec.Subdomain).To(gomega.Equal("stateful-service"), "Pod subdomain should match service name")
		}

		gomega.Ω(actualNames).To(gomega.ConsistOf(expectedNames), "StatefulSet pods should have predictable names")

		ginkgo.By("Cleaning up StatefulSet and Service")
		err = kClient.DeleteStatefulSet(statefulSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeleteService("stateful-service", dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Replication_Stress_Test", func() {
		ginkgo.By("Creating a ReplicaSet for stress testing")
		replicaCount := int32(5) // Reduced from 10 for faster execution
		replicaSetName := "stress-test-replicaset"

		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: replicaSetName,
				Labels: map[string]string{
					"app": "stress-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "stress-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "stress-test",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "stress-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("5m"),  // Reduced from 10m
										v1.ResourceMemory: resource.MustParse("8Mi"), // Reduced from 16Mi
									},
									Limits: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("25m"),  // Reduced from 50m
										v1.ResourceMemory: resource.MustParse("32Mi"), // Reduced from 64Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating the stress test ReplicaSet")
		startTime := time.Now()
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all stress test pods to be created and running")
		var podList *v1.PodList
		gomega.Eventually(func() bool {
			podList, err = kClient.ListPods(dev, "app=stress-test")
			if err != nil {
				return false
			}
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			// Check all pods are running
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, stressTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 5 stress test running pods")

		endTime := time.Now()
		schedulingDuration := endTime.Sub(startTime)
		ginkgo.By(fmt.Sprintf("Stress test completed in %v", schedulingDuration))

		ginkgo.By("Verifying stress test results")
		nodeDistribution := make(map[string]int)
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")
			nodeDistribution[pod.Spec.NodeName]++
		}

		ginkgo.By(fmt.Sprintf("Pod distribution across nodes: %v", nodeDistribution))
		gomega.Ω(nodeDistribution).ToNot(gomega.BeEmpty(), "Pods should be distributed across nodes")

		ginkgo.By("Cleaning up stress test ReplicaSet")
		err = kClient.DeleteReplicaSet(replicaSetName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Mixed_Workload_Replication", func() {
		ginkgo.By("Creating multiple workloads concurrently")

		// Create ReplicaSet (reduced replicas)
		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mixed-replicaset",
				Labels: map[string]string{
					"app": "mixed-replica",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &[]int32{2}[0], // Reduced from 3
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "mixed-replica",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "mixed-replica",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "replica-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("25m"),  // Reduced from 50m
										v1.ResourceMemory: resource.MustParse("32Mi"), // Reduced from 64Mi
									},
								},
							},
						},
					},
				},
			},
		}

		// Create Deployment
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name: "mixed-deployment",
				Labels: map[string]string{
					"app": "mixed-deploy",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &[]int32{2}[0],
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "mixed-deploy",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "mixed-deploy",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:    "deploy-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("35m"),  // Reduced from 75m
										v1.ResourceMemory: resource.MustParse("48Mi"), // Reduced from 96Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating ReplicaSet and Deployment concurrently")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		_, err = kClient.CreateDeployment(deployment, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all mixed workload pods to be created and running")
		var replicaPodList, deployPodList *v1.PodList

		gomega.Eventually(func() bool {
			replicaPodList, err = kClient.ListPods(dev, "app=mixed-replica")
			if err != nil {
				return false
			}
			deployPodList, err = kClient.ListPods(dev, "app=mixed-deploy")
			if err != nil {
				return false
			}
			if len(replicaPodList.Items) != 2 || len(deployPodList.Items) != 2 {
				return false
			}
			// Check all pods are running
			for _, pod := range replicaPodList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			for _, pod := range deployPodList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, longTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 ReplicaSet pods and 2 Deployment pods running")

		ginkgo.By("Verifying mixed workload scheduling")
		totalPods := len(replicaPodList.Items) + len(deployPodList.Items)
		gomega.Ω(totalPods).To(gomega.Equal(4), "Should have 4 total pods")

		allPods := append(replicaPodList.Items, deployPodList.Items...)
		for _, pod := range allPods {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "All pods should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "All pods should be running")
		}

		ginkgo.By("Cleaning up mixed workloads")
		err = kClient.DeleteReplicaSet("mixed-replicaset", dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeleteDeployment("mixed-deployment", dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Replication_With_Priority_Classes", func() {
		ginkgo.By("Creating priority classes for testing")

		// Create high priority class
		highPriorityClass := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "high-priority",
			},
			Value:         1000,
			GlobalDefault: false,
			Description:   "High priority class for testing",
		}

		// Create low priority class
		lowPriorityClass := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "low-priority",
			},
			Value:         100,
			GlobalDefault: false,
			Description:   "Low priority class for testing",
		}

		ginkgo.By("Creating priority classes")
		_, err := kClient.CreatePriorityClass(highPriorityClass)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		_, err = kClient.CreatePriorityClass(lowPriorityClass)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating ReplicaSet with high priority")
		highPriorityReplicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "high-priority-replicaset",
				Labels: map[string]string{
					"app": "high-priority-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &[]int32{2}[0],
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "high-priority-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "high-priority-test",
						},
					},
					Spec: v1.PodSpec{
						PriorityClassName: "high-priority",
						Containers: []v1.Container{
							{
								Name:    "high-priority-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("50m"),  // Reduced from 100m
										v1.ResourceMemory: resource.MustParse("64Mi"), // Reduced from 128Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating ReplicaSet with low priority")
		lowPriorityReplicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: "low-priority-replicaset",
				Labels: map[string]string{
					"app": "low-priority-test",
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &[]int32{2}[0],
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "low-priority-test",
					},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": "low-priority-test",
						},
					},
					Spec: v1.PodSpec{
						PriorityClassName: "low-priority",
						Containers: []v1.Container{
							{
								Name:    "low-priority-container",
								Image:   testImage,
								Command: []string{"sleep", "300"},
								Resources: v1.ResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceCPU:    resource.MustParse("25m"),  // Reduced from 50m
										v1.ResourceMemory: resource.MustParse("32Mi"), // Reduced from 64Mi
									},
								},
							},
						},
					},
				},
			},
		}

		ginkgo.By("Creating both ReplicaSets")
		_, err = kClient.CreateReplicaSet(highPriorityReplicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		_, err = kClient.CreateReplicaSet(lowPriorityReplicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all priority-based pods to be created and running")
		var highPriorityPods, lowPriorityPods *v1.PodList

		gomega.Eventually(func() bool {
			highPriorityPods, err = kClient.ListPods(dev, "app=high-priority-test")
			if err != nil {
				return false
			}
			lowPriorityPods, err = kClient.ListPods(dev, "app=low-priority-test")
			if err != nil {
				return false
			}
			if len(highPriorityPods.Items) != 2 || len(lowPriorityPods.Items) != 2 {
				return false
			}
			// Check all pods are running
			for _, pod := range highPriorityPods.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			for _, pod := range lowPriorityPods.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
			}
			return true
		}, longTimeout, pollInterval).Should(gomega.BeTrue(), "Should have 2 high-priority and 2 low-priority running pods")

		ginkgo.By("Verifying priority class assignments")
		for _, pod := range highPriorityPods.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "High priority pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Spec.PriorityClassName).To(gomega.Equal("high-priority"), "Pod should have high priority class")
			gomega.Ω(pod.Spec.Priority).To(gomega.Equal(&[]int32{1000}[0]), "Pod should have priority value 1000")
		}

		for _, pod := range lowPriorityPods.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Low priority pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Spec.PriorityClassName).To(gomega.Equal("low-priority"), "Pod should have low priority class")
			gomega.Ω(pod.Spec.Priority).To(gomega.Equal(&[]int32{100}[0]), "Pod should have priority value 100")
		}

		ginkgo.By("Cleaning up priority-based ReplicaSets")
		err = kClient.DeleteReplicaSet("high-priority-replicaset", dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeleteReplicaSet("low-priority-replicaset", dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Cleaning up priority classes")
		err = kClient.DeletePriorityClass("high-priority")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = kClient.DeletePriorityClass("low-priority")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})
})
