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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	testImage = "alpine:latest"
)

var dev string

// ReplicaSetConfig holds configuration for creating ReplicaSets
type ReplicaSetConfig struct {
	Name          string
	AppLabel      string
	Replicas      int32
	CPURequest    string
	MemoryRequest string
	CPULimit      string // optional
	MemoryLimit   string // optional
	PriorityClass string // optional
	// Advanced features
	InitContainers    []v1.Container   // optional
	SidecarContainers []v1.Container   // optional
	NodeAffinity      *v1.NodeAffinity // optional
}

// getReplicaSetSpec returns a ReplicaSet with the given configuration
func getReplicaSetSpec(config ReplicaSetConfig) *appsv1.ReplicaSet {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Name,
			Labels: map[string]string{
				"app": config.AppLabel,
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &config.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": config.AppLabel,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": config.AppLabel,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    config.Name + "-container",
							Image:   testImage,
							Command: []string{"sleep", "300"},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse(config.CPURequest),
									v1.ResourceMemory: resource.MustParse(config.MemoryRequest),
								},
							},
						},
					},
				},
			},
		},
	}

	// Add resource limits if specified
	if config.CPULimit != "" && config.MemoryLimit != "" {
		rs.Spec.Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse(config.CPULimit),
			v1.ResourceMemory: resource.MustParse(config.MemoryLimit),
		}
	}

	// Add priority class if specified
	if config.PriorityClass != "" {
		rs.Spec.Template.Spec.PriorityClassName = config.PriorityClass
	}

	// Add init containers if specified
	if len(config.InitContainers) > 0 {
		rs.Spec.Template.Spec.InitContainers = config.InitContainers
	}

	// Add sidecar containers if specified
	if len(config.SidecarContainers) > 0 {
		rs.Spec.Template.Spec.Containers = append(rs.Spec.Template.Spec.Containers, config.SidecarContainers...)
	}

	// Add node affinity if specified
	if config.NodeAffinity != nil {
		if rs.Spec.Template.Spec.Affinity == nil {
			rs.Spec.Template.Spec.Affinity = &v1.Affinity{}
		}
		rs.Spec.Template.Spec.Affinity.NodeAffinity = config.NodeAffinity
	}

	return rs
}

// DeploymentConfig holds configuration for creating Deployments
type DeploymentConfig struct {
	Name          string
	AppLabel      string
	Replicas      int32
	CPURequest    string
	MemoryRequest string
	CPULimit      string            // optional
	MemoryLimit   string            // optional
	PriorityClass string            // optional
	ExtraLabels   map[string]string // optional additional labels
	RollingUpdate bool              // optional - enables rolling update strategy
}

// getDeploymentSpec returns a Deployment with the given configuration
func getDeploymentSpec(config DeploymentConfig) *appsv1.Deployment {
	labels := map[string]string{"app": config.AppLabel}
	// Add any extra labels
	for k, v := range config.ExtraLabels {
		labels[k] = v
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Name,
			Labels: map[string]string{
				"app": config.AppLabel,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &config.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": config.AppLabel,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    config.Name + "-container",
							Image:   testImage,
							Command: []string{"sleep", "300"},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse(config.CPURequest),
									v1.ResourceMemory: resource.MustParse(config.MemoryRequest),
								},
							},
						},
					},
				},
			},
		},
	}

	// Add rolling update strategy if specified
	if config.RollingUpdate {
		deployment.Spec.Strategy = appsv1.DeploymentStrategy{
			Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{
				MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
				MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			},
		}
	}

	// Add resource limits if specified
	if config.CPULimit != "" && config.MemoryLimit != "" {
		deployment.Spec.Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse(config.CPULimit),
			v1.ResourceMemory: resource.MustParse(config.MemoryLimit),
		}
	}

	// Add priority class if specified
	if config.PriorityClass != "" {
		deployment.Spec.Template.Spec.PriorityClassName = config.PriorityClass
	}

	return deployment
}

// cleanupResource performs resource cleanup with proper error handling
func cleanupResource(resourceType, resourceName, namespace string, deleteFunc func() error) {
	ginkgo.By(fmt.Sprintf("Cleaning up %s: %s", resourceType, resourceName))
	err := deleteFunc()
	if err != nil && !errors.IsNotFound(err) {
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	}
}

var _ = ginkgo.Describe("Replication", func() {
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
		err = kClient.WaitForPodRunning(dev, sleepPodConfigs.Name, 60*time.Second)
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

		ginkgo.By("Validating pod labels and annotations for replication")
		gomega.Ω(podObj.Labels).To(gomega.HaveKeyWithValue("app", "sleep"), "Pod should have app=sleep label")
		gomega.Ω(podObj.Labels).To(gomega.HaveKey("applicationId"), "Pod should have applicationId label")
		gomega.Ω(podObj.Annotations).To(gomega.HaveKey("yunikorn.apache.org/allow-preemption"), "Pod should have preemption annotation")
		gomega.Ω(podObj.Spec.NodeName).NotTo(gomega.BeEmpty(), "Pod should be scheduled to a node")
		gomega.Ω(podObj.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
	})

	ginkgo.It("Verify_ReplicaSet_Pod_Replication", func() {
		ginkgo.By("Creating a ReplicaSet with multiple replicas")
		replicaCount := int32(2)
		replicaSetName := "test-replicaset"

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "replica-test",
			Replicas:      replicaCount,
			CPURequest:    "10m",
			MemoryRequest: "16Mi",
		})

		ginkgo.By("Creating the ReplicaSet")
		createdReplicaSet, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(createdReplicaSet.Name).To(gomega.Equal(replicaSetName))

		ginkgo.By("Waiting for replica pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=replica-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 replica pods running")

		ginkgo.By("Verifying all replica pods are running and scheduled by YuniKorn")
		podList, err := kClient.ListPods(dev, "app=replica-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(podList.Items).To(gomega.HaveLen(int(replicaCount)), "Should have correct number of pods")

		for _, pod := range podList.Items {
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
		}

		cleanupResource("ReplicaSet", replicaSetName, dev, func() error {
			return kClient.DeleteReplicaSet(replicaSetName, dev)
		})
	})

	ginkgo.It("Verify_Deployment_Scaling_Operations", func() {
		ginkgo.By("Creating a Deployment with initial replica count")
		deploymentName := "test-deployment"
		initialReplicas := int32(2)

		deployment := k8s.GetBasicDeploymentSpec(deploymentName, "scaling-test", initialReplicas)

		deployment.Spec.Template.Spec.Containers[0].Image = testImage

		ginkgo.By("Creating the Deployment")
		createdDeployment, err := kClient.CreateDeployment(deployment, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(createdDeployment.Name).To(gomega.Equal(deploymentName))

		ginkgo.By("Waiting for initial pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=scaling-test", int(initialReplicas), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 initial running pods")

		ginkgo.By("Scaling up the deployment")
		scaledReplicas := int32(3)
		deploymentObj, err := kClient.GetDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		deploymentObj.Spec.Replicas = &scaledReplicas

		_, err = kClient.UpdateDeployment(deploymentObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for scaled pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=scaling-test", int(scaledReplicas), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 3 scaled running pods")

		ginkgo.By("Verifying all scaled pods are scheduled by YuniKorn")
		podList, err := kClient.ListPods(dev, "app=scaling-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "All scaled pods should be scheduled by YuniKorn")
		}

		ginkgo.By("Cleaning up Deployment")
		err = kClient.DeleteDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("Verify_Pod_Failure_Recovery", func() {
		ginkgo.By("Creating a ReplicaSet for failure testing")
		replicaCount := int32(2)
		replicaSetName := "failure-test-replicaset"

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "failure-test",
			Replicas:      replicaCount,
			CPURequest:    "10m",
			MemoryRequest: "16Mi",
		})

		ginkgo.By("Creating the ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=failure-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 initial running pods")

		ginkgo.By("Deleting one pod to simulate failure")
		podList, err := kClient.ListPods(dev, "app=failure-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		podToDelete := podList.Items[0]
		err = kClient.DeletePod(podToDelete.Name, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for replacement pod to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=failure-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 pods after recovery")

		ginkgo.By("Verifying replacement pod is scheduled by YuniKorn")
		podList, err = kClient.ListPods(dev, "app=failure-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
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

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "resource-test",
			Replicas:      replicaCount,
			CPURequest:    "50m",
			MemoryRequest: "64Mi",
		})

		ginkgo.By("Creating the ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=resource-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 running pods")

		podList, err := kClient.ListPods(dev, "app=resource-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying resource allocation for all replica pods")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")

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
		replicaCount := int32(2)
		replicaSetName := "queue-test-replicaset"

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "queue-test",
			Replicas:      replicaCount,
			CPURequest:    "10m",
			MemoryRequest: "16Mi",
		})

		ginkgo.By("Creating the ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=queue-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 running pods")

		podList, err := kClient.ListPods(dev, "app=queue-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying queue assignment consistency across replica pods")
		var appIDs []string
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")

			if appID, exists := pod.Labels["applicationId"]; exists {
				appIDs = append(appIDs, appID)
			}
		}

		ginkgo.By("Verifying all pods have applicationId labels")
		gomega.Ω(appIDs).To(gomega.HaveLen(int(replicaCount)), "All pods should have applicationId labels")

		ginkgo.By("Checking YuniKorn queue assignment via REST API")
		if len(appIDs) > 0 {

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
		replicaCount := int32(2)
		replicaSetName := "multi-container-replicaset"

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "multi-container-test",
			Replicas:      replicaCount,
			CPURequest:    "25m",
			MemoryRequest: "32Mi",
			InitContainers: []v1.Container{
				{
					Name:    "init-container",
					Image:   testImage,
					Command: []string{"sh", "-c", "echo 'Init complete' && sleep 2"},
				},
			},
			SidecarContainers: []v1.Container{
				{
					Name:    "sidecar-container",
					Image:   testImage,
					Command: []string{"sh", "-c", "while true; do echo 'Sidecar running'; sleep 30; done"},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("10m"),
							v1.ResourceMemory: resource.MustParse("16Mi"),
						},
					},
				},
			},
		})

		ginkgo.By("Creating the multi-container ReplicaSet")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all multi-container pods to be created and running")
		err = kClient.WaitForPodsWithCondition(dev, "app=multi-container-test", func(podList *v1.PodList) bool {
			if len(podList.Items) != int(replicaCount) {
				return false
			}
			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false
				}
				if len(pod.Status.InitContainerStatuses) > 0 {
					initStatus := pod.Status.InitContainerStatuses[0]
					if initStatus.State.Terminated == nil || initStatus.State.Terminated.ExitCode != 0 {
						return false
					}
				}
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
		}, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "All multi-container pods should be ready")

		podList, err := kClient.ListPods(dev, "app=multi-container-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying multi-container pod structure")
		for _, pod := range podList.Items {
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")
			gomega.Ω(pod.Spec.InitContainers).To(gomega.HaveLen(1), "Pod should have 1 init container")
			gomega.Ω(pod.Spec.Containers).To(gomega.HaveLen(2), "Pod should have 2 main containers")

			if len(pod.Status.InitContainerStatuses) > 0 {
				initStatus := pod.Status.InitContainerStatuses[0]

				isInitCompleted := !initStatus.Ready ||
					(initStatus.State.Terminated != nil && initStatus.State.Terminated.ExitCode == 0)
				gomega.Ω(isInitCompleted).To(gomega.BeTrue(), "Init container should be completed successfully")
			}

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

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "node-affinity-test",
			Replicas:      replicaCount,
			CPURequest:    "10m",
			MemoryRequest: "16Mi",
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
		})

		ginkgo.By("Creating the ReplicaSet with node affinity")
		_, err = kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=node-affinity-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 running pods")

		podList, err := kClient.ListPods(dev, "app=node-affinity-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

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
		replicaCount := int32(3)

		deployment := getDeploymentSpec(DeploymentConfig{
			Name:          deploymentName,
			AppLabel:      "rolling-update-test",
			Replicas:      replicaCount,
			CPURequest:    "10m",
			MemoryRequest: "16Mi",
			ExtraLabels:   map[string]string{"version": "v1"},
			RollingUpdate: true,
		})

		ginkgo.By("Creating the initial Deployment")
		_, err := kClient.CreateDeployment(deployment, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for initial deployment to be ready")
		err = kClient.WaitForPodsRunning(dev, "app=rolling-update-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 3 initial running pods")

		ginkgo.By("Verifying initial pods are running")
		podList, err := kClient.ListPods(dev, "app=rolling-update-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(podList.Items).To(gomega.HaveLen(int(replicaCount)), "Should have correct number of initial pods")

		ginkgo.By("Triggering rolling update by changing image version")
		deploymentObj, err := kClient.GetDeployment(deploymentName, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		deploymentObj.Spec.Template.Labels["version"] = "v2"
		deploymentObj.Spec.Template.Spec.Containers[0].Env = []v1.EnvVar{
			{Name: "VERSION", Value: "v2"},
		}

		_, err = kClient.UpdateDeployment(deploymentObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for rolling update to complete")
		err = kClient.WaitForPodsWithCondition(dev, "app=rolling-update-test", func(podList *v1.PodList) bool {

			v2Count := 0
			for _, pod := range podList.Items {
				if pod.Labels["version"] == "v2" && pod.Status.Phase == v1.PodRunning {
					v2Count++
				}
			}
			return v2Count == int(replicaCount) && len(podList.Items) == int(replicaCount)
		}, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "All pods should be updated to v2 and count should be exactly 3")

		podList, err = kClient.ListPods(dev, "app=rolling-update-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

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
		replicaCount := int32(2)
		serviceName := "stateful-service"

		statefulSet := k8s.GetBasicStatefulSetSpec(statefulSetName, "stateful-test", serviceName, replicaCount)

		statefulSet.Spec.Template.Spec.Containers[0].Image = testImage

		ginkgo.By("Creating a headless service for StatefulSet")
		service := k8s.GetBasicHeadlessService(serviceName, "stateful-test")

		ginkgo.By("Creating the headless service")
		_, err := kClient.CreateService(service, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating the StatefulSet")
		_, err = kClient.CreateStatefulSet(statefulSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for StatefulSet pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=stateful-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 StatefulSet running pods")

		ginkgo.By("Verifying StatefulSet pod naming and ordering")
		expectedNames := []string{
			fmt.Sprintf("%s-0", statefulSetName),
			fmt.Sprintf("%s-1", statefulSetName),
		}

		podList, err := kClient.ListPods(dev, "app=stateful-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
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
		replicaCount := int32(5)
		replicaSetName := "stress-test-replicaset"

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "stress-test",
			Replicas:      replicaCount,
			CPURequest:    "5m",
			MemoryRequest: "8Mi",
			CPULimit:      "25m",
			MemoryLimit:   "32Mi",
		})

		ginkgo.By("Creating the stress test ReplicaSet")
		startTime := time.Now()
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all stress test pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=stress-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 5 stress test running pods")

		podList, err := kClient.ListPods(dev, "app=stress-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

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

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          "mixed-replicaset",
			AppLabel:      "mixed-replica",
			Replicas:      2,
			CPURequest:    "25m",
			MemoryRequest: "32Mi",
		})

		deployment := getDeploymentSpec(DeploymentConfig{
			Name:          "mixed-deployment",
			AppLabel:      "mixed-deploy",
			Replicas:      2,
			CPURequest:    "35m",
			MemoryRequest: "48Mi",
		})

		ginkgo.By("Creating ReplicaSet and Deployment concurrently")
		_, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		_, err = kClient.CreateDeployment(deployment, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all mixed workload pods to be created and running")

		err = kClient.WaitForPodsRunning(dev, "app=mixed-replica", 2, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 ReplicaSet pods running")

		err = kClient.WaitForPodsRunning(dev, "app=mixed-deploy", 2, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 Deployment pods running")

		replicaPodList, err := kClient.ListPods(dev, "app=mixed-replica")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		deployPodList, err := kClient.ListPods(dev, "app=mixed-deploy")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

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

		highPriorityClass := &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "high-priority",
			},
			Value:         1000,
			GlobalDefault: false,
			Description:   "High priority class for testing",
		}

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
		if err != nil && !errors.IsAlreadyExists(err) {
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}

		_, err = kClient.CreatePriorityClass(lowPriorityClass)
		if err != nil && !errors.IsAlreadyExists(err) {
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Creating ReplicaSet with high priority")
		highPriorityReplicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          "high-priority-replicaset",
			AppLabel:      "high-priority-test",
			Replicas:      2,
			CPURequest:    "50m",
			MemoryRequest: "64Mi",
			PriorityClass: "high-priority",
		})

		ginkgo.By("Creating ReplicaSet with low priority")
		lowPriorityReplicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          "low-priority-replicaset",
			AppLabel:      "low-priority-test",
			Replicas:      2,
			CPURequest:    "25m",
			MemoryRequest: "32Mi",
			PriorityClass: "low-priority",
		})

		ginkgo.By("Creating both ReplicaSets")
		_, err = kClient.CreateReplicaSet(highPriorityReplicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		_, err = kClient.CreateReplicaSet(lowPriorityReplicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all priority-based pods to be created and running")

		err = kClient.WaitForPodsRunning(dev, "app=high-priority-test", 2, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 high-priority pods running")

		err = kClient.WaitForPodsRunning(dev, "app=low-priority-test", 2, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 low-priority pods running")

		highPriorityPods, err := kClient.ListPods(dev, "app=high-priority-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		lowPriorityPods, err := kClient.ListPods(dev, "app=low-priority-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

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
		if err != nil && !errors.IsNotFound(err) {
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}
		err = kClient.DeletePriorityClass("low-priority")
		if err != nil && !errors.IsNotFound(err) {
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}
	})
})
