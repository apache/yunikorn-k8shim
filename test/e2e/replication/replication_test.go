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

package workload_test

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
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
	// Advanced features for multi-container test
	InitContainers    []v1.Container // optional
	SidecarContainers []v1.Container // optional
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

// GetBasicDeploymentSpec returns a basic Deployment with common fields populated
func GetBasicDeploymentSpec(name, appLabel string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"app": appLabel,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": appLabel,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": appLabel,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    name + "-container",
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
}

// GetBasicStatefulSetSpec returns a basic StatefulSet with common fields populated
func GetBasicStatefulSetSpec(name, appLabel, serviceName string, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"app": appLabel,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": appLabel,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": appLabel,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    name + "-container",
							Image:   testImage,
							Command: []string{"sleep", "300"},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("50m"),
									v1.ResourceMemory: resource.MustParse("64Mi"),
								},
							},
						},
					},
				},
			},
		},
	}
}

// GetBasicHeadlessService returns a basic headless service for StatefulSets
func GetBasicHeadlessService(name, appLabel string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"app": appLabel,
			},
		},
		Spec: v1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				"app": appLabel,
			},
			Ports: []v1.ServicePort{
				{
					Port:       80,
					TargetPort: intstr.IntOrString{Type: intstr.Int, IntVal: 80},
				},
			},
		},
	}
}

// cleanupResource performs resource cleanup with proper error handling
func cleanupResource(resourceType, resourceName, namespace string, deleteFunc func() error) {
	ginkgo.By(fmt.Sprintf("Cleaning up %s: %s", resourceType, resourceName))
	err := deleteFunc()
	if err != nil && !errors.IsNotFound(err) {
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	}
}

var _ = ginkgo.Describe("Workload Test", func() {
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

	ginkgo.It("Verify_ReplicaSet_Scheduling", func() {
		ginkgo.By("Creating a ReplicaSet with multiple replicas")
		replicaCount := int32(2)
		replicaSetName := "test-replicaset"

		replicaSet := getReplicaSetSpec(ReplicaSetConfig{
			Name:          replicaSetName,
			AppLabel:      "replica-test",
			Replicas:      replicaCount,
			CPURequest:    "50m",
			MemoryRequest: "64Mi",
		})

		ginkgo.By("Creating the ReplicaSet")
		createdReplicaSet, err := kClient.CreateReplicaSet(replicaSet, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(createdReplicaSet.Name).To(gomega.Equal(replicaSetName))

		ginkgo.By("Waiting for replica pods to be created and running")
		err = kClient.WaitForPodsRunning(dev, "app=replica-test", int(replicaCount), 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred(), "Should have 2 replica pods running")

		ginkgo.By("Verifying all replica pods are running, scheduled by YuniKorn, and have correct resources")
		podList, err := kClient.ListPods(dev, "app=replica-test")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(podList.Items).To(gomega.HaveLen(int(replicaCount)), "Should have correct number of pods")

		var appIDs []string
		for _, pod := range podList.Items {
			gomega.Ω(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should be running")
			gomega.Ω(pod.Spec.SchedulerName).To(gomega.Equal("yunikorn"), "Pod should be scheduled by YuniKorn")

			// Verify resource allocation
			container := pod.Spec.Containers[0]
			cpuReq := container.Resources.Requests[v1.ResourceCPU]
			memReq := container.Resources.Requests[v1.ResourceMemory]
			gomega.Ω(cpuReq.String()).To(gomega.Equal("50m"), "CPU request should be 50m")
			gomega.Ω(memReq.String()).To(gomega.Equal("64Mi"), "Memory request should be 64Mi")

			// Collect application IDs for queue verification
			if appID, exists := pod.Labels["applicationId"]; exists {
				appIDs = append(appIDs, appID)
			}
		}

		ginkgo.By("Verifying queue assignment consistency")
		gomega.Ω(appIDs).To(gomega.HaveLen(int(replicaCount)), "All pods should have applicationId labels")

		expectedQueue := "root." + dev
		appInfo, appErr := restClient.GetAppInfo("default", expectedQueue, appIDs[0])
		gomega.Ω(appErr).NotTo(gomega.HaveOccurred())
		gomega.Ω(appInfo).NotTo(gomega.BeNil())
		gomega.Ω(appInfo.QueueName).To(gomega.ContainSubstring(dev), "App should be in namespace-based queue")

		cleanupResource("ReplicaSet", replicaSetName, dev, func() error {
			return kClient.DeleteReplicaSet(replicaSetName, dev)
		})
	})

	ginkgo.It("Verify_Deployment_Scheduling", func() {
		ginkgo.By("Creating a Deployment with initial replica count")
		deploymentName := "test-deployment"
		initialReplicas := int32(2)

		deployment := GetBasicDeploymentSpec(deploymentName, "scaling-test", initialReplicas)

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

	ginkgo.It("Verify_ReplicaSet_PodRestart", func() {
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

			initStatus := pod.Status.InitContainerStatuses[0]
			isInitCompleted := !initStatus.Ready ||
				(initStatus.State.Terminated != nil && initStatus.State.Terminated.ExitCode == 0)
			gomega.Ω(isInitCompleted).To(gomega.BeTrue(), "Init container should be completed successfully")

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

	ginkgo.It("Verify_Deployment_RollingUpdate", func() {
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

		statefulSet := GetBasicStatefulSetSpec(statefulSetName, "stateful-test", serviceName, replicaCount)

		statefulSet.Spec.Template.Spec.Containers[0].Image = testImage

		ginkgo.By("Creating a headless service for StatefulSet")
		service := GetBasicHeadlessService(serviceName, "stateful-test")

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

})
