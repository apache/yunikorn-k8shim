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
package common

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8res "k8s.io/component-helpers/resource"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/predicates"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAdd(t *testing.T) {
	r1 := NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	r2 := NewResourceBuilder().
		AddResource(siCommon.Memory, 2).
		AddResource(siCommon.CPU, 2).
		Build()
	r := Add(r1, r2)
	assert.Equal(t, len(r.Resources), 2)
	assert.Equal(t, r.Resources[siCommon.Memory].Value, int64(3))
	assert.Equal(t, r.Resources[siCommon.CPU].Value, int64(3))

	r1 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(siCommon.Memory, 2).
		AddResource(siCommon.CPU, 2).
		Build()
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 2)
	assert.Equal(t, r.Resources[siCommon.Memory].Value, int64(3))
	assert.Equal(t, r.Resources[siCommon.CPU].Value, int64(2))

	r1 = nil
	r2 = nil
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 0)

	r1 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		Build()
	r2 = nil
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 1)
	assert.Equal(t, r.Resources[siCommon.Memory].Value, int64(1))

	r1 = nil
	r2 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		Build()
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 1)
	assert.Equal(t, r.Resources[siCommon.Memory].Value, int64(1))

	r1 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1024).
		AddResource(siCommon.CPU, 20).
		AddResource("nvidia.com/gpu", 2).
		AddResource("pods", 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(siCommon.Memory, 2048).
		AddResource(siCommon.CPU, 30).
		AddResource("nvidia.com/gpu", 3).
		AddResource("pods", 1).
		Build()
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 4)
	assert.Equal(t, r.Resources[siCommon.Memory].Value, int64(3072))
	assert.Equal(t, r.Resources[siCommon.CPU].Value, int64(50))
	assert.Equal(t, r.Resources["nvidia.com/gpu"].Value, int64(5))
	assert.Equal(t, r.Resources["pods"].Value, int64(2))
}

func TestEquals(t *testing.T) {
	r1 := NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	r2 := NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), true)

	r1 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(siCommon.Memory, 2).
		AddResource(siCommon.CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), false)

	r1 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), false)

	r1 = nil
	r2 = nil
	assert.Equal(t, Equals(r1, r2), true)

	r1 = nil
	r2 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), false)

	r1 = NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	r2 = nil
	assert.Equal(t, Equals(r1, r2), false)
}

//nolint:funlen
func TestParsePodResource(t *testing.T) {
	containers := make([]v1.Container, 0, 2)

	// container 01
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceMemory] = resource.MustParse("500M")
	c1Resources[v1.ResourceCPU] = resource.MustParse("1")
	c1Resources["nvidia.com/gpu"] = resource.MustParse("1")
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})

	// container 02
	c2Resources := make(map[v1.ResourceName]resource.Quantity)
	c2Resources[v1.ResourceMemory] = resource.MustParse("1024M")
	c2Resources[v1.ResourceCPU] = resource.MustParse("2")
	c2Resources["nvidia.com/gpu"] = resource.MustParse("4")
	containers = append(containers, v1.Container{
		Name: "container-02",
		Resources: v1.ResourceRequirements{
			Requests: c2Resources,
		},
	})

	// pod
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// verify we get aggregated resource from containers
	res := GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(1524*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(5))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	resK8s := siResourceFromList(k8res.PodRequests(pod, k8res.PodResourcesOptions{}))
	assert.Assert(t, Equals(resK8s, res), "K8s pod resource request calculation yielded different results")

	// Add pod OverHead
	overHeadResources := make(map[v1.ResourceName]resource.Quantity)
	overHeadResources[v1.ResourceMemory] = resource.MustParse("500M")
	overHeadResources[v1.ResourceCPU] = resource.MustParse("1")
	overHeadResources["nvidia.com/gpu"] = resource.MustParse("1")
	// Set pod OverHead
	pod.Spec.Overhead = overHeadResources
	// verify we get aggregated resource from containers
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(2024*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(4000))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(6))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	resK8s = siResourceFromList(k8res.PodRequests(pod, k8res.PodResourcesOptions{ExcludeOverhead: false}))
	assert.Assert(t, Equals(resK8s, res), "K8s pod resource request calculation yielded different results")

	// test initcontainer and container resouce compare
	initContainers := make([]v1.Container, 0, 2)
	initc1Resources := make(map[v1.ResourceName]resource.Quantity)
	initc1Resources[v1.ResourceMemory] = resource.MustParse("4096M")
	initc1Resources[v1.ResourceCPU] = resource.MustParse("0.5")
	initc1Resources["nvidia.com/gpu"] = resource.MustParse("1")
	initContainers = append(initContainers, v1.Container{
		Name: "initcontainer-01",
		Resources: v1.ResourceRequirements{
			Requests: initc1Resources,
		},
	})

	initc2Resources := make(map[v1.ResourceName]resource.Quantity)
	initc2Resources[v1.ResourceMemory] = resource.MustParse("10000M")
	initc2Resources[v1.ResourceCPU] = resource.MustParse("5.12")
	initc2Resources["nvidia.com/gpu"] = resource.MustParse("4")
	initContainers = append(initContainers, v1.Container{
		Name: "initcontainer-02",
		Resources: v1.ResourceRequirements{
			Requests: initc2Resources,
		},
	})

	containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("2000M")
	containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("4.096")
	containers[0].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("2")

	containers[1].Resources.Requests[v1.ResourceMemory] = resource.MustParse("5000M")
	containers[1].Resources.Requests[v1.ResourceCPU] = resource.MustParse("1.024")
	containers[1].Resources.Requests["nvidia.com/gpu"] = resource.MustParse("2")

	pod.ObjectMeta = apis.ObjectMeta{
		Name: "pod-resource-test-00002",
		UID:  "UID-00002",
	}
	pod.Spec = v1.PodSpec{
		Containers:     containers,
		InitContainers: initContainers,
	}
	// initcontainers
	// IC1{500mi, 1000m, 1}
	// IC2{5120mi, 10000m, 4}
	// sum of containers{5120mi, 7000m, 4}
	// C1{4096mi, 2000m, 2}
	// C2{1024mi, 5000m, 2}
	// result {5120mi, 10000m, 4}
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(10000000000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(5120))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(4))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	resK8s = siResourceFromList(k8res.PodRequests(pod, k8res.PodResourcesOptions{}))
	assert.Assert(t, Equals(resK8s, res), "K8s pod resource request calculation yielded different results")

	delete(containers[0].Resources.Requests, "nvidia.com/gpu")
	delete(containers[1].Resources.Requests, "nvidia.com/gpu")
	delete(initContainers[1].Resources.Requests, v1.ResourceCPU)
	delete(initContainers[1].Resources.Requests, "nvidia.com/gpu")
	pod.Spec = v1.PodSpec{
		Containers:     containers,
		InitContainers: initContainers,
	}
	// IC1{500mi, 1000m, 1}
	// IC2{0mi, 10000m}
	// sum of containers{5120mi, 7000m}
	// result {5120mi, 10000m, 1}
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(10000000000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(5120))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(1))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	resK8s = siResourceFromList(k8res.PodRequests(pod, k8res.PodResourcesOptions{}))
	assert.Assert(t, Equals(resK8s, res), "K8s pod resource request calculation yielded different results")

	// single restartable init container (sidecar) {mem,CPU}
	// IC1{10M, 1000mi}
	// C1{2000M, 4096mi}
	// C2{5000M, 1024mi}
	// sum of containers{7010M, 6120mi}
	initContainers = initContainers[:0]
	alwaysRestart := v1.ContainerRestartPolicyAlways
	smallResources := map[v1.ResourceName]resource.Quantity{
		v1.ResourceMemory: resource.MustParse("10M"),
		v1.ResourceCPU:    resource.MustParse("1"),
	}
	initContainers = append(initContainers, v1.Container{
		Name: "container-04",
		Resources: v1.ResourceRequirements{
			Requests: smallResources,
		},
		RestartPolicy: &alwaysRestart,
	})
	pod.Spec.InitContainers = initContainers
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(7010000000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(6120))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	resK8s = siResourceFromList(k8res.PodRequests(pod, k8res.PodResourcesOptions{}))
	assert.Assert(t, Equals(resK8s, res), "K8s pod resource request calculation yielded different results")

	// restartable + non-restartable init containers {mem,CPU}
	// C1{2000M, 4096mi}
	// C2{5000M, 1024mi}
	// IC1{10M, 1000mi} sidecar
	// IC2{10M, 1000mi} sidecar
	// IC3{4096M, 10000mi}
	// usage calculation:
	// When IC3 starts, necessary resource is Req1=IC1+IC2+IC3 {4116m, 12000mi}
	// After IC3 completed, necessary resource is Req2=IC1,IC2,C1,C2 {7020m, 7120mi}
	// Resource request for pod is max(Req1, Req2): {7020m, 12000m}
	initContainers = initContainers[:0]
	initContainers = append(initContainers, v1.Container{
		Name: "container-05",
		Resources: v1.ResourceRequirements{
			Requests: smallResources,
		},
		RestartPolicy: &alwaysRestart,
	})
	initContainers = append(initContainers, v1.Container{
		Name: "container-06",
		Resources: v1.ResourceRequirements{
			Requests: smallResources,
		},
		RestartPolicy: &alwaysRestart,
	})
	initContainers = append(initContainers, v1.Container{
		Name: "container-07",
		Resources: v1.ResourceRequirements{
			Requests: map[v1.ResourceName]resource.Quantity{
				v1.ResourceMemory: resource.MustParse("4096M"),
				v1.ResourceCPU:    resource.MustParse("10"),
			},
		},
	})
	pod.Spec.InitContainers = initContainers
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(7020000000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(12000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	resK8s = siResourceFromList(k8res.PodRequests(pod, k8res.PodResourcesOptions{}))
	assert.Assert(t, Equals(resK8s, res), "K8s pod resource request calculation yielded different results")
}

func siResourceFromList(list v1.ResourceList) *si.Resource {
	builder := NewResourceBuilder()
	for name, val := range list {
		if name == v1.ResourceCPU {
			builder.AddResource("vcore", val.MilliValue())
			continue
		}
		builder.AddResource(name.String(), val.Value())
	}

	builder.AddResource("pods", 1)
	return builder.Build()
}

func TestGetPodResourcesWithPodLevelRequests(t *testing.T) {
	// ensure required K8s feature gates are enabled
	predicates.EnableOptionalKubernetesFeatureGates()

	containers := make([]v1.Container, 0)

	// container 01
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceMemory] = resource.MustParse("500M")
	c1Resources[v1.ResourceCPU] = resource.MustParse("1")
	c1Resources["nvidia.com/gpu"] = resource.MustParse("1")
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})

	// container 02
	c2Resources := make(map[v1.ResourceName]resource.Quantity)
	c2Resources[v1.ResourceMemory] = resource.MustParse("1024M")
	c2Resources[v1.ResourceCPU] = resource.MustParse("2")
	c2Resources["nvidia.com/gpu"] = resource.MustParse("4")
	containers = append(containers, v1.Container{
		Name: "container-02",
		Resources: v1.ResourceRequirements{
			Requests: c2Resources,
		},
	})

	// pod
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Resources: &v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("128M"),
					v1.ResourceCPU:    resource.MustParse("5"),
					"invalid":         resource.MustParse("1"),
				},
			},
			Containers: containers,
		},
	}

	// verify cpu and memory overrides
	res := GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(128*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(5000))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(5))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	_, invalidOk := res.Resources["invalid"]
	assert.Assert(t, !invalidOk, "invalid should not be present")
}

//nolint:funlen
func TestGetPodResourcesWithInPlacePodVerticalScaling(t *testing.T) {
	// ensure required K8s feature gates are enabled
	predicates.EnableOptionalKubernetesFeatureGates()

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container-01",
					Resources: v1.ResourceRequirements{
						Requests: map[v1.ResourceName]resource.Quantity{
							v1.ResourceMemory: resource.MustParse("1000M"),
							v1.ResourceCPU:    resource.MustParse("1"),
						},
					},
				},
				{
					Name: "container-02",
					Resources: v1.ResourceRequirements{
						Requests: map[v1.ResourceName]resource.Quantity{
							v1.ResourceMemory: resource.MustParse("2000M"),
							v1.ResourceCPU:    resource.MustParse("2"),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			ContainerStatuses: nil,
			Resize:            "",
		},
	}

	// verify we get aggregated resource from containers
	res := GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(3000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// validate with empty ContainerStatuses
	pod.Status.ContainerStatuses = []v1.ContainerStatus{}
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(3000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// validate with empty resources
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "container-01", AllocatedResources: nil, Resources: nil},
		{Name: "container-02", AllocatedResources: nil, Resources: nil},
	}
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(3000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// simulate the pod moving to running state by setting assigned resources to the same values
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "container-01", AllocatedResources: pod.Spec.Containers[0].Resources.Requests.DeepCopy(), Resources: pod.Spec.Containers[0].Resources.DeepCopy()},
		{Name: "container-02", AllocatedResources: pod.Spec.Containers[1].Resources.Requests.DeepCopy(), Resources: pod.Spec.Containers[1].Resources.DeepCopy()},
	}
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(3000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// simulate a proposed pod resize (memory up, cpu down)
	pod.Status.Resize = v1.PodResizeStatusProposed
	pod.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("2000M")
	pod.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("500m")
	pod.Spec.Containers[1].Resources.Requests[v1.ResourceMemory] = resource.MustParse("4000M")
	pod.Spec.Containers[1].Resources.Requests[v1.ResourceCPU] = resource.MustParse("1")
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(6000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// simulate an infeasible pod resize
	pod.Status.Resize = v1.PodResizeStatusInfeasible
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(3000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// same, but using conditions only (simulates post-alpha behavior)
	pod.Status.Resize = ""
	pod.Status.Conditions = []v1.PodCondition{{Type: constants.PodStatusPodResizePending, Status: v1.ConditionTrue, Reason: string(v1.PodResizeStatusInfeasible)}}
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(3000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// simulate an in-progress pod resize
	pod.Status.Resize = v1.PodResizeStatusInProgress
	pod.Status.Conditions = []v1.PodCondition{{Type: constants.PodStatusPodResizing, Status: constants.True}}
	pod.Status.ContainerStatuses[0].AllocatedResources = pod.Spec.Containers[0].Resources.Requests.DeepCopy()
	pod.Status.ContainerStatuses[1].AllocatedResources = pod.Spec.Containers[1].Resources.Requests.DeepCopy()
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(6000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// simulate a completed pod resize
	pod.Status.Resize = ""
	pod.Status.Conditions = []v1.PodCondition{}
	pod.Status.ContainerStatuses[0].Resources = pod.Spec.Containers[0].Resources.DeepCopy()
	pod.Status.ContainerStatuses[1].Resources = pod.Spec.Containers[1].Resources.DeepCopy()
	res = GetPodResource(pod)
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(6000*1000*1000))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(1500))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
}

func TestBestEffortPod(t *testing.T) {
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})

	// pod, no resources requested
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// best effort pod all resources are nil or zero
	res := GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 1)
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// Add a resource to existing container (other than mem)
	resources[v1.ResourceCPU] = resource.MustParse("1")

	res = GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 2)
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(1000))
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))

	// reset the cpu request to zero and add memory
	resources[v1.ResourceMemory] = resource.MustParse("0")
	resources[v1.ResourceCPU] = resource.MustParse("0")

	res = GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 3)
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	assert.Equal(t, res.Resources[siCommon.CPU].GetValue(), int64(0))
	assert.Equal(t, res.Resources[siCommon.Memory].GetValue(), int64(0))
}

func TestGPUOnlyResources(t *testing.T) {
	containers := make([]v1.Container, 0)

	// container 01
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceName("nvidia.com/gpu")] = resource.MustParse("1")
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	res := GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 2)
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(1))

	c1Resources[v1.ResourceName("nvidia.com/gpu")] = resource.MustParse("0")
	res = GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 2)
	assert.Equal(t, res.Resources["pods"].GetValue(), int64(1))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(0))
}

func TestNodeResource(t *testing.T) {
	nodeCapacity := make(map[v1.ResourceName]resource.Quantity)
	nodeCapacity[v1.ResourceCPU] = resource.MustParse("14500m")
	result := GetNodeResource(&v1.NodeStatus{
		Allocatable: nodeCapacity,
	})

	assert.Equal(t, result.Resources[siCommon.CPU].GetValue(), int64(14500))
}

func TestIsZero(t *testing.T) {
	r := NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	assert.Equal(t, IsZero(r), false)

	r = NewResourceBuilder().
		AddResource(siCommon.CPU, 0).
		Build()
	assert.Equal(t, IsZero(r), true)

	r = NewResourceBuilder().
		AddResource(siCommon.Memory, 0).
		AddResource(siCommon.CPU, 0).
		Build()
	assert.Equal(t, IsZero(r), true)

	r = NewResourceBuilder().
		AddResource(siCommon.Memory, 0).
		AddResource(siCommon.CPU, 1).
		Build()
	assert.Equal(t, IsZero(r), false)

	assert.Equal(t, IsZero(nil), true)

	r = &si.Resource{}
	assert.Equal(t, IsZero(r), true)
}

func TestSub(t *testing.T) {
	// simple case (nil checks)
	result := Sub(nil, nil)
	if result == nil || len(result.Resources) != 0 {
		t.Errorf("sub nil resources did not return zero resource: %v", result)
	}

	// empty resources
	left := NewResourceBuilder().Build()
	result = Sub(left, nil)
	if result == nil || len(result.Resources) != 0 || result != left {
		t.Errorf("sub Zero resource (right) did not return cloned resource: %v", result)
	}

	// simple empty resources
	res1 := NewResourceBuilder().
		AddResource("a", 5).
		Build()
	result = Sub(left, res1)

	expected := NewResourceBuilder().
		AddResource("a", -5).
		Build()
	if !Equals(result, expected) {
		t.Errorf("sub failed expected %v, actual %v", expected, result.Resources)
	}

	// complex case: just checking the resource merge, values check is secondary
	res1 = NewResourceBuilder().
		AddResource("a", 0).
		AddResource("b", 1).
		Build()
	res2 := NewResourceBuilder().
		AddResource("a", 1).
		AddResource("c", 0).
		AddResource("d", -1).
		Build()
	res3 := Sub(res1, res2)

	expected = NewResourceBuilder().
		AddResource("a", -1).
		AddResource("b", 1).
		AddResource("c", 0).
		AddResource("d", 1).
		Build()

	if !Equals(res3, expected) {
		t.Errorf("sub failed expected %v, actual %v", expected, res3.Resources)
	}
}

func TestParseResourceString(t *testing.T) {
	testCases := []struct {
		cpu          string
		mem          string
		cpuExist     bool
		memoryExist  bool
		expectCPU    int64
		expectMemory int64
	}{
		// empty values
		{"", "", false, false, 0, 0},
		// cpu values
		{"1", "", true, false, 1000, 0},
		{"0.5", "", true, false, 500, 0},
		{"0.33", "", true, false, 330, 0},
		{"1000", "", true, false, 1000000, 0},
		{"-10", "", true, false, -10000, 0},
		{"100m", "", true, false, 100, 0},
		// memory values
		{"", "65536", false, true, 0, 65536},
		{"", "129M", false, true, 0, 129 * 1000 * 1000},
		{"", "123Mi", false, true, 0, 123 * 1024 * 1024},
		{"", "128974848", false, true, 0, 128974848},
		{"", "1G", false, true, 0, 1000 * 1000 * 1000},
		{"", "1Gi", false, true, 0, 1024 * 1024 * 1024},
		{"", "1T", false, true, 0, 1000 * 1000 * 1000 * 1000},
		{"", "1P", false, true, 0, 1000 * 1000 * 1000 * 1000 * 1000},
		{"", "1E", false, true, 0, 1000 * 1000 * 1000 * 1000 * 1000 * 1000},
		// cpu and memory
		{"0.5", "64M", true, true, 500, 64 * 1000 * 1000},
		// parsing error on cpu
		{"xyz", "64M", false, false, 0, 0},
		// parsing error on memory
		{"100m", "64MiB", false, false, 0, 0},
		// parsing failed for both cpu and memory
		{"xyz", "64MiB", false, false, 0, 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("cpu: %s, memory: %s", tc.cpu, tc.mem), func(t *testing.T) {
			siResource := ParseResource(tc.cpu, tc.mem)
			cpuRes, hasCPU := siResource.GetResources()[siCommon.CPU]
			assert.Equal(t, hasCPU, tc.cpuExist)
			assert.Equal(t, cpuRes.GetValue(), tc.expectCPU)
			memRes, hasMem := siResource.GetResources()[siCommon.Memory]
			assert.Equal(t, hasMem, tc.memoryExist)
			assert.Equal(t, memRes.GetValue(), tc.expectMemory)
		})
	}
}

func TestGetResource(t *testing.T) {
	tests := []struct {
		name        string
		resMap      map[string]string
		expectedRes map[string]int64
	}{
		{
			name:        "empty resMap",
			resMap:      map[string]string{},
			expectedRes: map[string]int64{},
		},
		{
			name: "single resource",
			resMap: map[string]string{
				v1.ResourceCPU.String(): "100m",
			},
			expectedRes: map[string]int64{
				siCommon.CPU: 100,
			},
		},
		{
			name: "multiple resources",
			resMap: map[string]string{
				v1.ResourceCPU.String():    "1",
				v1.ResourceMemory.String(): "1G",
			},
			expectedRes: map[string]int64{
				siCommon.CPU:    1000,
				siCommon.Memory: 1000 * 1000 * 1000,
			},
		},
		{
			name: "invalid cpu resources",
			resMap: map[string]string{
				v1.ResourceCPU.String(): "xyz",
			},
			expectedRes: nil,
		},
		{
			name: "invalid memory resources",
			resMap: map[string]string{
				v1.ResourceMemory.String(): "64MiB",
			},
			expectedRes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualRes := GetResource(tt.resMap)
			if tt.expectedRes == nil {
				assert.Assert(t, actualRes == nil)
			} else {
				assert.Equal(t, len(actualRes.Resources), len(tt.expectedRes))
				if len(tt.expectedRes) > 0 {
					for name, value := range tt.expectedRes {
						assert.Equal(t, actualRes.Resources[name].GetValue(), value)
					}
				}
			}
		})
	}
}

func TestGetTGResource(t *testing.T) {
	tests := []struct {
		name        string
		resMap      map[string]resource.Quantity
		members     int64
		expectedRes map[string]int64
	}{
		{
			name:    "empty resMap",
			resMap:  map[string]resource.Quantity{},
			members: 2,
			expectedRes: map[string]int64{
				"pods": 2,
			},
		},
		{
			name: "single resource",
			resMap: map[string]resource.Quantity{
				v1.ResourceCPU.String(): resource.MustParse("100m"),
			},
			members: 2,
			expectedRes: map[string]int64{
				"pods":       2,
				siCommon.CPU: 2 * 100,
			},
		},
		{
			name: "multiple resources",
			resMap: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("1"),
				v1.ResourceMemory.String(): resource.MustParse("1G"),
			},
			members: 2,
			expectedRes: map[string]int64{
				"pods":          2,
				siCommon.CPU:    2 * 1000,
				siCommon.Memory: 2 * 1000 * 1000 * 1000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualRes := GetTGResource(tt.resMap, tt.members)
			assert.Equal(t, len(actualRes.Resources), len(tt.expectedRes))
			for name, value := range tt.expectedRes {
				assert.Equal(t, actualRes.Resources[name].GetValue(), value)
			}
		})
	}
}
