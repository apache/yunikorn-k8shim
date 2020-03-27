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
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAdd(t *testing.T) {
	r1 := NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	r2 := NewResourceBuilder().
		AddResource(Memory, 2).
		AddResource(CPU, 2).
		Build()
	r := Add(r1, r2)
	assert.Equal(t, len(r.Resources), 2)
	assert.Equal(t, r.Resources[Memory].Value, int64(3))
	assert.Equal(t, r.Resources[CPU].Value, int64(3))

	r1 = NewResourceBuilder().
		AddResource(Memory, 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(Memory, 2).
		AddResource(CPU, 2).
		Build()
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 2)
	assert.Equal(t, r.Resources[Memory].Value, int64(3))
	assert.Equal(t, r.Resources[CPU].Value, int64(2))

	r1 = nil
	r2 = nil
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 0)

	r1 = NewResourceBuilder().
		AddResource(Memory, 1).
		Build()
	r2 = nil
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 1)
	assert.Equal(t, r.Resources[Memory].Value, int64(1))

	r1 = nil
	r2 = NewResourceBuilder().
		AddResource(Memory, 1).
		Build()
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 1)
	assert.Equal(t, r.Resources[Memory].Value, int64(1))

	r1 = NewResourceBuilder().
		AddResource(Memory, 1024).
		AddResource(CPU, 20).
		AddResource("nvidia.com/gpu", 2).
		Build()
	r2 = NewResourceBuilder().
		AddResource(Memory, 2048).
		AddResource(CPU, 30).
		AddResource("nvidia.com/gpu", 3).
		Build()
	r = Add(r1, r2)
	assert.Equal(t, len(r.Resources), 3)
	assert.Equal(t, r.Resources[Memory].Value, int64(3072))
	assert.Equal(t, r.Resources[CPU].Value, int64(50))
	assert.Equal(t, r.Resources["nvidia.com/gpu"].Value, int64(5))
}

func TestEquals(t *testing.T) {
	r1 := NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	r2 := NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), true)

	r1 = NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(Memory, 2).
		AddResource(CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), false)

	r1 = NewResourceBuilder().
		AddResource(Memory, 1).
		Build()
	r2 = NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), false)

	r1 = nil
	r2 = nil
	assert.Equal(t, Equals(r1, r2), true)

	r1 = nil
	r2 = NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	assert.Equal(t, Equals(r1, r2), false)

	r1 = NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	r2 = nil
	assert.Equal(t, Equals(r1, r2), false)
}

func TestParsePodResource(t *testing.T) {
	containers := make([]v1.Container, 0)

	// container 01
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceMemory] = resource.MustParse("500M")
	c1Resources[v1.ResourceCPU] = resource.MustParse("1")
	c1Resources[v1.ResourceName("nvidia.com/gpu")] = resource.MustParse("1")
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
	c2Resources[v1.ResourceName("nvidia.com/gpu")] = resource.MustParse("4")
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
	assert.Equal(t, res.Resources[Memory].GetValue(), int64(1524))
	assert.Equal(t, res.Resources[CPU].GetValue(), int64(3000))
	assert.Equal(t, res.Resources["nvidia.com/gpu"].GetValue(), int64(5))
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
	assert.Equal(t, res.Resources[Memory].GetValue(), int64(1))

	// Add a resource to existing container (other than mem)
	resources[v1.ResourceCPU] = resource.MustParse("1")

	res = GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 1)
	assert.Equal(t, res.Resources[CPU].GetValue(), int64(1000))

	// reset the cpu request to zero and add memory
	resources[v1.ResourceMemory] = resource.MustParse("0")
	resources[v1.ResourceCPU] = resource.MustParse("0")

	res = GetPodResource(pod)
	assert.Equal(t, len(res.Resources), 1)
	assert.Equal(t, res.Resources[Memory].GetValue(), int64(1))
}

func TestNodeResource(t *testing.T) {
	nodeCapacity := make(map[v1.ResourceName]resource.Quantity)
	nodeCapacity[v1.ResourceCPU] = resource.MustParse("14500m")
	result := GetNodeResource(&v1.NodeStatus{
		Allocatable: nodeCapacity,
	})

	assert.Equal(t, result.Resources[CPU].GetValue(), int64(14500))
}

func TestCreateReleaseAllocationRequest(t *testing.T) {
	request := CreateReleaseAllocationRequestForTask("app01", "alloc01", "default")
	assert.Assert(t, request.Releases != nil)
	assert.Assert(t, request.Releases.AllocationsToRelease != nil)
	assert.Assert(t, request.Releases.AllocationAsksToRelease == nil)
	assert.Equal(t, len(request.Releases.AllocationsToRelease), 1)
	assert.Equal(t, len(request.Releases.AllocationAsksToRelease), 0)
	assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, "app01")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].UUID, "alloc01")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
}

func TestCreateReleaseAskRequestForTask(t *testing.T) {
	request := CreateReleaseAskRequestForTask("app01", "task01", "default")
	assert.Assert(t, request.Releases != nil)
	assert.Assert(t, request.Releases.AllocationsToRelease == nil)
	assert.Assert(t, request.Releases.AllocationAsksToRelease != nil)
	assert.Equal(t, len(request.Releases.AllocationsToRelease), 0)
	assert.Equal(t, len(request.Releases.AllocationAsksToRelease), 1)
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].ApplicationID, "app01")
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].Allocationkey, "task01")
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].PartitionName, "default")
}

func TestIsZero(t *testing.T) {
	r := NewResourceBuilder().
		AddResource(Memory, 1).
		AddResource(CPU, 1).
		Build()
	assert.Equal(t, IsZero(r), false)

	r = NewResourceBuilder().
		AddResource(CPU, 0).
		Build()
	assert.Equal(t, IsZero(r), true)

	r = NewResourceBuilder().
		AddResource(Memory, 0).
		AddResource(CPU, 0).
		Build()
	assert.Equal(t, IsZero(r), true)

	r = NewResourceBuilder().
		AddResource(Memory, 0).
		AddResource(CPU, 1).
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