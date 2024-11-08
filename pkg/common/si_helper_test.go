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
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const nodeID = "node-01"

func TestCreateReleaseRequestForTask(t *testing.T) {
	// with allocationKey
	request := CreateReleaseRequestForTask("app01", "task01", "default", si.TerminationType_STOPPED_BY_RM)
	assert.Assert(t, request.Releases != nil)
	assert.Assert(t, request.Releases.AllocationsToRelease != nil)
	assert.Equal(t, len(request.Releases.AllocationsToRelease), 1)
	assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, "app01")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].TerminationType, si.TerminationType_STOPPED_BY_RM)

	request = CreateReleaseRequestForTask("app01", "task01", "default", si.TerminationType_UNKNOWN_TERMINATION_TYPE)
	assert.Assert(t, request.Releases != nil)
	assert.Assert(t, request.Releases.AllocationsToRelease != nil)
	assert.Equal(t, len(request.Releases.AllocationsToRelease), 1)
	assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, "app01")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
	assert.Equal(t, request.Releases.AllocationsToRelease[0].TerminationType, si.TerminationType_UNKNOWN_TERMINATION_TYPE)
}

func TestCreateUpdateRequestForRemoveApplication(t *testing.T) {
	request := CreateUpdateRequestForRemoveApplication("app01", "default")
	assert.Assert(t, request.Remove != nil)
	assert.Equal(t, len(request.Remove), 1)
	assert.Equal(t, request.Remove[0].ApplicationID, "app01")
	assert.Equal(t, request.Remove[0].PartitionName, "default")
}

func TestCreateUpdateRequestForTask(t *testing.T) {
	res := NewResourceBuilder().Build()
	podName := "pod-resource-test-00001"
	namespace := "important"
	labels := map[string]string{
		"label1": "val1",
		"label2": "val2",
	}
	annotations := map[string]string{
		"key": "value",
	}
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        podName,
			UID:         "UID-00001",
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
	}

	preemptionPolicy := &si.PreemptionPolicy{
		AllowPreemptSelf:  true,
		AllowPreemptOther: true,
	}

	updateRequest := CreateAllocationRequestForTask("appId1", "taskId1", res, false, "", pod, false, preemptionPolicy)
	asks := updateRequest.Allocations
	assert.Equal(t, len(asks), 1)
	allocAsk := asks[0]
	assert.Assert(t, allocAsk != nil)
	assert.Assert(t, allocAsk.PreemptionPolicy != nil)
	assert.Equal(t, allocAsk.PreemptionPolicy.AllowPreemptSelf, true)
	assert.Equal(t, allocAsk.PreemptionPolicy.AllowPreemptOther, true)
	tags := allocAsk.AllocationTags
	assert.Assert(t, tags != nil)
	assert.Equal(t, tags[common.DomainK8s+common.GroupMeta+"podName"], podName)
	assert.Equal(t, tags[common.DomainK8s+common.GroupMeta+"namespace"], namespace)

	assert.Equal(t, tags[common.DomainK8s+common.GroupLabel+"label1"], "val1")
	assert.Equal(t, tags[common.DomainK8s+common.GroupLabel+"label2"], "val2")
}

func TestCreateTagsForTask(t *testing.T) {
	podName1 := "test1"
	podName2 := "test2"
	podNamespace := "default"
	labels := map[string]string{
		"label1": "val1",
		"label2": "val2",
	}
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      podName1,
			UID:       "UID-00001",
			Namespace: podNamespace,
			Labels:    labels,
		},
	}
	metaPrefix := common.DomainK8s + common.GroupMeta
	labelPrefix := common.DomainK8s + common.GroupLabel
	// pod without ownerReference
	result1 := CreateTagsForTask(pod)
	assert.Equal(t, len(result1), 4)
	assert.Equal(t, result1[metaPrefix+common.KeyNamespace], podNamespace)
	assert.Equal(t, result1[metaPrefix+common.KeyPodName], podName1)
	for k, v := range pod.Labels {
		assert.Equal(t, result1[labelPrefix+k], v)
	}
	// pod with DaemonSet ownerReference
	pod.Name = podName2
	owner := apis.OwnerReference{
		APIVersion: "v1",
		Kind:       "DaemonSet",
		Name:       "DaemonSetPod",
		UID:        "UID-001",
	}
	refer := []apis.OwnerReference{
		owner,
	}
	pod.SetOwnerReferences(refer)
	// pod with nodeAffinity wich add by daemonSet controller
	requiremant1 := v1.NodeSelectorRequirement{
		Key:      "key1",
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{"value1"},
	}
	requiremant2 := v1.NodeSelectorRequirement{
		Key:      "metadata.name",
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{"nodeName"},
	}
	fields := []v1.NodeSelectorRequirement{requiremant1, requiremant2}
	terms := []v1.NodeSelectorTerm{
		{
			MatchFields: fields,
		},
	}
	affinity := &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: terms,
			},
		},
	}
	pod.Spec.Affinity = affinity
	result2 := CreateTagsForTask(pod)
	assert.Equal(t, len(result2), 5)
	assert.Equal(t, result2[metaPrefix+common.KeyNamespace], podNamespace)
	assert.Equal(t, result2[metaPrefix+common.KeyPodName], podName2)
	assert.Equal(t, result2[common.DomainYuniKorn+common.KeyRequiredNode], "nodeName")
	for k, v := range pod.Labels {
		assert.Equal(t, result2[labelPrefix+k], v)
	}
	// Affinity is nil
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
	result3 := CreateTagsForTask(pod)
	assert.Equal(t, len(result3), 4)
	pod.Spec.Affinity.NodeAffinity = nil
	result3 = CreateTagsForTask(pod)
	assert.Equal(t, len(result3), 4)
	pod.Spec.Affinity = nil
	result3 = CreateTagsForTask(pod)
	assert.Equal(t, len(result3), 4)

	// pod with ReplicaSet ownerReference
	owner2 := apis.OwnerReference{
		APIVersion: "v1",
		Kind:       "ReplicaSet",
		Name:       "ReplicaSetPod",
		UID:        "UID-002",
	}
	refer2 := []apis.OwnerReference{
		owner2,
	}
	pod.SetOwnerReferences(refer2)
	result4 := CreateTagsForTask(pod)
	assert.Equal(t, len(result4), 4)
}

func TestCreateUpdateRequestForUpdatedNode(t *testing.T) {
	capacity := NewResourceBuilder().AddResource(common.Memory, 200).AddResource(common.CPU, 2).Build()
	request := CreateUpdateRequestForUpdatedNode(nodeID, capacity)
	assert.Equal(t, len(request.Nodes), 1)
	assert.Equal(t, request.Nodes[0].NodeID, nodeID)
	assert.Equal(t, request.Nodes[0].SchedulableResource, capacity)
	assert.Equal(t, len(request.Nodes[0].Attributes), 0)
}

func TestCreateUpdateRequestForDeleteNode(t *testing.T) {
	action := si.NodeInfo_DECOMISSION
	// asserting against this empty map ensures core doesn't have any issues
	request := CreateUpdateRequestForDeleteOrRestoreNode(nodeID, action)
	assert.Equal(t, len(request.Nodes), 1)
	assert.Equal(t, request.Nodes[0].NodeID, nodeID)
	assert.Equal(t, request.Nodes[0].Action, action)

	action1 := si.NodeInfo_DRAIN_NODE
	request1 := CreateUpdateRequestForDeleteOrRestoreNode(nodeID, action1)
	assert.Equal(t, len(request1.Nodes), 1)
	assert.Equal(t, request1.Nodes[0].NodeID, nodeID)
	assert.Equal(t, request1.Nodes[0].Action, action1)

	action2 := si.NodeInfo_DRAIN_TO_SCHEDULABLE
	request2 := CreateUpdateRequestForDeleteOrRestoreNode(nodeID, action2)
	assert.Equal(t, len(request2.Nodes), 1)
	assert.Equal(t, request2.Nodes[0].NodeID, nodeID)
	assert.Equal(t, request2.Nodes[0].Action, action2)
}

func TestCreateAllocationRequestForTask(t *testing.T) {
	res := NewResourceBuilder().Build()
	podName := "pod-resource-test-00001"
	namespace := "important"
	annotations := map[string]string{
		"key": "value",
	}
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        podName,
			UID:         "UID-00001",
			Namespace:   namespace,
			Annotations: annotations,
		},
	}

	preemptionPolicy := &si.PreemptionPolicy{
		AllowPreemptSelf:  false,
		AllowPreemptOther: true,
	}

	updateRequest := CreateAllocationRequestForTask("appId1", "taskId1", res, false, "", pod, false, preemptionPolicy)
	asks := updateRequest.Allocations
	assert.Equal(t, len(asks), 1)
	allocAsk := asks[0]
	if allocAsk == nil {
		t.Fatal("ask cannot be nil")
	}
	assert.Equal(t, allocAsk.Priority, int32(0))
	assert.Assert(t, allocAsk.PreemptionPolicy != nil)
	assert.Equal(t, allocAsk.PreemptionPolicy.AllowPreemptSelf, false)
	assert.Equal(t, allocAsk.PreemptionPolicy.AllowPreemptOther, true)

	podName1 := "pod-resource-test-00002"
	var pri = int32(100)
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        podName1,
			UID:         "UID-00002",
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: v1.PodSpec{Priority: &pri},
	}

	preemptionPolicy1 := &si.PreemptionPolicy{
		AllowPreemptSelf:  true,
		AllowPreemptOther: false,
	}

	updateRequest1 := CreateAllocationRequestForTask("appId1", "taskId1", res, false, "", pod1, false, preemptionPolicy1)
	asks1 := updateRequest1.Allocations
	assert.Equal(t, len(asks1), 1)
	allocAsk1 := asks1[0]
	if allocAsk1 == nil {
		t.Fatal("ask cannot be nil")
	}
	assert.Assert(t, allocAsk1.PreemptionPolicy != nil)
	assert.Equal(t, allocAsk1.PreemptionPolicy.AllowPreemptSelf, true)
	assert.Equal(t, allocAsk1.PreemptionPolicy.AllowPreemptOther, false)
	tags := allocAsk1.AllocationTags
	assert.Equal(t, tags[common.DomainK8s+common.GroupMeta+"podName"], podName1)
	assert.Equal(t, allocAsk1.Priority, int32(100))
}

func TestCreateAllocationForTask(t *testing.T) {
	res := NewResourceBuilder().Build()
	podName := "pod-resource-test-00001"
	namespace := "important"
	annotations := map[string]string{
		"key": "value",
	}
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        podName,
			UID:         "UID-00001",
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			NodeName: "node1",
		},
	}

	preemptionPolicy := &si.PreemptionPolicy{
		AllowPreemptSelf:  false,
		AllowPreemptOther: true,
	}

	updateRequest := CreateAllocationForTask("appId1", "taskId1", "node1", res, false, "", pod, false, preemptionPolicy)
	allocs := updateRequest.Allocations
	assert.Equal(t, len(allocs), 1)
	alloc := allocs[0]
	if alloc == nil {
		t.Fatal("alloc cannot be nil")
	}
	assert.Equal(t, alloc.Priority, int32(0))
	assert.Assert(t, alloc.PreemptionPolicy != nil)
	assert.Equal(t, alloc.PreemptionPolicy.AllowPreemptSelf, false)
	assert.Equal(t, alloc.PreemptionPolicy.AllowPreemptOther, true)

	podName1 := "pod-resource-test-00002"
	var pri = int32(100)
	pod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:        podName1,
			UID:         "UID-00002",
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			Priority: &pri,
			NodeName: "node1",
		},
	}

	preemptionPolicy1 := &si.PreemptionPolicy{
		AllowPreemptSelf:  true,
		AllowPreemptOther: false,
	}

	updateRequest1 := CreateAllocationForTask("appId1", "taskId1", "node1", res, false, "", pod1, false, preemptionPolicy1)
	allocs1 := updateRequest1.Allocations
	assert.Equal(t, len(allocs1), 1)
	alloc1 := allocs1[0]
	if alloc1 == nil {
		t.Fatal("alloc cannot be nil")
	}
	assert.Assert(t, alloc1.PreemptionPolicy != nil)
	assert.Equal(t, alloc1.PreemptionPolicy.AllowPreemptSelf, true)
	assert.Equal(t, alloc1.PreemptionPolicy.AllowPreemptOther, false)
	tags := alloc1.AllocationTags
	assert.Equal(t, tags[common.DomainK8s+common.GroupMeta+"podName"], podName1)
	assert.Equal(t, alloc1.Priority, int32(100))
}

// TestGetTerminationTypeFromString tests the GetTerminationTypeFromString function.
func TestGetTerminationTypeFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected si.TerminationType
	}{
		{"UNKNOWN_TERMINATION_TYPE", si.TerminationType_UNKNOWN_TERMINATION_TYPE},
		{"STOPPED_BY_RM", si.TerminationType_STOPPED_BY_RM},
		{"TIMEOUT", si.TerminationType_TIMEOUT},
		{"PREEMPTED_BY_SCHEDULER", si.TerminationType_PREEMPTED_BY_SCHEDULER},
		{"PLACEHOLDER_REPLACED", si.TerminationType_PLACEHOLDER_REPLACED},
		{"INVALID_TYPE", si.TerminationType_STOPPED_BY_RM},
		{"", si.TerminationType_STOPPED_BY_RM},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := GetTerminationTypeFromString(test.input)
			if result != test.expected {
				t.Errorf("For input '%s', expected %v, got %v", test.input, test.expected, result)
			}
		})
	}
}

func TestCreateAllocationForForeignPod(t *testing.T) {
	cResources := make(map[v1.ResourceName]resource.Quantity)
	cResources[v1.ResourceMemory] = resource.MustParse("500M")
	cResources[v1.ResourceCPU] = resource.MustParse("1")
	var containers []v1.Container
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: cResources,
		},
	})

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "test",
			UID:  "UID-00001",
			CreationTimestamp: apis.Time{
				Time: time.Unix(1, 0),
			},
			Labels: map[string]string{
				"testKey": "testValue",
			},
			Namespace: "testNamespace",
		},
		Spec: v1.PodSpec{
			Containers: containers,
			NodeName:   nodeID,
		},
	}

	allocReq := CreateAllocationForForeignPod(pod)
	assert.Equal(t, 1, len(allocReq.Allocations))
	assert.Equal(t, "mycluster", allocReq.RmID)
	assert.Assert(t, allocReq.Releases == nil)
	alloc := allocReq.Allocations[0]
	assert.Equal(t, nodeID, alloc.NodeID)
	assert.Equal(t, "UID-00001", alloc.AllocationKey)
	assert.Equal(t, int32(0), alloc.Priority)
	res := alloc.ResourcePerAlloc
	assert.Equal(t, 3, len(res.Resources))
	assert.Equal(t, int64(500000000), res.Resources["memory"].Value)
	assert.Equal(t, int64(1000), res.Resources["vcore"].Value)
	assert.Equal(t, int64(1), res.Resources["pods"].Value)
	assert.Equal(t, 5, len(alloc.AllocationTags))
	assert.Equal(t, "1", alloc.AllocationTags[common.CreationTime])
	assert.Equal(t, alloc.AllocationTags["kubernetes.io/meta/namespace"], "testNamespace")
	assert.Equal(t, alloc.AllocationTags["kubernetes.io/meta/podName"], "test")
	assert.Equal(t, alloc.AllocationTags["kubernetes.io/label/testKey"], "testValue")

	// set priority & change pod type to static
	prio := int32(12)
	pod.Spec.Priority = &prio
	pod.OwnerReferences = []apis.OwnerReference{
		{
			Kind: "Node",
		},
	}
	allocReq = CreateAllocationForForeignPod(pod)
	assert.Equal(t, 5, len(alloc.AllocationTags))
	alloc = allocReq.Allocations[0]
	assert.Equal(t, common.AllocTypeStatic, alloc.AllocationTags[common.Foreign])
	assert.Equal(t, int32(12), alloc.Priority)
}

func TestCreateReleaseRequestForForeignPod(t *testing.T) {
	allocReq := CreateReleaseRequestForForeignPod("UID-0001", "partition")

	assert.Assert(t, allocReq.Releases != nil)
	assert.Equal(t, "mycluster", allocReq.RmID)
	releaseReq := allocReq.Releases
	assert.Equal(t, 1, len(releaseReq.AllocationsToRelease))
	release := releaseReq.AllocationsToRelease[0]
	assert.Equal(t, "UID-0001", release.AllocationKey)
	assert.Equal(t, "partition", release.PartitionName)
	assert.Equal(t, si.TerminationType_STOPPED_BY_RM, release.TerminationType)
	assert.Equal(t, "pod terminated", release.Message)
}
