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
	"strconv"
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const nodeID = "node-01"
const nodeLabels = "{\"label1\":\"key1\",\"label2\":\"key2\"}"

func TestCreateReleaseAllocationRequest(t *testing.T) {
	request := CreateReleaseAllocationRequestForTask("app01", "alloc01", "default", "STOPPED_BY_RM")
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
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].AllocationKey, "task01")
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].PartitionName, "default")
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

	updateRequest := CreateAllocationRequestForTask("appId1", "taskId1", res, false, "", pod, false)
	asks := updateRequest.Asks
	assert.Equal(t, len(asks), 1)
	allocAsk := asks[0]
	assert.Assert(t, allocAsk != nil)
	tags := allocAsk.Tags
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

func TestCreateUpdateRequestForNewNode(t *testing.T) {
	capacity := NewResourceBuilder().AddResource(common.Memory, 200).AddResource(common.CPU, 2).Build()
	occupied := NewResourceBuilder().AddResource(common.Memory, 50).AddResource(common.CPU, 1).Build()
	var existingAllocations []*si.Allocation
	ready := true
	request := CreateUpdateRequestForNewNode(nodeID, nodeLabels, capacity, occupied, existingAllocations, ready)
	assert.Equal(t, len(request.Nodes), 1)
	assert.Equal(t, request.Nodes[0].NodeID, nodeID)
	assert.Equal(t, request.Nodes[0].SchedulableResource, capacity)
	assert.Equal(t, request.Nodes[0].OccupiedResource, occupied)
	assert.Equal(t, len(request.Nodes[0].Attributes), 4)
	assert.Equal(t, request.Nodes[0].Attributes[constants.DefaultNodeAttributeHostNameKey], nodeID)
	assert.Equal(t, request.Nodes[0].Attributes[constants.DefaultNodeAttributeRackNameKey], constants.DefaultRackName)
	assert.Equal(t, request.Nodes[0].Attributes[constants.DefaultNodeAttributeNodeLabelsKey], nodeLabels)
	assert.Equal(t, request.Nodes[0].Attributes[common.NodeReadyAttribute], strconv.FormatBool(ready))
}

func TestCreateUpdateRequestForUpdatedNode(t *testing.T) {
	capacity := NewResourceBuilder().AddResource(common.Memory, 200).AddResource(common.CPU, 2).Build()
	occupied := NewResourceBuilder().AddResource(common.Memory, 50).AddResource(common.CPU, 1).Build()
	ready := true
	request := CreateUpdateRequestForUpdatedNode(nodeID, capacity, occupied, ready)
	assert.Equal(t, len(request.Nodes), 1)
	assert.Equal(t, request.Nodes[0].NodeID, nodeID)
	assert.Equal(t, request.Nodes[0].SchedulableResource, capacity)
	assert.Equal(t, request.Nodes[0].OccupiedResource, occupied)
	assert.Equal(t, len(request.Nodes[0].Attributes), 1)
	assert.Equal(t, request.Nodes[0].Attributes[common.NodeReadyAttribute], strconv.FormatBool(ready))
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

	updateRequest := CreateAllocationRequestForTask("appId1", "taskId1", res, false, "", pod, false)
	asks := updateRequest.Asks
	assert.Equal(t, len(asks), 1)
	allocAsk := asks[0]
	if allocAsk == nil {
		t.Fatal("ask cannot be nil")
	}
	assert.Equal(t, allocAsk.Priority, int32(0))

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

	updateRequest1 := CreateAllocationRequestForTask("appId1", "taskId1", res, false, "", pod1, false)
	asks1 := updateRequest1.Asks
	assert.Equal(t, len(asks1), 1)
	allocAsk1 := asks1[0]
	if allocAsk1 == nil {
		t.Fatal("ask cannot be nil")
	}
	tags := allocAsk1.Tags
	assert.Equal(t, tags[common.DomainK8s+common.GroupMeta+"podName"], podName1)
	assert.Equal(t, allocAsk1.Priority, int32(100))
}
