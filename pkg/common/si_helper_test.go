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

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].Allocationkey, "task01")
	assert.Equal(t, request.Releases.AllocationAsksToRelease[0].PartitionName, "default")
}

func TestCreateUpdateRequestForRemoveApplication(t *testing.T) {
	request := CreateUpdateRequestForRemoveApplication("app01", "default")
	assert.Assert(t, request.RemoveApplications != nil)
	assert.Equal(t, len(request.RemoveApplications), 1)
	assert.Equal(t, request.RemoveApplications[0].ApplicationID, "app01")
	assert.Equal(t, request.RemoveApplications[0].PartitionName, "default")
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

	updateRequest := CreateUpdateRequestForTask("appId1", "taskId1", res, false, "", pod)
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
