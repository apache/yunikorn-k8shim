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

package cache

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAddExistingAllocation(t *testing.T) {
	node := NewTestSchedulerNode()
	alloc01 := si.Allocation{
		AllocationKey:    "pod001",
		AllocationTags:   nil,
		UUID:             "podUID001",
		ResourcePerAlloc: nil,
		Priority:         0,
		NodeID:           "host001",
		ApplicationID:    "",
		PartitionName:    constants.DefaultPartition,
	}
	node.addExistingAllocation(&alloc01)
	assert.Equal(t, len(node.existingAllocations), 1)
	alloc02 := node.existingAllocations[0]
	assert.Equal(t, alloc02.AllocationKey, alloc01.AllocationKey)
	assert.Equal(t, alloc02.UUID, alloc01.UUID)
	assert.Equal(t, alloc02.NodeID, alloc01.NodeID)
	assert.Equal(t, alloc02.PartitionName, alloc01.PartitionName)
}

func TestUpdateOccupiedResource(t *testing.T) {
	node := NewTestSchedulerNode()
	r1 := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 5).
		AddResource(siCommon.CPU, 5).
		Build()
	r2 := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	r3 := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 4).
		AddResource(siCommon.CPU, 4).
		Build()

	capacity, occupied, ready := node.updateOccupiedResource(r1, AddOccupiedResource)
	assert.DeepEqual(t, capacity, r2)
	assert.DeepEqual(t, occupied, r1)
	assert.Assert(t, ready)

	capacity, occupied, ready = node.updateOccupiedResource(r2, SubOccupiedResource)
	assert.DeepEqual(t, capacity, r2)
	assert.DeepEqual(t, occupied, r3)
	assert.Assert(t, ready)
}

func NewTestSchedulerNode() *SchedulerNode {
	api := test.NewSchedulerAPIMock()
	r1 := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1).
		AddResource(siCommon.CPU, 1).
		Build()
	node := newSchedulerNode("host001", "UID001", "{\"label1\":\"key1\",\"label2\":\"key2\"}", r1, api, false, true)
	return node
}
