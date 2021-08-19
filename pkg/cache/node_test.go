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
	"reflect"
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func TestAddExistingAllocation(t *testing.T) {
	node := NewTestSchedulerNode()
	alloc01 := si.Allocation{
		AllocationKey:    "pod001",
		AllocationTags:   nil,
		UUID:             "podUID001",
		ResourcePerAlloc: nil,
		Priority:         nil,
		QueueName:        "",
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

func TestSetOccupiedResource(t *testing.T) {
	node := NewTestSchedulerNode()
	r1 := common.NewResourceBuilder().
		AddResource(constants.Memory, 2).
		AddResource(constants.CPU, 2).
		Build()
	node.setOccupiedResource(r1)
	assert.Equal(t, node.occupied, r1)
}

func NewTestSchedulerNode() *SchedulerNode {
	return NewTestSchedulerNodeWithLabels(nil)
}

func NewTestSchedulerNodeWithLabels(label map[string]string) *SchedulerNode {
	api := test.NewSchedulerAPIMock()
	r1 := common.NewResourceBuilder().
		AddResource(constants.Memory, 1).
		AddResource(constants.CPU, 1).
		Build()
	node := newSchedulerNode("host001", "UID001", r1, api, false, label)
	return node
}

func TestAttributes(t *testing.T) {
	labels := map[string]string{
		"a": "b",
		"k": "v",
	}
	node := NewTestSchedulerNodeWithLabels(labels)

	assert.Assert(t, reflect.DeepEqual(labels, node.labels))

	attributes := node.toAttributes()
	for k, v := range labels {
		assert.Equal(t, v, attributes[k])
	}
}
