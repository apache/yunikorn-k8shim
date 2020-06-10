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
)

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
