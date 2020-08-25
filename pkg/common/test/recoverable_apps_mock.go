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

package test

import (
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type MockedRecoverableAppManager struct {
}

func NewMockedRecoverableAppManager() *MockedRecoverableAppManager {
	return &MockedRecoverableAppManager{}
}

func (m *MockedRecoverableAppManager) ListApplications() (map[string]interfaces.ApplicationMetadata, error) {
	return nil, nil
}

func (m *MockedRecoverableAppManager) GetExistingAllocation(pod *v1.Pod) *si.Allocation {
	return &si.Allocation{
		AllocationKey:    pod.Name,
		AllocationTags:   nil,
		UUID:             string(pod.UID),
		ResourcePerAlloc: nil,
		Priority:         nil,
		QueueName:        "",
		NodeID:           pod.Spec.NodeName,
		ApplicationID:    "",
		PartitionName:    constants.DefaultPartition,
	}
}
