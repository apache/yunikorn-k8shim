package test

import (
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	v1 "k8s.io/api/core/v1"
)

type MockedRecoverableAppManager struct {
}

func NewMockedRecoverableAppManager() *MockedRecoverableAppManager {
	return &MockedRecoverableAppManager{}
}

func (m *MockedRecoverableAppManager) ListApplications() (map[string]interfaces.ApplicationMetadata, error) {
	return nil, nil
}

func (m *MockedRecoverableAppManager) GetExistingAllocation(pod *v1.Pod) (*si.Allocation, bool) {
	return &si.Allocation{
		AllocationKey:    pod.Name,
		AllocationTags:   nil,
		UUID:             string(pod.UID),
		ResourcePerAlloc: nil,
		Priority:         nil,
		QueueName:        "",
		NodeID:           pod.Spec.NodeName,
		ApplicationID:    "",
		PartitionName:    common.DefaultPartition,
	}, true
}
