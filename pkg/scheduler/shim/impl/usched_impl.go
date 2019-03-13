package impl

import (
	"github.com/golang/glog"
	"github.com/universal-scheduler/k8s-shim/pkg/common"
	"github.com/universal-scheduler/scheduler-spec/lib/go/si"
	"github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/api"
)

//
// TODO replace this with real scheduler impl
//
type MockScheduler struct {
	SchedulerName string
}

func NewMockScheduler() *MockScheduler {
	return &MockScheduler{common.SchedulerName}
}

func (scheduler *MockScheduler) Update(request *si.UpdateRequest) error {
	glog.V(4).Infof("Received request %s", request.String())
	return nil
}

func (scheduler *MockScheduler) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
		glog.V(3).Infof("Received registration")
		glog.V(3).Infof("RM: %s", request.RmId)
		glog.V(3).Infof("Version: %s", request.Version)
		return &(si.RegisterResourceManagerResponse{}), nil
}


