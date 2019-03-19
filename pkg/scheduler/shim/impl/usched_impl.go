/*
Copyright 2019 The Unity Scheduler Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package impl

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
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


