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

package test

import (
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
)

type FakeSchedulerApi struct {
	RegisterCount int
	UpdateCount int
	RegisterFn func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	UpdateFn func(request *si.UpdateRequest) error
}

func (api *FakeSchedulerApi) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
		api.RegisterCount++
		return api.RegisterFn(request, callback)
}

func (api *FakeSchedulerApi) Update(request *si.UpdateRequest) error {
	api.UpdateCount++
	return api.UpdateFn(request)
}
