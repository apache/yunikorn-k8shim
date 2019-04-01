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
