/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"sync"
	"sync/atomic"
)

type SchedulerApiMock struct {
	registerCount int32
	updateCount   int32
	registerFn    func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	updateFn      func(request *si.UpdateRequest) error
	lock          sync.Mutex
}

func NewSchedulerApiMock() *SchedulerApiMock {
	return &SchedulerApiMock{
		registerCount: int32(0),
		updateCount:   int32(0),
		registerFn: func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, nil
		},
		updateFn: func(request *si.UpdateRequest) error {
			return nil
		},
		lock: sync.Mutex{},
	}
}

func (api *SchedulerApiMock) RegisterFunction(rfn func(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)) *SchedulerApiMock {
		api.registerFn = rfn
		return api
}

func (api *SchedulerApiMock) UpdateFunction(ufn func(request *si.UpdateRequest) error) *SchedulerApiMock {
	api.updateFn = ufn
	return api
}

func (api *SchedulerApiMock) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
		api.lock.Lock()
		defer api.lock.Unlock()
		atomic.AddInt32(&api.registerCount, 1)
		return api.registerFn(request, callback)
}

func (api *SchedulerApiMock) Update(request *si.UpdateRequest) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	atomic.AddInt32(&api.updateCount, 1)
	return api.updateFn(request)
}

func (api *SchedulerApiMock) ReloadConfiguration(rmId string) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	return nil
}

func (api *SchedulerApiMock) GetRegisterCount() int32 {
	return atomic.LoadInt32(&api.registerCount)
}

func (api *SchedulerApiMock) GetUpdateCount() int32 {
	return atomic.LoadInt32(&api.updateCount)
}