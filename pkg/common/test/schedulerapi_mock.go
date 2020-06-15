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
	"sync"
	"sync/atomic"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type SchedulerAPIMock struct {
	registerCount int32
	updateCount   int32
	registerFn    func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	updateFn func(request *si.UpdateRequest) error
	lock     sync.Mutex
}

func NewSchedulerAPIMock() *SchedulerAPIMock {
	return &SchedulerAPIMock{
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

func (api *SchedulerAPIMock) RegisterFunction(rfn func(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)) *SchedulerAPIMock {
	api.registerFn = rfn
	return api
}

func (api *SchedulerAPIMock) UpdateFunction(ufn func(request *si.UpdateRequest) error) *SchedulerAPIMock {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.updateFn = ufn
	return api
}

func (api *SchedulerAPIMock) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
	api.lock.Lock()
	defer api.lock.Unlock()
	atomic.AddInt32(&api.registerCount, 1)
	return api.registerFn(request, callback)
}

func (api *SchedulerAPIMock) Update(request *si.UpdateRequest) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	atomic.AddInt32(&api.updateCount, 1)
	return api.updateFn(request)
}

func (api *SchedulerAPIMock) ReloadConfiguration(rmID string) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	return nil
}

func (api *SchedulerAPIMock) GetRegisterCount() int32 {
	return atomic.LoadInt32(&api.registerCount)
}

func (api *SchedulerAPIMock) GetUpdateCount() int32 {
	return atomic.LoadInt32(&api.updateCount)
}

func (api *SchedulerAPIMock) ResetAllCounters() {
	atomic.StoreInt32(&api.registerCount, 0)
	atomic.StoreInt32(&api.updateCount, 0)
}
