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
	"sync/atomic"

	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type SchedulerAPIMock struct {
	registerCount          atomic.Int32
	UpdateAllocationCount  atomic.Int32
	UpdateApplicationCount atomic.Int32
	UpdateNodeCount        atomic.Int32
	registerFn             func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	UpdateAllocationFn  func(request *si.AllocationRequest) error
	UpdateApplicationFn func(request *si.ApplicationRequest) error
	UpdateNodeFn        func(request *si.NodeRequest) error
	lock                locking.Mutex
}

func NewSchedulerAPIMock() *SchedulerAPIMock {
	return &SchedulerAPIMock{
		registerFn: func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, nil
		},
		UpdateAllocationFn: func(request *si.AllocationRequest) error {
			return nil
		},
		UpdateApplicationFn: func(request *si.ApplicationRequest) error {
			return nil
		},
		UpdateNodeFn: func(request *si.NodeRequest) error {
			return nil
		},
		lock: locking.Mutex{},
	}
}

func (api *SchedulerAPIMock) RegisterFunction(rfn func(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)) *SchedulerAPIMock {
	api.registerFn = rfn
	return api
}

func (api *SchedulerAPIMock) UpdateAllocationFunction(ufn func(request *si.AllocationRequest) error) *SchedulerAPIMock {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.UpdateAllocationFn = ufn
	return api
}

func (api *SchedulerAPIMock) UpdateApplicationFunction(ufn func(request *si.ApplicationRequest) error) *SchedulerAPIMock {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.UpdateApplicationFn = ufn
	return api
}

func (api *SchedulerAPIMock) UpdateNodeFunction(ufn func(request *si.NodeRequest) error) *SchedulerAPIMock {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.UpdateNodeFn = ufn
	return api
}

func (api *SchedulerAPIMock) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.registerCount.Add(1)
	return api.registerFn(request, callback)
}

func (api *SchedulerAPIMock) UpdateAllocation(request *si.AllocationRequest) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.UpdateAllocationCount.Add(1)
	return api.UpdateAllocationFn(request)
}

func (api *SchedulerAPIMock) UpdateApplication(request *si.ApplicationRequest) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.UpdateApplicationCount.Add(1)
	return api.UpdateApplicationFn(request)
}

func (api *SchedulerAPIMock) UpdateNode(request *si.NodeRequest) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	api.UpdateNodeCount.Add(1)
	return api.UpdateNodeFn(request)
}

func (api *SchedulerAPIMock) UpdateConfiguration(request *si.UpdateConfigurationRequest) error {
	api.lock.Lock()
	defer api.lock.Unlock()
	return nil
}

func (api *SchedulerAPIMock) GetRegisterCount() int32 {
	return api.registerCount.Load()
}

func (api *SchedulerAPIMock) GetUpdateAllocationCount() int32 {
	return api.UpdateAllocationCount.Load()
}

func (api *SchedulerAPIMock) GetUpdateApplicationCount() int32 {
	return api.UpdateApplicationCount.Load()
}

func (api *SchedulerAPIMock) GetUpdateNodeCount() int32 {
	return api.UpdateNodeCount.Load()
}

func (api *SchedulerAPIMock) ResetAllCounters() {
	api.registerCount.Store(0)
	api.UpdateAllocationCount.Store(0)
	api.UpdateApplicationCount.Store(0)
	api.UpdateNodeCount.Store(0)
}

func (api *SchedulerAPIMock) Stop() {
}
