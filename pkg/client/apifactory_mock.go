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

package client

import (
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"

	v1 "k8s.io/api/core/v1"
	corev1 "k8s.io/client-go/listers/core/v1"
	storagev1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/client/clientset/versioned/fake"
)

type MockedAPIProvider struct {
	clients *Clients
}

func NewMockedAPIProvider(showError bool) *MockedAPIProvider {
	return &MockedAPIProvider{
		clients: &Clients{
			conf: &conf.SchedulerConf{
				ClusterID:            "yk-test-cluster",
				ClusterVersion:       "0.1",
				PolicyGroup:          "queues",
				Interval:             0,
				KubeConfig:           "",
				LoggingLevel:         0,
				VolumeBindTimeout:    0,
				TestMode:             true,
				EventChannelCapacity: 0,
				DispatchTimeout:      0,
				KubeQPS:              0,
				KubeBurst:            0,
				Namespace:            "yunikorn",
			},
			KubeClient:        NewKubeClientMock(showError),
			SchedulerAPI:      test.NewSchedulerAPIMock(),
			AppClient:         fake.NewSimpleClientset(),
			PodInformer:       test.NewMockedPodInformer(),
			NodeInformer:      test.NewMockedNodeInformer(),
			ConfigMapInformer: test.NewMockedConfigMapInformer(),
			PVInformer:        &MockedPersistentVolumeInformer{},
			PVCInformer:       &MockedPersistentVolumeClaimInformer{},
			StorageInformer:   &MockedStorageClassInformer{},
			VolumeBinder:      nil,
			AppInformer:       test.NewAppInformerMock(),
			NamespaceInformer: test.NewMockNamespaceInformer(),
		},
	}
}

func (m *MockedAPIProvider) MockSchedulerAPIUpdateAllocationFn(ufn func(request *si.AllocationRequest) error) {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		mock.UpdateAllocationFunction(ufn)
	}
}

func (m *MockedAPIProvider) GetSchedulerAPIUpdateAllocationCount() int32 {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		return mock.GetUpdateAllocationCount()
	}
	return int32(0)
}

func (m *MockedAPIProvider) MockSchedulerAPIUpdateApplicationFn(ufn func(request *si.ApplicationRequest) error) {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		mock.UpdateApplicationFunction(ufn)
	}
}

func (m *MockedAPIProvider) GetSchedulerAPIUpdateApplicationCount() int32 {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		return mock.GetUpdateApplicationCount()
	}
	return int32(0)
}

func (m *MockedAPIProvider) MockSchedulerAPIUpdateNodeFn(ufn func(request *si.NodeRequest) error) {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		mock.UpdateNodeFunction(ufn)
	}
}

func (m *MockedAPIProvider) GetSchedulerAPIUpdateNodeCount() int32 {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		return mock.GetUpdateNodeCount()
	}
	return int32(0)
}

func (m *MockedAPIProvider) GetSchedulerAPIRegisterCount() int32 {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		return mock.GetRegisterCount()
	}
	return int32(0)
}

func (m *MockedAPIProvider) MockBindFn(bfn func(pod *v1.Pod, hostID string) error) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.bindFn = bfn
	}
}

func (m *MockedAPIProvider) MockDeleteFn(dfn func(pod *v1.Pod) error) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.deleteFn = dfn
	}
}

func (m *MockedAPIProvider) MockCreateFn(cfn func(pod *v1.Pod) (*v1.Pod, error)) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.createFn = cfn
	}
}

func (m *MockedAPIProvider) MockUpdateStatusFn(cfn func(pod *v1.Pod) (*v1.Pod, error)) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.updateStatusFn = cfn
	}
}

func (m *MockedAPIProvider) MockGetFn(cfn func(podName string) (*v1.Pod, error)) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.getFn = cfn
	}
}

func (m *MockedAPIProvider) SetNodeLister(lister corev1.NodeLister) {
	informer := m.clients.NodeInformer
	if i, ok := informer.(*test.MockedNodeInformer); ok {
		i.SetLister(lister)
	}
}

func (m *MockedAPIProvider) SetPodLister(lister corev1.PodLister) {
	informer := m.clients.PodInformer
	if i, ok := informer.(*test.MockedPodInformer); ok {
		i.SetLister(lister)
	}
}

func (m *MockedAPIProvider) GetAPIs() *Clients {
	return m.clients
}

func (m *MockedAPIProvider) IsTestingMode() bool {
	return true
}

func (m *MockedAPIProvider) AddEventHandler(handlers *ResourceEventHandlers) {
	// no impl
}

func (m *MockedAPIProvider) Start() {
	// no impl
}

func (m *MockedAPIProvider) Stop() {
	// no impl
}

func (m *MockedAPIProvider) WaitForSync() error {
	return nil
}

// MockedPersistentVolumeInformer implements PersistentVolumeInformer interface
type MockedPersistentVolumeInformer struct{}

func (m *MockedPersistentVolumeInformer) Informer() cache.SharedIndexInformer {
	return nil
}

func (m *MockedPersistentVolumeInformer) Lister() corev1.PersistentVolumeLister {
	return nil
}

// MockedPersistentVolumeClaimInformer implements PersistentVolumeClaimInformer interface
type MockedPersistentVolumeClaimInformer struct{}

func (m *MockedPersistentVolumeClaimInformer) Informer() cache.SharedIndexInformer {
	return nil
}

func (m *MockedPersistentVolumeClaimInformer) Lister() corev1.PersistentVolumeClaimLister {
	return nil
}

// MockedStorageClassInformer implements StorageClassInformer interface
type MockedStorageClassInformer struct{}

func (m *MockedStorageClassInformer) Informer() cache.SharedIndexInformer {
	return nil
}

func (m *MockedStorageClassInformer) Lister() storagev1.StorageClassLister {
	return nil
}
