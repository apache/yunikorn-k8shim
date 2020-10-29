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
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"

	v1 "k8s.io/api/core/v1"
	corev1 "k8s.io/client-go/listers/core/v1"
	storagev1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client/clientset/versioned/fake"
)

type MockedAPIProvider struct {
	clients *Clients
}

func NewMockedAPIProvider() *MockedAPIProvider {
	return &MockedAPIProvider{
		clients: &Clients{
			Conf: &conf.SchedulerConf{
				ClusterID:            "yk-test-cluster",
				ClusterVersion:       "0.1",
				PolicyGroup:          "queues",
				Interval:             0,
				KubeConfig:           "",
				LoggingLevel:         0,
				LogEncoding:          "",
				LogFile:              "",
				VolumeBindTimeout:    0,
				TestMode:             true,
				EventChannelCapacity: 0,
				DispatchTimeout:      0,
				KubeQPS:              0,
				KubeBurst:            0,
				Predicates:           "",
			},
			KubeClient:        NewKubeClientMock(),
			SchedulerAPI:      test.NewSchedulerAPIMock(),
			AppClient:         fake.NewSimpleClientset(),
			PodInformer:       nil,
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

func (m *MockedAPIProvider) MockSchedulerApiUpdateFn(ufn func(request *si.UpdateRequest) error) {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		mock.UpdateFunction(ufn)
	}
}

func (m *MockedAPIProvider) GetSchedulerApiUpdateCount() int32 {
	if mock, ok := m.clients.SchedulerAPI.(*test.SchedulerAPIMock); ok {
		return mock.GetUpdateCount()
	}
	return int32(0)
}

func (m *MockedAPIProvider) GetSchedulerApiRegisterCount() int32 {
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

func (m *MockedAPIProvider) SetNodeLister(lister corev1.NodeLister) {
	informer := m.clients.NodeInformer
	if i, ok := informer.(*test.MockedNodeInformer); ok {
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
