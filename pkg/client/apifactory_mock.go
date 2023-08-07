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
	"sync"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	schedv1 "k8s.io/api/scheduling/v1"
	"k8s.io/client-go/informers"
	k8fake "k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/listers/core/v1"
	storagev1 "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type MockedAPIProvider struct {
	sync.Mutex
	clients *Clients

	stop         chan struct{}
	eventHandler chan *ResourceEventHandlers
	events       chan informerEvent
	running      bool
}

type operation int

type informerEvent struct {
	handlerType Type
	op          operation
	obj         interface{}
	oldObj      interface{}
}

const (
	Add operation = iota
	Update
	Delete
)

func NewMockedAPIProvider(showError bool) *MockedAPIProvider {
	return &MockedAPIProvider{
		clients: &Clients{
			conf: &conf.SchedulerConf{
				ClusterID:            "yk-test-cluster",
				ClusterVersion:       "0.1",
				PolicyGroup:          "queues",
				Interval:             0,
				KubeConfig:           "",
				VolumeBindTimeout:    0,
				TestMode:             true,
				EventChannelCapacity: 0,
				DispatchTimeout:      0,
				KubeQPS:              0,
				KubeBurst:            0,
				Namespace:            "yunikorn",
			},
			KubeClient:            NewKubeClientMock(showError),
			SchedulerAPI:          test.NewSchedulerAPIMock(),
			PodInformer:           test.NewMockedPodInformer(),
			NodeInformer:          test.NewMockedNodeInformer(),
			ConfigMapInformer:     test.NewMockedConfigMapInformer(),
			PVInformer:            &MockedPersistentVolumeInformer{},
			PVCInformer:           &MockedPersistentVolumeClaimInformer{},
			StorageInformer:       &MockedStorageClassInformer{},
			VolumeBinder:          nil,
			NamespaceInformer:     test.NewMockNamespaceInformer(false),
			PriorityClassInformer: test.NewMockPriorityClassInformer(),
			InformerFactory:       informers.NewSharedInformerFactory(k8fake.NewSimpleClientset(), time.Second*60),
		},
		events:       make(chan informerEvent),
		eventHandler: make(chan *ResourceEventHandlers),
		stop:         make(chan struct{}),
		running:      false,
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

func (m *MockedAPIProvider) GetPodListerMock() *test.PodListerMock {
	if informer, ok := m.clients.PodInformer.(*test.MockedPodInformer); ok {
		if lister, ok := informer.Lister().(*test.PodListerMock); ok {
			return lister
		}
		return nil
	}
	return nil
}

func (m *MockedAPIProvider) GetNodeListerMock() *test.NodeListerMock {
	if informer, ok := m.clients.NodeInformer.(*test.MockedNodeInformer); ok {
		if lister, ok := informer.Lister().(*test.NodeListerMock); ok {
			return lister
		}
		return nil
	}
	return nil
}

func (m *MockedAPIProvider) GetAPIs() *Clients {
	return m.clients
}

func (m *MockedAPIProvider) IsTestingMode() bool {
	return true
}

func (m *MockedAPIProvider) AddEventHandler(handlers *ResourceEventHandlers) {
	m.Lock()
	defer m.Unlock()

	if !m.running {
		return
	}

	m.eventHandler <- handlers
	log.Log(log.Test).Info("registering event handler", zap.Stringer("type", handlers.Type))
}

func (m *MockedAPIProvider) RunEventHandler() {
	m.Lock()
	defer m.Unlock()
	if m.running {
		return
	}
	m.running = true
	log.Log(log.Test).Info("mock shared informers: starting background event handler")
	go func() {
		eventHandlers := make(map[Type][]cache.ResourceEventHandler)

		for {
			select {
			case <-m.stop:
				return
			case handlers := <-m.eventHandler:
				var h cache.ResourceEventHandler
				fns := cache.ResourceEventHandlerFuncs{
					AddFunc:    handlers.AddFn,
					UpdateFunc: handlers.UpdateFn,
					DeleteFunc: handlers.DeleteFn,
				}

				if handlers.FilterFn != nil {
					h = cache.FilteringResourceEventHandler{
						FilterFunc: handlers.FilterFn,
						Handler:    fns,
					}
				} else {
					h = fns
				}
				handlerType := handlers.Type

				forType := eventHandlers[handlerType]
				forType = append(forType, h)
				eventHandlers[handlerType] = forType
			case event := <-m.events:
				handlerType := event.handlerType
				obj := event.obj
				old := event.oldObj

				switch event.op {
				case Add:
					for _, e := range eventHandlers[handlerType] {
						e.OnAdd(obj, false)
					}
				case Delete:
					for _, e := range eventHandlers[handlerType] {
						e.OnDelete(obj)
					}
				case Update:
					for _, e := range eventHandlers[handlerType] {
						e.OnUpdate(old, obj)
					}
				default:
					log.Log(log.Test).Fatal("Unknown operation", zap.Int("type", int(event.op)))
				}
			}
		}
	}()
}

func (m *MockedAPIProvider) Start() {
	// no impl
}

func (m *MockedAPIProvider) Stop() {
	m.Lock()
	defer m.Unlock()
	close(m.stop)
	m.running = false
}

func (m *MockedAPIProvider) WaitForSync() {
	// no impl
}

func (m *MockedAPIProvider) AddPod(obj *v1.Pod) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Add,
		handlerType: PodInformerHandlers,
	}
}

func (m *MockedAPIProvider) UpdatePod(oldObj *v1.Pod, newObj *v1.Pod) {
	m.events <- informerEvent{
		obj:         newObj,
		oldObj:      oldObj,
		op:          Update,
		handlerType: PodInformerHandlers,
	}
}

func (m *MockedAPIProvider) DeletePod(obj *v1.Pod) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Delete,
		handlerType: PodInformerHandlers,
	}
}

func (m *MockedAPIProvider) AddNode(obj *v1.Node) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Add,
		handlerType: NodeInformerHandlers,
	}
}

func (m *MockedAPIProvider) DeleteNode(obj *v1.Node) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Delete,
		handlerType: NodeInformerHandlers,
	}
}

func (m *MockedAPIProvider) UpdateNode(oldObj *v1.Node, newObj *v1.Node) {
	m.events <- informerEvent{
		obj:         newObj,
		oldObj:      oldObj,
		op:          Update,
		handlerType: NodeInformerHandlers,
	}
}

func (m *MockedAPIProvider) AddConfigMap(obj *v1.ConfigMap) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Add,
		handlerType: ConfigMapInformerHandlers,
	}
}

func (m *MockedAPIProvider) DeleteConfigMap(obj *v1.ConfigMap) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Delete,
		handlerType: ConfigMapInformerHandlers,
	}
}

func (m *MockedAPIProvider) UpdateConfigMap(oldObj *v1.ConfigMap, newObj *v1.ConfigMap) {
	m.events <- informerEvent{
		obj:         newObj,
		oldObj:      oldObj,
		op:          Update,
		handlerType: NodeInformerHandlers,
	}
}

func (m *MockedAPIProvider) AddPriorityClass(obj *schedv1.PriorityClass) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Add,
		handlerType: PriorityClassInformerHandlers,
	}
}

func (m *MockedAPIProvider) DeletePriorityClass(obj *schedv1.PriorityClass) {
	m.events <- informerEvent{
		obj:         obj,
		op:          Delete,
		handlerType: PriorityClassInformerHandlers,
	}
}

func (m *MockedAPIProvider) UpdatePriorityClass(oldObj *schedv1.PriorityClass, newObj *schedv1.PriorityClass) {
	m.events <- informerEvent{
		obj:         newObj,
		oldObj:      oldObj,
		op:          Update,
		handlerType: PriorityClassInformerHandlers,
	}
}

func (m *MockedAPIProvider) GetPodBindStats() BindStats {
	return m.clients.KubeClient.(*KubeClientMock).GetBindStats()
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
