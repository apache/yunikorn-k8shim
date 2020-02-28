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

	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
)

type Type int

const (
	PodInformerHandlers Type = iota
	NodeInformerHandlers
	ConfigMapInformerHandlers
	StorageInformerHandlers
	PVInformerHandlers
	PVCInformerHandlers
)

type APIProvider interface {
	GetAPIs() *Clients
	AddEventHandler(handlers *ResourceEventHandlers)
	Start()
	Stop()
	WaitForSync() error
	IsTestingMode() bool
}

// resource handlers defines add/update/delete operations in response to the corresponding resources updates.
// The associated the type field points the handler functions to the correct receiver.
type ResourceEventHandlers struct {
	Type
	FilterFn func(obj interface{}) bool
	AddFn    func(obj interface{})
	UpdateFn func(old, new interface{})
	DeleteFn func(obj interface{})
}

// API factory maintains shared clients which can be used to access other external components
// e.g K8s api-server, or scheduler-core.
type APIFactory struct {
	clients  *Clients
	testMode bool
	stopChan chan struct{}
	lock     *sync.RWMutex
}

func NewAPIFactory(scheduler api.SchedulerAPI, configs *conf.SchedulerConf, testMode bool) *APIFactory {
	kubeClient := NewKubeClient(configs.KubeConfig)

	// we have disabled re-sync to keep ourselves up-to-date
	informerFactory := informers.NewSharedInformerFactory(kubeClient.GetClientSet(), 0)

	// init informers
	// volume informers are also used to get the Listers for the predicates
	nodeInformer := informerFactory.Core().V1().Nodes()
	podInformer := informerFactory.Core().V1().Pods()
	configMapInformer := informerFactory.Core().V1().ConfigMaps()
	storageInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	// create a volume binder (needs the informers)
	volumeBinder := volumebinder.NewVolumeBinder(
		kubeClient.GetClientSet(),
		nodeInformer, pvcInformer,
		pvInformer,
		storageInformer,
		configs.VolumeBindTimeout)

	return &APIFactory{
		clients: &Clients{
			Conf:              configs,
			KubeClient:        kubeClient,
			SchedulerAPI:      scheduler,
			InformerFactory:   informerFactory,
			PodInformer:       podInformer,
			NodeInformer:      nodeInformer,
			ConfigMapInformer: configMapInformer,
			PVInformer:        pvInformer,
			PVCInformer:       pvcInformer,
			StorageInformer:   storageInformer,
			VolumeBinder:      volumeBinder,
		},
		testMode: testMode,
		stopChan: make(chan struct{}),
		lock:     &sync.RWMutex{},
	}
}

func (s *APIFactory) GetAPIs() *Clients {
	return s.clients
}

func (s *APIFactory) IsTestingMode() bool {
	return s.testMode
}

func (s *APIFactory) AddEventHandler(handlers *ResourceEventHandlers) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// register all handlers
	var h cache.ResourceEventHandler
	fns := cache.ResourceEventHandlerFuncs{
		AddFunc:    handlers.AddFn,
		UpdateFunc: handlers.UpdateFn,
		DeleteFunc: handlers.DeleteFn,
	}

	// if filter function exists
	// add a wrapper
	if handlers.FilterFn != nil {
		h = cache.FilteringResourceEventHandler{
			FilterFunc: handlers.FilterFn,
			Handler:    fns,
		}
	} else {
		h = fns
	}

	s.addEventHandlers(handlers.Type, h, 0)
}

func (s *APIFactory) addEventHandlers(
	handlerType Type, handler cache.ResourceEventHandler, resyncPeriod time.Duration) {
	switch handlerType {
	case PodInformerHandlers:
		s.GetAPIs().PodInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case NodeInformerHandlers:
		s.GetAPIs().NodeInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case ConfigMapInformerHandlers:
		s.GetAPIs().ConfigMapInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case StorageInformerHandlers:
		s.GetAPIs().StorageInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PVInformerHandlers:
		s.GetAPIs().PVInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PVCInformerHandlers:
		s.GetAPIs().PVCInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	}
}

func (s *APIFactory) WaitForSync() error {
	if s.testMode {
		// skip this in test mode
		return nil
	}
	return s.clients.WaitForSync(time.Second, 30*time.Second)
}

func (s *APIFactory) Start() {
	// launch clients
	if !s.IsTestingMode() {
		s.clients.Run(s.stopChan)
	}
}

func (s *APIFactory) Stop() {
	if !s.IsTestingMode() {
		close(s.stopChan)
	}
}
