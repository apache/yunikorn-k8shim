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
	"fmt"
	"time"

	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

type APIProvider interface {
	GetAPIs() *Clients
	AddEventHandler(handlers *ResourceEventHandlers) error
	Start()
	Stop()
	WaitForSync()
	IsTestingMode() bool
}

// resource handlers defines add/update/delete operations in response to the corresponding resources updates.
// The associated the type field points the handler functions to the correct receiver.
type ResourceEventHandlers struct {
	InformerType
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
	lock     *locking.RWMutex
}

func NewAPIFactory(scheduler api.SchedulerAPI, informerFactory informers.SharedInformerFactory, configs *conf.SchedulerConf, testMode bool) *APIFactory {
	kubeClient := NewKubeClient(configs.KubeConfig)

	return &APIFactory{
		clients:  NewClients(scheduler, informerFactory, configs, kubeClient),
		testMode: testMode,
		stopChan: make(chan struct{}),
		lock:     &locking.RWMutex{},
	}
}

func (s *APIFactory) GetAPIs() *Clients {
	return s.clients
}

func (s *APIFactory) IsTestingMode() bool {
	return s.testMode
}

func (s *APIFactory) AddEventHandler(handlers *ResourceEventHandlers) error {
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

	log.Log(log.ShimClient).Info("registering event handler", zap.Stringer("type", handlers.InformerType))
	if err := s.addEventHandlers(handlers.InformerType, h, 0); err != nil {
		return fmt.Errorf("failed to initialize event handlers: %w", err)
	}
	return nil
}

func (s *APIFactory) addEventHandlers(
	handlerType InformerType, handler cache.ResourceEventHandler, resyncPeriod time.Duration) error {
	var err error
	switch handlerType {
	case PodInformerHandlers:
		_, err = s.GetAPIs().PodInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case NodeInformerHandlers:
		_, err = s.GetAPIs().NodeInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case ConfigMapInformerHandlers:
		_, err = s.GetAPIs().ConfigMapInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case StorageInformerHandlers:
		_, err = s.GetAPIs().StorageInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PVInformerHandlers:
		_, err = s.GetAPIs().PVInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PVCInformerHandlers:
		_, err = s.GetAPIs().PVCInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PriorityClassInformerHandlers:
		_, err = s.GetAPIs().PriorityClassInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	}

	if err != nil {
		return fmt.Errorf("failed to add event handlers: %w", err)
	}
	return nil
}

func (s *APIFactory) WaitForSync() {
	if s.testMode {
		// skip this in test mode
		return
	}
	s.clients.WaitForSync()
}

func (s *APIFactory) Start() {
	// launch clients
	if !s.IsTestingMode() {
		s.clients.Run(s.stopChan)
		s.clients.WaitForSync()
	}
}

func (s *APIFactory) Stop() {
	if !s.IsTestingMode() {
		close(s.stopChan)
	}
}
