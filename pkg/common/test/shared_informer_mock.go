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
	ctx "context"
	"sync/atomic"
	"time"

	"k8s.io/client-go/tools/cache"
)

func init() {
	SyncDone.Store(true) // shouldn't block by default
}

var (
	SyncDone         atomic.Bool
	RunningInformers atomic.Int64

	_ cache.SharedIndexInformer = &SharedInformerMock{}
)

type SharedInformerMock struct {
}

func (s *SharedInformerMock) AddEventHandlerWithOptions(_ cache.ResourceEventHandler, _ cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *SharedInformerMock) RunWithContext(_ ctx.Context) {
}

func (s *SharedInformerMock) SetWatchErrorHandlerWithContext(_ cache.WatchErrorHandlerWithContext) error {
	return nil
}

func (s *SharedInformerMock) AddEventHandler(_ cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *SharedInformerMock) AddEventHandlerWithResyncPeriod(_ cache.ResourceEventHandler, _ time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *SharedInformerMock) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (s *SharedInformerMock) GetStore() cache.Store {
	return nil
}

func (s *SharedInformerMock) GetController() cache.Controller {
	return nil
}

func (s *SharedInformerMock) Run(stopCh <-chan struct{}) {
	RunningInformers.Add(1)
	<-stopCh
	RunningInformers.Add(-1)
}

func (s *SharedInformerMock) HasSynced() bool {
	return SyncDone.Load()
}

func (s *SharedInformerMock) LastSyncResourceVersion() string {
	return ""
}

func (s *SharedInformerMock) SetWatchErrorHandler(_ cache.WatchErrorHandler) error {
	return nil
}

func (s *SharedInformerMock) SetTransform(_ cache.TransformFunc) error {
	return nil
}

func (s *SharedInformerMock) IsStopped() bool {
	return false
}

func (s *SharedInformerMock) AddIndexers(_ cache.Indexers) error {
	return nil
}

func (s *SharedInformerMock) GetIndexer() cache.Indexer {
	return nil
}
