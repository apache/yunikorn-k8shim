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

	apiresourcev1 "k8s.io/api/resource/v1"
	v1 "k8s.io/client-go/informers/resource/v1"
	"k8s.io/client-go/tools/cache"
)

func init() {
	SyncDone.Store(true) // shouldn't block by default
}

var (
	SyncDone         atomic.Bool
	RunningInformers atomic.Int64

	_ cache.SharedIndexInformer = &SharedInformerMock{}

	_ cache.TypedSharedIndexInformer[*apiresourcev1.ResourceSlice]   = &TypedSharedIndexInformerMock{}
	_ cache.TypedSharedIndexInformer[*apiresourcev1.DeviceTaintRule] = &TypedDeviceTaintInformerMock{}
)

type DoneCheckerMock struct {
}

func (*DoneCheckerMock) Name() string {
	return "mock"
}

func (*DoneCheckerMock) Done() <-chan struct{} {
	return nil
}

type ResourceEventHandlerRegistrationMock struct {
}

func (*ResourceEventHandlerRegistrationMock) HasSynced() bool {
	return false
}

func (*ResourceEventHandlerRegistrationMock) HasSyncedChecker() cache.DoneChecker {
	return &DoneCheckerMock{}
}

type SharedInformerMock struct {
}

func (s *SharedInformerMock) HasSyncedChecker() cache.DoneChecker {
	return &DoneCheckerMock{}
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

type TypedSharedIndexInformerMock struct {
}

func (s *TypedSharedIndexInformerMock) AddTypedEventHandler(_ cache.TypedResourceEventHandler[*apiresourcev1.ResourceSlice], _ ...cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return &ResourceEventHandlerRegistrationMock{}, nil
}

func (s *TypedSharedIndexInformerMock) AddTypedIndexers(_ cache.TypedIndexers[*apiresourcev1.ResourceSlice]) error {
	return nil
}

func (s *TypedSharedIndexInformerMock) GetTypedIndexer() cache.TypedIndexer[*apiresourcev1.ResourceSlice] {
	return nil
}

func (s *TypedSharedIndexInformerMock) HasSyncedChecker() cache.DoneChecker {
	return &DoneCheckerMock{}
}

func (s *TypedSharedIndexInformerMock) AddEventHandlerWithOptions(_ cache.ResourceEventHandler, _ cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *TypedSharedIndexInformerMock) RunWithContext(_ ctx.Context) {
}

func (s *TypedSharedIndexInformerMock) SetWatchErrorHandlerWithContext(_ cache.WatchErrorHandlerWithContext) error {
	return nil
}

func (s *TypedSharedIndexInformerMock) AddEventHandler(_ cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *TypedSharedIndexInformerMock) AddEventHandlerWithResyncPeriod(_ cache.ResourceEventHandler, _ time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *TypedSharedIndexInformerMock) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (s *TypedSharedIndexInformerMock) GetStore() cache.Store {
	return nil
}

func (s *TypedSharedIndexInformerMock) GetController() cache.Controller {
	return nil
}

func (s *TypedSharedIndexInformerMock) Run(stopCh <-chan struct{}) {
	RunningInformers.Add(1)
	<-stopCh
	RunningInformers.Add(-1)
}

func (s *TypedSharedIndexInformerMock) HasSynced() bool {
	return SyncDone.Load()
}

func (s *TypedSharedIndexInformerMock) LastSyncResourceVersion() string {
	return ""
}

func (s *TypedSharedIndexInformerMock) SetWatchErrorHandler(_ cache.WatchErrorHandler) error {
	return nil
}

func (s *TypedSharedIndexInformerMock) SetTransform(_ cache.TransformFunc) error {
	return nil
}

func (s *TypedSharedIndexInformerMock) IsStopped() bool {
	return false
}

func (s *TypedSharedIndexInformerMock) AddIndexers(_ cache.Indexers) error {
	return nil
}

func (s *TypedSharedIndexInformerMock) GetIndexer() cache.Indexer {
	return nil
}

func MockNewTypedSharedIndexInformer() v1.ResourceSliceIndexInformer {
	return &TypedSharedIndexInformerMock{}
}

type TypedDeviceTaintInformerMock struct {
}

func (s *TypedDeviceTaintInformerMock) AddTypedEventHandler(_ cache.TypedResourceEventHandler[*apiresourcev1.DeviceTaintRule], _ ...cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return &ResourceEventHandlerRegistrationMock{}, nil
}

func (s *TypedDeviceTaintInformerMock) AddTypedIndexers(_ cache.TypedIndexers[*apiresourcev1.DeviceTaintRule]) error {
	return nil
}

func (s *TypedDeviceTaintInformerMock) GetTypedIndexer() cache.TypedIndexer[*apiresourcev1.DeviceTaintRule] {
	return nil
}

func (s *TypedDeviceTaintInformerMock) HasSyncedChecker() cache.DoneChecker {
	return &DoneCheckerMock{}
}

func (s *TypedDeviceTaintInformerMock) AddEventHandlerWithOptions(_ cache.ResourceEventHandler, _ cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *TypedDeviceTaintInformerMock) RunWithContext(_ ctx.Context) {
}

func (s *TypedDeviceTaintInformerMock) SetWatchErrorHandlerWithContext(_ cache.WatchErrorHandlerWithContext) error {
	return nil
}

func (s *TypedDeviceTaintInformerMock) AddEventHandler(_ cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *TypedDeviceTaintInformerMock) AddEventHandlerWithResyncPeriod(_ cache.ResourceEventHandler, _ time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (s *TypedDeviceTaintInformerMock) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	return nil
}

func (s *TypedDeviceTaintInformerMock) GetStore() cache.Store {
	return nil
}

func (s *TypedDeviceTaintInformerMock) GetController() cache.Controller {
	return nil
}

func (s *TypedDeviceTaintInformerMock) Run(stopCh <-chan struct{}) {
	RunningInformers.Add(1)
	<-stopCh
	RunningInformers.Add(-1)
}

func (s *TypedDeviceTaintInformerMock) HasSynced() bool {
	return SyncDone.Load()
}

func (s *TypedDeviceTaintInformerMock) LastSyncResourceVersion() string {
	return ""
}

func (s *TypedDeviceTaintInformerMock) SetWatchErrorHandler(_ cache.WatchErrorHandler) error {
	return nil
}

func (s *TypedDeviceTaintInformerMock) SetTransform(_ cache.TransformFunc) error {
	return nil
}

func (s *TypedDeviceTaintInformerMock) IsStopped() bool {
	return false
}

func (s *TypedDeviceTaintInformerMock) AddIndexers(_ cache.Indexers) error {
	return nil
}

func (s *TypedDeviceTaintInformerMock) GetIndexer() cache.Indexer {
	return nil
}

func MockNewTypedDeviceTainInformerMock() v1.DeviceTaintRuleIndexInformer {
	return &TypedDeviceTaintInformerMock{}
}
