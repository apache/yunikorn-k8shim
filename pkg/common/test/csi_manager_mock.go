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
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// CSIManagerImplMock is an implementation of the CSIManager interface.
type CSIManagerImplMock struct {
	csiNodeLister *CsiNodeListerImplMock
}

var _ fwk.CSIManager = &CSIManagerImplMock{}

func NewCSIManagerMock(csiNodeLister storagelisters.CSINodeLister) *CSIManagerImplMock {
	return &CSIManagerImplMock{csiNodeLister: NewCSINodeListerMock(csiNodeLister)}
}

func (m *CSIManagerImplMock) CSINodes() fwk.CSINodeLister {
	return m.csiNodeLister
}

type CsiNodeListerImplMock struct {
	csiNodeLister storagelisters.CSINodeLister
}

var _ fwk.CSINodeLister = &CsiNodeListerImplMock{}

func NewCSINodeListerMock(csiNodeLister storagelisters.CSINodeLister) *CsiNodeListerImplMock {
	return &CsiNodeListerImplMock{csiNodeLister: csiNodeLister}
}

func (l *CsiNodeListerImplMock) List() ([]*storagev1.CSINode, error) {
	return l.csiNodeLister.List(labels.Everything())
}

func (l *CsiNodeListerImplMock) Get(name string) (*storagev1.CSINode, error) {
	return l.csiNodeLister.Get(name)
}
