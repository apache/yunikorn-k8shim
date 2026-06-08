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
	fwk "k8s.io/kube-scheduler/framework"
)

type SharedListerMock struct {
	nodeLister    *NodeInfoListerMock
	storageLister *StorageInfoListerMock
}

func (s *SharedListerMock) PodGroupStates() fwk.PodGroupStateLister {
	return nil
}

func (s *SharedListerMock) NodeInfos() fwk.NodeInfoLister {
	return s.nodeLister
}

func (s *SharedListerMock) StorageInfos() fwk.StorageInfoLister {
	return s.storageLister
}

func NewEmptySharedListerMock() fwk.SharedLister {
	return &SharedListerMock{}
}
func (s *SharedListerMock) NodeLister() *NodeInfoListerMock {
	return s.nodeLister
}
func NewSharedListerMock() *SharedListerMock {
	return &SharedListerMock{
		nodeLister: &NodeInfoListerMock{
			nodeInfos: make([]fwk.NodeInfo, 0),
		},
	}
}
