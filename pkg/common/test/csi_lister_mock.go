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
	"fmt"

	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
)

// CSINodeListerMock declares a storagev1.CSINode type for testing.
type CSINodeListerMock []storagev1.CSINode

// Get returns a fake CSINode object.
func (n CSINodeListerMock) Get(name string) (*storagev1.CSINode, error) {
	for _, cn := range n {
		if cn.Name == name {
			return &cn, nil
		}
	}
	return nil, errors.NewNotFound(storagev1.Resource("csinodes"), name)
}

// List lists all CSINodes in the indexer.
func (n CSINodeListerMock) List(selector labels.Selector) (ret []*storagev1.CSINode, err error) {
	return nil, fmt.Errorf("not implemented")
}
