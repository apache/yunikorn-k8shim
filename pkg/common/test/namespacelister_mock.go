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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	listersV1 "k8s.io/client-go/listers/core/v1"
)

type MockNamespaceLister struct {
	namespaces    map[string]*v1.Namespace
	errIfNotFound bool
}

func NewMockNamespaceLister(errIfNotFound bool) listersV1.NamespaceLister {
	return &MockNamespaceLister{
		namespaces:    make(map[string]*v1.Namespace),
		errIfNotFound: errIfNotFound,
	}
}

func (nsl *MockNamespaceLister) List(labels.Selector) (ret []*v1.Namespace, err error) {
	return nil, nil
}

func (nsl *MockNamespaceLister) Add(ns *v1.Namespace) {
	nsl.namespaces[ns.Name] = ns
}

func (nsl *MockNamespaceLister) Get(name string) (*v1.Namespace, error) {
	ns, ok := nsl.namespaces[name]
	if !ok {
		if nsl.errIfNotFound {
			return nil, fmt.Errorf("namespace %s is not found", name)
		}
		return nil, nil
	}
	return ns, nil
}
