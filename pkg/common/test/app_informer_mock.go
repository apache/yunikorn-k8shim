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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	lister "github.com/apache/yunikorn-k8shim/pkg/client/listers/yunikorn.apache.org/v1alpha1"
)

type MockedAppInformer struct {
	appLister lister.ApplicationLister
}

func NewAppInformerMock() *MockedAppInformer {
	appLister := MockedAppLister{}
	return &MockedAppInformer{
		appLister: appLister,
	}
}

func (m MockedAppInformer) Informer() cache.SharedIndexInformer {
	return nil
}

func (m MockedAppInformer) Lister() lister.ApplicationLister {
	return m.appLister
}

type MockedAppLister struct {
}

func (m MockedAppLister) List(selector labels.Selector) (ret []*v1alpha1.Application, err error) {
	return []*v1alpha1.Application{}, nil
}

func (m MockedAppLister) Applications(namespace string) lister.ApplicationNamespaceLister {
	return m
}

func (m MockedAppLister) Get(name string) (*v1alpha1.Application, error) {
	return nil, nil
}
