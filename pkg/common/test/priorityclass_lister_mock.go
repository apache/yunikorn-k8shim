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

	v1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/labels"
	listersV1 "k8s.io/client-go/listers/scheduling/v1"
)

type MockPriorityClassLister struct {
	priorityClasses map[string]*v1.PriorityClass
}

func NewMockPriorityClassLister() listersV1.PriorityClassLister {
	return &MockPriorityClassLister{
		priorityClasses: make(map[string]*v1.PriorityClass),
	}
}

func (nsl *MockPriorityClassLister) List(labels.Selector) (ret []*v1.PriorityClass, err error) {
	ret = make([]*v1.PriorityClass, 0)
	for _, pc := range nsl.priorityClasses {
		ret = append(ret, pc)
	}
	return ret, nil
}

func (nsl *MockPriorityClassLister) Add(pc *v1.PriorityClass) {
	nsl.priorityClasses[pc.Name] = pc
}

func (nsl *MockPriorityClassLister) Get(name string) (*v1.PriorityClass, error) {
	ns, ok := nsl.priorityClasses[name]
	if !ok {
		return nil, fmt.Errorf("priorityClass is not found")
	}
	return ns, nil
}
