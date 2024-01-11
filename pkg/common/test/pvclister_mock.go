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
	"errors"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	listersv1 "k8s.io/client-go/listers/core/v1"
)

type MockedPersistentVolumeClaimLister struct {
	pvClaims      map[*v1.PersistentVolumeClaim]struct{}
	getPVCFailure bool

	sync.RWMutex
}

func (p *MockedPersistentVolumeClaimLister) SetFailureOnGetPVC(b bool) {
	p.Lock()
	defer p.Unlock()
	p.getPVCFailure = b
}

func (p *MockedPersistentVolumeClaimLister) AddPVC(pvc *v1.PersistentVolumeClaim) {
	p.Lock()
	defer p.Unlock()
	p.pvClaims[pvc] = struct{}{}
}

func (p *MockedPersistentVolumeClaimLister) DeletePVC(pvc *v1.PersistentVolumeClaim) {
	p.Lock()
	defer p.Unlock()
	delete(p.pvClaims, pvc)
}

func (p *MockedPersistentVolumeClaimLister) List(selector labels.Selector) (ret []*v1.PersistentVolumeClaim, err error) {
	p.RLock()
	defer p.RUnlock()
	result := make([]*v1.PersistentVolumeClaim, 0)
	for pod := range p.pvClaims {
		if selector.Matches(labels.Set(pod.Labels)) {
			result = append(result, pod)
		}
	}
	return result, nil
}

func (p *MockedPersistentVolumeClaimLister) PersistentVolumeClaims(namespace string) listersv1.PersistentVolumeClaimNamespaceLister {
	p.RLock()
	defer p.RUnlock()
	filtered := make(map[string]*v1.PersistentVolumeClaim)
	for k := range p.pvClaims {
		if k.Namespace == namespace {
			filtered[k.Name] = k
		}
	}
	return &MockPersistentVolumeClaimNamespaceLister{
		pvClaims:      filtered,
		getPVCFailure: p.getPVCFailure,
	}
}

type MockPersistentVolumeClaimNamespaceLister struct {
	pvClaims      map[string]*v1.PersistentVolumeClaim
	getPVCFailure bool
}

func (pns *MockPersistentVolumeClaimNamespaceLister) List(selector labels.Selector) ([]*v1.PersistentVolumeClaim, error) {
	result := make([]*v1.PersistentVolumeClaim, 0)
	for _, pvc := range pns.pvClaims {
		if selector.Matches(labels.Set(pvc.Labels)) {
			result = append(result, pvc)
		}
	}
	return result, nil
}

func (pns *MockPersistentVolumeClaimNamespaceLister) Get(name string) (*v1.PersistentVolumeClaim, error) {
	if pns.getPVCFailure {
		return nil, errors.New("error while getting PVC")
	}
	pvc := pns.pvClaims[name]
	return pvc, nil
}
