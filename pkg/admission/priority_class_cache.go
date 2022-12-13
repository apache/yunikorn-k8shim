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

package admission

import (
	"sync"

	schedulingv1 "k8s.io/api/scheduling/v1"
	informersv1 "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type PriorityClassCache struct {
	priorityClasses map[string]*schedulingv1.PriorityClass
	lock            sync.RWMutex
}

func NewPriorityClassCache() *PriorityClassCache {
	return &PriorityClassCache{
		priorityClasses: make(map[string]*schedulingv1.PriorityClass),
	}
}

func (pcc *PriorityClassCache) RegisterHandlers(priorityClasses informersv1.PriorityClassInformer) {
	priorityClasses.Informer().AddEventHandler(&priorityClassUpdateHandler{cache: pcc})
}

func (pcc *PriorityClassCache) IsPreemptSelfAllowed(priorityClassName string) bool {
	priorityClass := pcc.getPriorityClass(priorityClassName)

	// default to true
	if priorityClass == nil {
		return true
	}

	value, ok := priorityClass.Annotations[constants.AnnotationAllowPreemption]
	if !ok {
		return true
	}

	switch value {
	case constants.False:
		return false
	default:
		return true
	}
}

func (pcc *PriorityClassCache) getPriorityClass(priorityClassName string) *schedulingv1.PriorityClass {
	pcc.lock.RLock()
	defer pcc.lock.RUnlock()

	result, ok := pcc.priorityClasses[priorityClassName]
	if !ok {
		return nil
	}
	return result
}

type priorityClassUpdateHandler struct {
	cache *PriorityClassCache
}

func (h *priorityClassUpdateHandler) OnAdd(obj interface{}) {
	pc := utils.Convert2PriorityClass(obj)
	if pc == nil {
		return
	}

	h.cache.lock.Lock()
	defer h.cache.lock.Unlock()
	h.cache.priorityClasses[pc.Name] = pc
}

func (h *priorityClassUpdateHandler) OnUpdate(_, newObj interface{}) {
	h.OnAdd(newObj)
}

func (h *priorityClassUpdateHandler) OnDelete(obj interface{}) {
	var pc *schedulingv1.PriorityClass
	switch t := obj.(type) {
	case *schedulingv1.PriorityClass:
		pc = t
	case cache.DeletedFinalStateUnknown:
		pc = utils.Convert2PriorityClass(obj)
	default:
		log.Logger().Warn("unable to convert to PriorityClass")
		return
	}
	if pc == nil {
		return
	}

	h.cache.lock.Lock()
	defer h.cache.lock.Unlock()
	delete(h.cache.priorityClasses, pc.Name)
}
