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
	"errors"

	schedulingv1 "k8s.io/api/scheduling/v1"
	informersv1 "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type PriorityClassCache struct {
	priorityClasses map[string]bool

	locking.RWMutex
}

// NewPriorityClassCache creates a new cache and registers the handler for the cache with the Informer.
func NewPriorityClassCache(priorityClasses informersv1.PriorityClassInformer) (*PriorityClassCache, error) {
	pcc := &PriorityClassCache{
		priorityClasses: make(map[string]bool),
	}
	if priorityClasses != nil {
		_, err := priorityClasses.Informer().AddEventHandler(&priorityClassUpdateHandler{cache: pcc})
		if err != nil {
			return nil, errors.Join(errors.New("failed to create a new cache and register the handler: "), err)
		}
	}
	return pcc, nil
}

// isPreemptSelfAllowed returns the preemption value. Only returns false if configured.
func (pcc *PriorityClassCache) isPreemptSelfAllowed(priorityClassName string) bool {
	pcc.RLock()
	defer pcc.RUnlock()

	value, ok := pcc.priorityClasses[priorityClassName]
	if !ok {
		return true
	}
	return value
}

// priorityClassExists for test only to see if the PriorityClass has been added to the cache or not.
func (pcc *PriorityClassCache) priorityClassExists(priorityClassName string) bool {
	pcc.RLock()
	defer pcc.RUnlock()

	_, ok := pcc.priorityClasses[priorityClassName]
	return ok
}

// priorityClassUpdateHandler implements the K8s ResourceEventHandler interface for PriorityClass.
type priorityClassUpdateHandler struct {
	cache *PriorityClassCache
}

// OnAdd adds or replaces the priority class entry in the cache.
// The cached value is only the resulting value of the annotation, not the whole PriorityClass object.
// An empty string for the Name is technically possible but should not occur.
func (h *priorityClassUpdateHandler) OnAdd(obj interface{}, _ bool) {
	pc := utils.Convert2PriorityClass(obj)
	if pc == nil {
		return
	}

	b := getAnnotationBoolean(pc.Annotations, constants.AnnotationAllowPreemption)
	h.cache.Lock()
	defer h.cache.Unlock()
	h.cache.priorityClasses[pc.Name] = b
}

// OnUpdate calls OnAdd for processing the PriorityClass cache update.
func (h *priorityClassUpdateHandler) OnUpdate(_, newObj interface{}) {
	h.OnAdd(newObj, false)
}

// OnDelete removes the PriorityClass from the cache.
func (h *priorityClassUpdateHandler) OnDelete(obj interface{}) {
	var pc *schedulingv1.PriorityClass
	switch t := obj.(type) {
	case *schedulingv1.PriorityClass:
		pc = t
	case cache.DeletedFinalStateUnknown:
		pc = utils.Convert2PriorityClass(obj)
	default:
		log.Log(log.Admission).Warn("unable to convert to PriorityClass")
		return
	}
	if pc == nil {
		return
	}

	h.cache.Lock()
	defer h.cache.Unlock()
	delete(h.cache.priorityClasses, pc.Name)
}

// getAnnotationBoolean retrieves the value from the map and returns it.
// Defaults to true if the name does not exist.
func getAnnotationBoolean(m map[string]string, name string) bool {
	strVal, ok := m[name]
	if !ok {
		return true
	}
	switch strVal {
	case constants.False:
		return false
	default:
		return true
	}
}
