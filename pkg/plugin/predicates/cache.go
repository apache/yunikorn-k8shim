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

package predicates

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type predicateCache struct {
	sync.RWMutex
	cache map[string]map[string]*framework.Status //key_1: nodename key_2:pod uid
}

// PredicateWithCache: check the predicate result existed in cache
func (pc *predicateCache) PredicateWithCache(nodeName string, pod *v1.Pod) *framework.Status {
	podUID := string(pod.UID)
	pc.RLock()
	defer pc.RUnlock()
	if nodeCache, exist := pc.cache[nodeName]; exist {
		if result, exist := nodeCache[podUID]; exist {
			return result
		}
	}

	return nil
}

// UpdateCache update cache data
func (pc *predicateCache) UpdateCache(nodeName string, pod *v1.Pod, cacheStatusUpdate *framework.Status) {
	podUID := string(pod.UID)
	pc.Lock()
	defer pc.Unlock()

	if _, exist := pc.cache[nodeName]; !exist {
		podCache := make(map[string]*framework.Status)
		podCache[podUID] = cacheStatusUpdate
		pc.cache[nodeName] = podCache
	} else {
		pc.cache[nodeName][podUID] = cacheStatusUpdate
	}
}
