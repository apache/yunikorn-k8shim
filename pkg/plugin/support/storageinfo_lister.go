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

package support

import (
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
)

type storageInfoListerImpl struct {
	cache *external.SchedulerCache
}

func (s storageInfoListerImpl) IsPVCUsedByPods(key string) bool {
	return s.cache.IsPVCUsedByPods(key)
}

var _ framework.StorageInfoLister = &storageInfoListerImpl{}

// NewStorageInfoLister returns a new StorageInfoLister which references the scheduler cache. The returned lister is
// not safe for access without acquiring the scheduler cache read lock first.
func NewStorageInfoLister(cache *external.SchedulerCache) framework.StorageInfoLister {
	sl := &storageInfoListerImpl{
		cache: cache,
	}
	return sl
}
