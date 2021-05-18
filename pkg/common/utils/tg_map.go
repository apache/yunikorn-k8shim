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

package utils

import (
	"sync"
)

type TaskGroupInstanceCountMap struct {
	counts map[string]int32
	sync.RWMutex
}

func NewTaskGroupInstanceCountMap() *TaskGroupInstanceCountMap {
	return &TaskGroupInstanceCountMap{
		counts: make(map[string]int32),
	}
}

func (t *TaskGroupInstanceCountMap) Add(taskGroupName string, num int32) {
	t.update(taskGroupName, num)
}

func (t *TaskGroupInstanceCountMap) AddOne(taskGroupName string) {
	t.update(taskGroupName, 1)
}

func (t *TaskGroupInstanceCountMap) DeleteOne(taskGroupName string) {
	t.update(taskGroupName, -1)
}

func (t *TaskGroupInstanceCountMap) update(taskGroupName string, delta int32) {
	t.Lock()
	defer t.Unlock()
	if v, ok := t.counts[taskGroupName]; ok {
		t.counts[taskGroupName] = v + delta
	} else {
		t.counts[taskGroupName] = delta
	}
}

func (t *TaskGroupInstanceCountMap) Size() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.counts)
}

func (t *TaskGroupInstanceCountMap) GetTaskGroupInstanceCount(groupName string) int32 {
	t.RLock()
	defer t.RUnlock()
	return t.counts[groupName]
}

func (t *TaskGroupInstanceCountMap) Equals(target *TaskGroupInstanceCountMap) bool {
	if t == nil {
		return t == target
	}

	t.RLock()
	defer t.RUnlock()

	if target == nil {
		return false
	}

	if t.Size() != target.Size() {
		return false
	}

	for k, v := range t.counts {
		if target.counts[k] != v {
			return false
		}
	}

	return true
}
