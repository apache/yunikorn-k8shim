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

package interfaces

import (
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// recoverable interface defines a certain type of app that can be recovered upon scheduler' restart
// each app manager needs to implement this interface in order to support fault recovery
//
// why we need this?
// the scheduler is stateless, all states are maintained just in memory,
// so each time when scheduler restarts, it needs to recover apps and nodes states from scratch.
// nodes state will be taken care of by the scheduler itself, however for apps state recovery,
// the scheduler will need to call this function to collect existing app info,
// and then properly recover these applications before recovering nodes.
type Recoverable interface {
	// list applications returns all existing applications known to this app manager.
	ListPods() ([]*v1.Pod, error)

	// this is called during recovery
	// for a given pod, return an allocation if found
	GetExistingAllocation(pod *v1.Pod) *si.Allocation
}
