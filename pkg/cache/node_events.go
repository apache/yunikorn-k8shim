/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import "github.com/cloudera/yunikorn-k8shim/pkg/common/events"

type CachedSchedulerNodeEvent struct {
	NodeId        string
	Event         events.SchedulerNodeEventType
	Arguments     []interface{}
}

func (sn CachedSchedulerNodeEvent) GetEvent() events.SchedulerNodeEventType {
	return sn.Event
}

func (sn CachedSchedulerNodeEvent) GetArgs() []interface{} {
	return sn.Arguments
}

func (sn CachedSchedulerNodeEvent) GetNodeId() string {
	return sn.NodeId
}