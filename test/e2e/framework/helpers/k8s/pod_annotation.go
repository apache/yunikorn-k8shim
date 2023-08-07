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

package k8s

import "github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"

type PodAnnotation struct {
	TaskGroupName          string                 `json:"yunikorn.apache.org/task-group-name,omitempty"`
	TaskGroups             []interfaces.TaskGroup `json:"-"`
	SchedulingPolicyParams string                 `json:"yunikorn.apache.org/schedulingPolicyParameters,omitempty"`
	Other                  map[string]string      `json:"-"`
}

const (
	TaskGroupName          = "yunikorn.apache.org/task-group-name"
	TaskGroups             = "yunikorn.apache.org/task-groups"
	PlaceHolder            = "yunikorn.apache.org/placeholder"
	SchedulingPolicyParams = "yunikorn.apache.org/schedulingPolicyParameters"

	MaxCPU = "yunikorn.apache.org/namespace.max.cpu"
	MaxMem = "yunikorn.apache.org/namespace.max.memory"
)
