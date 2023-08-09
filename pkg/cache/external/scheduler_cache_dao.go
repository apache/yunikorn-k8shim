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

package external

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type SchedulerCacheDao struct {
	Statistics      SchedulerCacheStatisticsDao     `json:"statistics"`
	Nodes           map[string]NodeDao              `json:"nodes,omitempty"`
	Pods            map[string]PodDao               `json:"pods,omitempty"`
	PriorityClasses map[string]PriorityClassDao     `json:"priorityClasses,omitempty"`
	SchedulingPods  map[string]PodSchedulingInfoDao `json:"schedulingState,omitempty"`
}

type SchedulerCacheStatisticsDao struct {
	Nodes                 int            `json:"nodes,omitempty"`
	Pods                  int            `json:"pods,omitempty"`
	PriorityClasses       int            `json:"priorityClasses,omitempty"`
	Assumed               int            `json:"assumed,omitempty"`
	PendingAllocations    int            `json:"pendingAllocations,omitempty"`
	InProgressAllocations int            `json:"inProgressAllocations,omitempty"`
	PodsAssigned          int            `json:"podsAssigned,omitempty"`
	Phases                map[string]int `json:"phases,omitempty"`
}

type NodeDao struct {
	Name              string             `json:"name"`
	UID               types.UID          `json:"uid,omitempty"`
	NodeInfo          v1.NodeSystemInfo  `json:"nodeInfo"`
	CreationTimestamp time.Time          `json:"creationTimestamp"`
	Annotations       map[string]string  `json:"annotations,omitempty"`
	Labels            map[string]string  `json:"labels,omitempty"`
	PodCIDRs          []string           `json:"podCIDRs,omitempty"`
	Taints            []v1.Taint         `json:"taints,omitempty"`
	Addresses         []v1.NodeAddress   `json:"addresses,omitempty"`
	Allocatable       v1.ResourceList    `json:"allocatable,omitempty"`
	Capacity          v1.ResourceList    `json:"capacity,omitempty"`
	Conditions        []v1.NodeCondition `json:"conditions,omitempty"`
}

type PodDao struct {
	Namespace         string               `json:"namespace"`
	Name              string               `json:"name"`
	GenerateName      string               `json:"generateName,omitempty"`
	UID               types.UID            `json:"uid,omitempty"`
	CreationTimestamp time.Time            `json:"creationTimestamp"`
	Annotations       map[string]string    `json:"annotations,omitempty"`
	Labels            map[string]string    `json:"labels,omitempty"`
	NodeName          string               `json:"nodeName,omitempty"`
	Affinity          *v1.Affinity         `json:"affinity,omitempty"`
	NodeSelector      map[string]string    `json:"nodeSelector,omitempty"`
	PriorityClassName string               `json:"priorityClassName,omitempty"`
	Priority          *int32               `json:"priority,omitempty"`
	PreemptionPolicy  *v1.PreemptionPolicy `json:"preemptionPolicy,omitempty"`
	SchedulerName     string               `json:"schedulerName,omitempty"`
	Tolerations       []v1.Toleration      `json:"tolerations,omitempty"`
	Containers        []ContainerDao       `json:"containers,omitempty"`
	Status            v1.PodStatus         `json:"status"`
}

type PriorityClassDao struct {
	Name             string               `json:"name"`
	Annotations      map[string]string    `json:"annotations,omitempty"`
	Labels           map[string]string    `json:"labels,omitempty"`
	Value            int32                `json:"value"`
	GlobalDefault    bool                 `json:"globalDefault,omitempty"`
	PreemptionPolicy *v1.PreemptionPolicy `json:"preemptionPolicy,omitempty"`
}

type ContainerDao struct {
	Name      string                  `json:"name"`
	Resources v1.ResourceRequirements `json:"resources"`
}

type PodSchedulingInfoDao struct {
	Namespace       string    `json:"-"`
	Name            string    `json:"-"`
	UID             types.UID `json:"uid,omitempty"`
	AssignedNode    string    `json:"assignedNode,omitempty"`
	Assumed         bool      `json:"assumed,omitempty"`
	AllVolumesBound bool      `json:"allVolumesBound,omitempty"`
	PendingNode     string    `json:"pendingNode,omitempty"`
	InProgressNode  string    `json:"inProgressNode,omitempty"`
}
