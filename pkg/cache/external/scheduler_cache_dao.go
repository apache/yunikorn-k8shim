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
	Statistics     SchedulerCacheStatisticsDao     `json:"statistics"`
	Nodes          map[string]NodeDao              `json:"nodes"`
	Pods           map[string]PodDao               `json:"pods"`
	SchedulingPods map[string]PodSchedulingInfoDao `json:"schedulingState"`
}

type SchedulerCacheStatisticsDao struct {
	Nodes                 int            `json:"nodes"`
	Pods                  int            `json:"pods"`
	Assumed               int            `json:"assumed"`
	PendingAllocations    int            `json:"pendingAllocations"`
	InProgressAllocations int            `json:"inProgressAllocations"`
	PodsAssigned          int            `json:"podsAssigned"`
	Phases                map[string]int `json:"phases"`
}

type NodeDao struct {
	Name              string             `json:"name"`
	UID               types.UID          `json:"uid"`
	NodeInfo          v1.NodeSystemInfo  `json:"nodeInfo"`
	CreationTimestamp time.Time          `json:"creationTimestamp"`
	Annotations       map[string]string  `json:"annotations"`
	Labels            map[string]string  `json:"labels"`
	PodCIDRs          []string           `json:"podCIDRs"`
	Taints            []v1.Taint         `json:"taints"`
	Addresses         []v1.NodeAddress   `json:"addresses"`
	Allocatable       v1.ResourceList    `json:"allocatable"`
	Capacity          v1.ResourceList    `json:"capacity"`
	Conditions        []v1.NodeCondition `json:"conditions"`
}

type PodDao struct {
	Namespace         string               `json:"namespace"`
	Name              string               `json:"name"`
	GenerateName      string               `json:"generateName"`
	UID               types.UID            `json:"uid"`
	CreationTimestamp time.Time            `json:"creationTimestamp"`
	Annotations       map[string]string    `json:"annotations"`
	Labels            map[string]string    `json:"labels"`
	NodeName          string               `json:"nodeName"`
	Affinity          *v1.Affinity         `json:"affinity"`
	NodeSelector      map[string]string    `json:"nodeSelector"`
	PriorityClassName string               `json:"priorityClassName"`
	Priority          *int32               `json:"priority"`
	PreemptionPolicy  *v1.PreemptionPolicy `json:"preemptionPolicy"`
	SchedulerName     string               `json:"schedulerName"`
	Tolerations       []v1.Toleration      `json:"tolerations"`
	Containers        []ContainerDao       `json:"containers"`
	Status            v1.PodStatus         `json:"status"`
}

type ContainerDao struct {
	Name      string                  `json:"name"`
	Resources v1.ResourceRequirements `json:"resources"`
}

type PodSchedulingInfoDao struct {
	Namespace       string    `json:"-"`
	Name            string    `json:"-"`
	UID             types.UID `json:"uid"`
	AssignedNode    string    `json:"assignedNode"`
	Assumed         bool      `json:"assumed"`
	AllVolumesBound bool      `json:"allVolumesBound"`
	PendingNode     string    `json:"pendingNode"`
	InProgressNode  string    `json:"inProgressNode"`
}
