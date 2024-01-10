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

package cache

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AddApplicationRequest struct {
	Metadata ApplicationMetadata
}

type AddTaskRequest struct {
	Metadata TaskMetadata
}

type ApplicationMetadata struct {
	ApplicationID              string
	QueueName                  string
	User                       string
	Tags                       map[string]string
	Groups                     []string
	TaskGroups                 []TaskGroup
	OwnerReferences            []metav1.OwnerReference
	SchedulingPolicyParameters *SchedulingPolicyParameters
	CreationTime               int64
}

type TaskGroup struct {
	Name         string
	MinMember    int32
	Labels       map[string]string
	Annotations  map[string]string
	MinResource  map[string]resource.Quantity
	NodeSelector map[string]string
	Tolerations  []v1.Toleration
	Affinity     *v1.Affinity
}

type TaskMetadata struct {
	ApplicationID string
	TaskID        string
	Pod           *v1.Pod
	Placeholder   bool
	TaskGroupName string
}

type SchedulingPolicyParameters struct {
	placeholderTimeout  int64
	gangSchedulingStyle string
}

func NewSchedulingPolicyParameters(placeholderTimeout int64, gangSchedulingStyle string) *SchedulingPolicyParameters {
	spp := &SchedulingPolicyParameters{placeholderTimeout: placeholderTimeout, gangSchedulingStyle: gangSchedulingStyle}
	return spp
}

func (spp *SchedulingPolicyParameters) GetPlaceholderTimeout() int64 {
	return spp.placeholderTimeout
}

func (spp *SchedulingPolicyParameters) GetGangSchedulingStyle() string {
	return spp.gangSchedulingStyle
}
