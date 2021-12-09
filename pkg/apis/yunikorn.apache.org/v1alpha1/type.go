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

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Application struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   ApplicationSpec   `json:"spec"`
	Status ApplicationStatus `json:"status"`
}

// Spec part

type ApplicationSpec struct {
	SchedulingPolicy SchedulingPolicy `json:"schedulingPolicy"`
	Queue            string           `json:"queue"`
	TaskGroups       []TaskGroup      `json:"taskGroups"`
}

type SchedulingPolicy struct {
	Type       SchedulingPolicyType `json:"type"`
	Parameters map[string]string    `json:"parameters,omitempty"`
}

type SchedulingPolicyType string

const (
	TryOnce    SchedulingPolicyType = "TryOnce"
	MaxRetry   SchedulingPolicyType = "MaxRetry"
	TryReserve SchedulingPolicyType = "TryReserve"
	TryPreempt SchedulingPolicyType = "TryPreempt"
)

type TaskGroup struct {
	Name         string                       `json:"name"`
	MinMember    int32                        `json:"minMember"`
	Labels       map[string]string            `json:"labels,omitempty"`
	Annotations  map[string]string            `json:"annotations,omitempty"`
	MinResource  map[string]resource.Quantity `json:"minResource"`
	NodeSelector map[string]string            `json:"nodeSelector,omitempty"`
	Tolerations  []v1.Toleration              `json:"tolerations,omitempty"`
	Affinity     *v1.Affinity                 `json:"affinity,omitempty"`
}

// Status part

type ApplicationStateType string

const (
	NewApplicationState ApplicationStateType = "New"
	AcceptedState       ApplicationStateType = "Accepted"
	StartingState       ApplicationStateType = "Starting"
	RunningState        ApplicationStateType = "Running"
	WaitingState        ApplicationStateType = "Waiting"
	RejectedState       ApplicationStateType = "Rejected"
	CompletedState      ApplicationStateType = "Completed"
	KilledState         ApplicationStateType = "Killed"
)

type ApplicationStatus struct {
	AppID      string               `json:"appID,,omitempty"`
	AppStatus  ApplicationStateType `json:"applicationState,omitempty"`
	Message    string               `json:"message,omitempty"`
	LastUpdate metav1.Time          `json:"lastUpdate,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Application `json:"items"`
}
