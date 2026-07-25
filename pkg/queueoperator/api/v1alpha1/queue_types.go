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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QueueSpec defines the desired state of Queue
// Each Queue CR represents a top-level parent queue (L1) under root, along with its entire hierarchy.
// For example, if root has team-a, team-b, team-c, each with dev, stg, prod children,
// you would create 3 Queue CRs: one for team-a, one for team-b, and one for team-c.
type QueueSpec struct {
	// Queue defines the queue hierarchy configuration.
	// This mirrors YuniKorn's QueueConfig structure and contains the L1 parent queue
	// along with all its child queues in a nested structure.
	// The queue name should match the Queue CR name (metadata.name).
	// +required
	Queue QueueConfig `json:"queue"`
}

// QueueConfig represents a queue and its entire hierarchy, mirroring YuniKorn's QueueConfig structure.
// This is a recursive structure where queues can contain child queues.
type QueueConfig struct {
	// Name is the name of the queue.
	// For L1 parent queues, this should match the Queue CR name (metadata.name).
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`

	// Parent indicates if this queue is a parent queue (can have children).
	// +optional
	Parent *bool `json:"parent,omitempty"`

	// Resources defines resource limits and guarantees for the queue.
	// +optional
	Resources *Resources `json:"resources,omitempty"`

	// MaxApplications is the maximum number of applications that can run in this queue.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxApplications *uint64 `json:"maxApplications,omitempty"`

	// Properties is a map of key-value properties for queue behavior configuration.
	// Common properties include:
	// - application.sort.policy: "fifo", "fair", "stateaware"
	// - application.sort.priority: "enabled"
	// - priority.offset: priority offset value (e.g., "1000", "-1000")
	// - priority.policy: "fence" (prevents preemption)
	// - preemption.policy: "fence"
	// - preemption.delay: delay duration (e.g., "10s")
	// +optional
	Properties map[string]string `json:"properties,omitempty"`

	// AdminACL defines who can perform admin operations on this queue.
	// Format: space-separated list of users/groups (e.g., " admin", " group-a")
	// +optional
	AdminACL string `json:"adminACL,omitempty"`

	// SubmitACL defines who can submit applications to this queue.
	// Format: space-separated list of users/groups (e.g., "*", " admin", " group-a")
	// Use "*" to allow all users.
	// +optional
	SubmitACL string `json:"submitACL,omitempty"`

	// Queues is a list of child queues. This allows for nested queue hierarchies.
	// The structure is recursive - child queues can themselves have child queues.
	// +optional
	// +listType=atomic
	Queues []QueueConfig `json:"queues,omitempty"`

	// Limits defines user and group-specific limits for this queue.
	// +optional
	Limits []Limit `json:"limits,omitempty"`

	// ChildTemplate defines a template for auto-created child queues via placement rules.
	// +optional
	ChildTemplate *ChildTemplate `json:"childTemplate,omitempty"`
}

// Resources defines resource limits and guarantees for a queue.
type Resources struct {
	// Guaranteed resources are the minimum resources reserved for this queue.
	// Resources are specified as a map of resource name to quantity string.
	// Common resources: "memory" (e.g., "2G", "1000Mi"), "vcore" (e.g., "2", "1000m")
	// +optional
	Guaranteed map[string]string `json:"guaranteed,omitempty"`

	// Max resources are the maximum resources this queue can use.
	// Resources are specified as a map of resource name to quantity string.
	// Common resources: "memory" (e.g., "6G", "1000Mi"), "vcore" (e.g., "6", "1000m")
	// +optional
	Max map[string]string `json:"max,omitempty"`
}

// Limit defines user and group-specific limits for a queue or partition.
type Limit struct {
	// Limit is a description of this limit entry.
	// +required
	// +kubebuilder:validation:MinLength=1
	Limit string `json:"limit"`

	// Users is a list of users this limit applies to.
	// If exactly one user is specified, it is interpreted as a regular expression.
	// +optional
	Users []string `json:"users,omitempty"`

	// Groups is a list of groups this limit applies to.
	// If exactly one group is specified, it is interpreted as a regular expression.
	// +optional
	Groups []string `json:"groups,omitempty"`

	// MaxResources defines the maximum resources allowed for the user/group.
	// Resources are specified as a map of resource name to quantity string.
	// +optional
	MaxResources map[string]string `json:"maxResources,omitempty"`

	// MaxApplications is the maximum number of applications the user/group can have running.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxApplications *uint64 `json:"maxApplications,omitempty"`
}

// ChildTemplate defines a template for auto-created child queues via placement rules.
type ChildTemplate struct {
	// MaxApplications is the maximum number of applications for auto-created child queues.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxApplications *uint64 `json:"maxApplications,omitempty"`

	// Properties are properties to be applied to auto-created child queues.
	// +optional
	Properties map[string]string `json:"properties,omitempty"`

	// Resources are resources to be applied to auto-created child queues.
	// +optional
	Resources *Resources `json:"resources,omitempty"`
}

const (
	// ConditionTypeAvailable indicates the queue is included in the YuniKorn ConfigMap.
	ConditionTypeAvailable = "Available"
	// ConditionTypeDegraded indicates the queue was excluded from the ConfigMap
	// (e.g., duplicate name, ConfigMap write failure).
	ConditionTypeDegraded = "Degraded"
)

// QueueStatus defines the observed state of Queue.
type QueueStatus struct {
	// observedGeneration is the most recent generation observed by the controller.
	// It corresponds to the Queue's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Queue resource.
	// Condition types: "Available" (queue in ConfigMap), "Degraded" (queue excluded).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Available",type="string",JSONPath=".status.conditions[?(@.type=='Available')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubernetes/enhancements/pull/1111"

// Queue is the Schema for the queues API
type Queue struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Queue
	// +required
	Spec QueueSpec `json:"spec"`

	// status defines the observed state of Queue
	// +optional
	Status QueueStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// QueueList contains a list of Queue
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Queue `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Queue{}, &QueueList{})
}
