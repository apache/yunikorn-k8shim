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

package constants

import (
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

// Common
const True = "true"
const False = "false"

// Cluster
const DefaultNodeAttributeHostNameKey = "si.io/hostname"
const DefaultNodeAttributeRackNameKey = "si.io/rackname"
const DefaultNodeInstanceTypeNodeLabelKey = "node.kubernetes.io/instance-type"
const DefaultRackName = "/rack-default"
const DomainYuniKorn = siCommon.DomainYuniKorn

// Application
const LabelApp = "app"
const LabelApplicationID = "applicationId"
const AnnotationApplicationID = DomainYuniKorn + "app-id"
const LabelQueueName = "queue"
const RootQueue = "root"
const AnnotationQueueName = DomainYuniKorn + "queue"
const AnnotationParentQueue = DomainYuniKorn + "parentqueue"
const LabelDisableStateAware = "disableStateAware"
const AnnotationDisableStateAware = DomainYuniKorn + "disable-state-aware"
const ApplicationDefaultQueue = "root.default"
const DefaultPartition = "default"
const AppTagNamespace = "namespace"
const AppTagNamespaceParentQueue = "namespace.parentqueue"
const AppTagImagePullSecrets = "imagePullSecrets"
const DefaultAppNamespace = "default"
const DefaultUserLabel = DomainYuniKorn + "username"
const DefaultUser = "nobody"

// Spark
const SparkLabelAppID = "spark-app-selector"

// Configuration
const ConfigMapName = "yunikorn-configs"
const DefaultConfigMapName = "yunikorn-defaults"
const SchedulerName = "yunikorn"

// OwnerReferences
const DaemonSetType = "DaemonSet"

// Gang scheduling
const PlaceholderContainerImage = "registry.k8s.io/pause:3.7"
const PlaceholderContainerName = "pause"
const PlaceholderPodRestartPolicy = "Never"
const LabelPlaceholderFlag = "placeholder"
const AnnotationPlaceholderFlag = DomainYuniKorn + "placeholder"
const AnnotationTaskGroupName = DomainYuniKorn + "task-group-name"
const AnnotationTaskGroups = DomainYuniKorn + "task-groups"
const AnnotationSchedulingPolicyParam = DomainYuniKorn + "schedulingPolicyParameters"
const SchedulingPolicyTimeoutParam = "placeholderTimeoutInSeconds"
const SchedulingPolicyParamDelimiter = " "
const SchedulingPolicyStyleParam = "gangSchedulingStyle"
const SchedulingPolicyStyleParamDefault = "Soft"

var SchedulingPolicyStyleParamValues = map[string]string{"Hard": "Hard", "Soft": "Soft"}

const ApplicationInsufficientResourcesFailure = "ResourceReservationTimeout"
const ApplicationRejectedFailure = "ApplicationRejected"

// namespace.max.* (Retaining for backwards compatibility. Need to be removed in next major release)
const CPUQuota = DomainYuniKorn + "namespace.max.cpu"
const MemQuota = DomainYuniKorn + "namespace.max.memory"

// NamespaceQuota Namespace Quota
const NamespaceQuota = DomainYuniKorn + "namespace.quota"

// NamespaceGuaranteed Namespace Guaranteed
const NamespaceGuaranteed = DomainYuniKorn + "namespace.guaranteed"

// AnnotationAllowPreemption set on PriorityClass, opt out of preemption for pods with this priority class
const AnnotationAllowPreemption = DomainYuniKorn + "allow-preemption"

// AnnotationIgnoreApplication set on Pod prevents by admission controller, prevents YuniKorn from honoring application ID
const AnnotationIgnoreApplication = DomainYuniKorn + "ignore-application"

// AnnotationGenerateAppID adds application ID to workloads in the namespace even if not set in the admission config.
// Overrides the regexp behaviour if set, checked before the regexp is evaluated.
// true: add an application ID label
// false: do not add an application ID
const AnnotationGenerateAppID = DomainYuniKorn + "namespace.generateAppId"

// AnnotationEnableYuniKorn sets the scheduler name to YuniKorn for workloads in the namespace even if not set in the admission config.
// Overrides the regexp behaviour if set, checked before the regexp is evaluated.
// true: set the scheduler name to YuniKorn
// false: do not do anything
const AnnotationEnableYuniKorn = DomainYuniKorn + "namespace.enableYuniKorn"

// Admission Controller pod update constants
const AutoGenAppPrefix = "yunikorn"
const AutoGenAppSuffix = "autogen"
const AddPatchOp = "add"
const AnnotationPatchPath = "/metadata/annotations"
const LabelPatchPath = "/metadata/labels"
const SchedulerNamePatchPath = "/spec/schedulerName"

// Compression Algorithms for schedulerConfig
const GzipSuffix = "gz"
