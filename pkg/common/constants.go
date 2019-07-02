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

package common

// Cluster
const DefaultNodeAttributeHostNameKey = "si.io/hostname"
const DefaultNodeAttributeRackNameKey = "si.io/rackname"
const DefaultRackName = "/rack-default"

// Application
const LabelApplicationId = "applicationId"
const LabelQueueName = "queue"
const ApplicationDefaultQueue = "root"
const DefaultPartition = "default"

// Resource
const Memory = "memory"
const CPU = "vcore"

// Spark
const SparkLabelAppId = "spark-app-id"
const SparkLabelRole = "spark-role"
const SparkLabelRoleDriver = "driver"

// Configuration
const DefaultConfigMapName = "yunikorn-configs"
