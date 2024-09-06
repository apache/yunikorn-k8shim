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

package configmanager

const (
	TestResultsPath = "test_results/"

	// LogPath is the path to store the dumped log files and should be equal to the artifact path set in pre-commit.yaml.
	LogPath = "build/e2e/"

	YKScheduler          = "yunikorn-scheduler"
	YKSchedulerContainer = "yunikorn-scheduler-k8s"
	SchedulerName        = "yunikorn"
	YKAdmCtrl            = "yunikorn-admission-controller"
	YKSvcName            = "yunikorn-service" // YuniKorn service name
	YKDeploymentName     = "yunikorn-scheduler"

	YKAdmCtrlName = "yunikorn-admission-controller-service" // YuniKorn Admission controller serivce name

	// REST endpoints of YuniKorn
	PartitionsPath    = "ws/v1/partitions"
	QueuesPath        = "ws/v1/partition/%s/queues"
	QueuePath         = "ws/v1/partition/%s/queue/%s"
	AppsPath          = "ws/v1/partition/%s/queue/%s/applications"
	AppPath           = "ws/v1/partition/%s/queue/%s/application/%s"
	PartitionAppPath  = "ws/v1/partition/%s/application/%s"
	CompletedAppsPath = "ws/v1/partition/%s/applications/completed"
	ConfigPath        = "ws/v1/config"
	ClustersPath      = "ws/v1/clusters"
	NodesPath         = "ws/v1/partition/%s/nodes"
	UserUsagePath     = "ws/v1/partition/%s/usage/user/%s"
	GroupUsagePath    = "ws/v1/partition/%s/usage/group/%s"
	GroupsUsagePath   = "ws/v1/partition/%s/usage/groups"
	HealthCheckPath   = "ws/v1/scheduler/healthcheck"
	ValidateConfPath  = "ws/v1/validate-conf"
	FullStateDumpPath = "ws/v1/fullstatedump"

	// YuniKorn Service Details
	DefaultYuniKornHost   = "localhost"
	DefaultYuniKornPort   = "9080"
	DefaultYuniKornScheme = "http"

	DefaultYuniKornConfigMap = "yunikorn-configs"
	DefaultPluginConfigMap   = "yunikorn-configs"
	DefaultPolicyGroup       = "queues.yaml"

	// Queues
	RootQueue = "root"

	// Partitions
	DefaultPartition = "default"
)

func GetConfigMapName() string {
	return DefaultYuniKornConfigMap
}
