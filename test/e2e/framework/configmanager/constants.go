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

import (
	"os"
)

const (
	TestResultsPath = "test_results/"

	// LogPerm is the permission for files that are created by this framework
	// that contain logs, outputs etc
	LogPerm = os.FileMode(0666)

	YKScheduler          = "yunikorn-scheduler"
	YKSchedulerContainer = "yunikorn-scheduler-k8s"
	SchedulerName        = "yunikorn"
	YKAdmCtrl            = "yunikorn-admission-controller"
	YKSvcName            = "yunikorn-service" // YuniKorn service name
	YKDeploymentName     = "yunikorn-scheduler"

	YKAdmCtrlName = "yunikorn-admission-controller-service" // YuniKorn Admission controller serivce name

	// REST endpoints of YuniKorn
	PartitionsPath   = "ws/v1/partitions"
	QueuesPath       = "ws/v1/partition/%s/queues"
	AppsPath         = "ws/v1/partition/%s/queue/%s/applications"
	AppPath          = "ws/v1/partition/%s/queue/%s/application/%s"
	ClustersPath     = "ws/v1/clusters"
	NodesPath        = "ws/v1/nodes"
	HealthCheckPath  = "ws/v1/scheduler/healthcheck"
	ValidateConfPath = "ws/v1/validate-conf"

	// YuniKorn Service Details
	DefaultYuniKornHost   = "localhost"
	DefaultYuniKornPort   = "9080"
	DefaultYuniKornScheme = "http"

	DefaultYuniKornConfigMap = "yunikorn-configs"
	DefaultPolicyGroup       = "queues.yaml"
)
