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

	YKScheduler   = "yunikorn-scheduler"
	SchedulerName = "yunikorn"
	YKAdmCtrl     = "yunikorn-admission-controller"
	// YuniKorn service name
	YKSvcName = "yunikorn-service"

	//YuniKorn Admission controller serivce name
	YKAdmCtrlName = "yunikorn-admission-controller-service"

	// REST endpoints of YuniKorn
	QueuesPath   = "ws/v1/queues"
	AppsPath     = "ws/v1/apps"
	ClustersPath = "ws/v1/clusters"

	//YuniKorn Service Details
	DefaultYuniKornHost   = "localhost"
	DefaultYuniKornPort   = "9080"
	DefaultYuniKornScheme = "http"

	DefaultYuniKornConfigMap = "yunikorn-configs"
	DefaultPolicyGroup       = "queues.yaml"
)
