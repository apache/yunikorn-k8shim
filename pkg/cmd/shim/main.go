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

package main

import (
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/shim"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/api"
)

var (
	version string
	date    string
)

func main() {
	log.Logger().Info("Build info", zap.String("version", version), zap.String("date", date))
	log.Logger().Info("starting scheduler",
		zap.String("name", constants.SchedulerName))

	conf.BuildVersion = version
	conf.BuildDate = date
	conf.IsPluginVersion = false

	serviceContext := entrypoint.StartAllServicesWithLogger(log.Logger(), log.GetZapConfigs())

	if sa, ok := serviceContext.RMProxy.(api.SchedulerAPI); ok {
		ss := shim.NewShimScheduler(sa, conf.GetSchedulerConf())
		ss.Run()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		for range signalChan {
			log.Logger().Info("Shutdown signal received, exiting...")
			ss.Stop()
			os.Exit(0)
		}
	}
}
