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

package main

import (
	"github.com/cloudera/yunikorn-core/pkg/entrypoint"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-k8shim/pkg/scheduler/shim"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

var (
	version string
	date    string
)

func main() {
	log.Logger.Info("Build info", zap.String("version", version), zap.String("date", date))
	log.Logger.Info("starting scheduler",
		zap.String("name", conf.GetSchedulerConf().SchedulerName))

	serviceContext := entrypoint.StartAllServices()

	stopChan := make(chan struct{})
	ss := shim.NewShimScheduler(serviceContext.RMProxy, conf.GetSchedulerConf())
	ss.Run(stopChan)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			log.Logger.Info("Shutdown signal received, exiting...")
			close(stopChan)
			os.Exit(0)
		}
	}
}
