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
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/shim"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

var (
	version   string
	date      string
	goVersion string
	arch      string
	coreSHA   string
	siSHA     string
	shimSHA   string
)

func main() {
	conf.BuildVersion = version
	conf.BuildDate = date
	conf.IsPluginVersion = false
	conf.GoVersion = goVersion
	conf.Arch = arch
	conf.CoreSHA = coreSHA
	conf.SiSHA = siSHA
	conf.ShimSHA = shimSHA

	log.Log(log.Shim).Info(fmt.Sprintf("Build info: version=%s date=%s isPluginVersion=%t goVersion=%s arch=%s coreSHA=%s siSHA=%s shimSHA=%s", version, date, false, goVersion, arch, coreSHA, siSHA, shimSHA))

	configMaps, err := client.LoadBootstrapConfigMaps(conf.GetSchedulerNamespace())
	if err != nil {
		log.Log(log.Shim).Fatal("Unable to bootstrap configuration", zap.Error(err))
	}

	err = conf.UpdateConfigMaps(configMaps, true)
	if err != nil {
		log.Log(log.Shim).Fatal("Unable to load initial configmaps", zap.Error(err))
	}

	log.Log(log.Shim).Info("Starting scheduler", zap.String("name", constants.SchedulerName))
	serviceContext := entrypoint.StartAllServicesWithLogger(log.RootLogger(), log.GetZapConfigs())

	if sa, ok := serviceContext.RMProxy.(api.SchedulerAPI); ok {
		ss := shim.NewShimScheduler(sa, conf.GetSchedulerConf(), configMaps)
		ss.Run()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		for range signalChan {
			log.Log(log.Shim).Info("Shutdown signal received, exiting...")
			ss.Stop()
			os.Exit(0)
		}
	}
}
