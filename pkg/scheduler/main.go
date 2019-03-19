/*
Copyright 2019 The Unity Scheduler Authors

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
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/conf"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/fsm"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/shim/callback"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/entrypoint"
	"os"
	"os/signal"
	"syscall"
)

var (
	version string
	date    string
)

func main() {
	configs := conf.ParseFromCommandline()
	configs.DumpConfiguration()

	glog.V(3).Infof("******************************************************************")
	glog.V(3).Infof("Build info: version=%s, date=%s", version, date)
	glog.V(3).Infof("******************************************************************")


	glog.V(3).Infof("starting unified-scheduler")
	rmProxy, _, _ := entrypoint.StartAllServices()

	context := state.NewContext(rmProxy, configs)
	callback := callback.NewSimpleRMCallback(context)

	stopChan := make(chan struct{})
	ss := fsm.NewShimScheduler(rmProxy, context, callback)
	ss.Run(stopChan)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			glog.V(3).Infof("Shutdown signal received, exiting...")
			close(stopChan)
			os.Exit(0)
		}
	}
}
