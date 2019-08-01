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
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/callback"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"sync"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type KubernetesShim struct {
	rmProxy      api.SchedulerApi
	context      *cache.Context
	callback     api.ResourceManagerCallback
	stateMachine *fsm.FSM
	dispatcher   *dispatcher.Dispatcher
	lock         *sync.Mutex
}

func newShimScheduler(api api.SchedulerApi, configs *conf.SchedulerConf) *KubernetesShim {
	context := cache.NewContext(api, configs)
	rmCallback := callback.NewAsyncRMCallback(context)
	return newShimSchedulerInternal(api, context, rmCallback)
}

// this is visible for testing
func newShimSchedulerInternal(api api.SchedulerApi, ctx *cache.Context, cb api.ResourceManagerCallback) *KubernetesShim {
	var states = events.States().Scheduler
	ss := &KubernetesShim{
		rmProxy:      api,
		context:      ctx,
		callback:     cb,
	}

	// init dispatcher
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, ctx.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, ctx.TaskEventHandler())

	// init state machine
	ss.stateMachine = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: string(RegisterScheduler),
				Src: []string{states.New},
				Dst: states.Registered},
		},
		fsm.Callbacks{
			string(RegisterScheduler): ss.register(),
		},
	)

	return ss
}

func (ss *KubernetesShim) register() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		if err := ss.registerShimLayer(); err != nil {
			log.Logger.Fatal("failed to register to yunikorn-core", zap.Error(err))
		}
	}
}

func (ss *KubernetesShim) registerShimLayer() error {
	configuration := conf.GetSchedulerConf()
	registerMessage := si.RegisterResourceManagerRequest{
		RmId:        configuration.ClusterId,
		Version:     configuration.ClusterVersion,
		PolicyGroup: configuration.PolicyGroup,
	}

	log.Logger.Info("register RM to the scheduler",
		zap.String("clusterId", configuration.ClusterId),
		zap.String("clusterVersion", configuration.ClusterVersion),
		zap.String("policyGroup", configuration.PolicyGroup))
	if _, err := ss.rmProxy.RegisterResourceManager(&registerMessage, ss.callback); err != nil {
		return err
	}

	return nil
}

func (ss *KubernetesShim) GetSchedulerState() string {
	return ss.stateMachine.Current()
}

// event handling
func (ss *KubernetesShim) Handle(se SchedulerEvent) error {
	log.Logger.Info("shim-scheduler state transition",
		zap.String("preState", ss.stateMachine.Current()),
		zap.String("pending event", string(se.GetEvent())))
	err := ss.stateMachine.Event(string(se.GetEvent()))
	log.Logger.Info("shim-scheduler state transition",
		zap.String("postState", ss.stateMachine.Current()),
		zap.String("handled event", string(se.GetEvent())))
	return err
}

// each schedule iteration, we scan all apps and triggers app state transition
func (ss *KubernetesShim) schedule() {
	apps := ss.context.SelectApplications(nil)
	for _, app := range apps {
		for _, pendingTask := range app.GetPendingTasks() {
			var states = events.States().Application
			log.Logger.Info("scheduling",
				zap.String("app", app.GetApplicationId()),
				zap.String("pendingTask", pendingTask.GetTaskPod().Name))
			switch app.GetApplicationState() {
			case states.New:
				ev := cache.NewSubmitApplicationEvent(app.GetApplicationId())
				dispatcher.Dispatch(ev)
			case states.Accepted:
				ev := cache.NewRunApplicationEvent(app.GetApplicationId(), pendingTask)
				dispatcher.Dispatch(ev)
			case states.Running:
				ev := cache.NewRunApplicationEvent(app.GetApplicationId(), pendingTask)
				dispatcher.Dispatch(ev)
			default:
				log.Logger.Warn("application is in unexpected state, tasks cannot be scheduled under this state",
					zap.String("appId", app.GetApplicationId()),
					zap.String("appState", app.GetApplicationState()),
					zap.String("desiredState", events.States().Application.Running))
			}
		}
	}
}

func (ss *KubernetesShim) run(stopChan chan struct{}) {
	// first register to scheduler
	if err := ss.Handle(newRegisterSchedulerEvent()); err != nil {
		panic(fmt.Sprintf("state transition failed, error %s", err.Error()))
	}

	// run dispatcher
	dispatcher.Start()

	ss.context.Run(stopChan)
	go wait.Until(ss.schedule, conf.GetSchedulerConf().GetSchedulingInterval(), stopChan)
}
