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

package shim

import (
	"fmt"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-k8shim/pkg/scheduler/callback"
	"github.com/cloudera/yunikorn-k8shim/pkg/state"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-core/pkg/rmproxy"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"sync"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type ShimScheduler struct {
	rmProxy    *rmproxy.RMProxy
	context    *state.Context
	callback   api.ResourceManagerCallback
	sm         *fsm.FSM
	events     *SchedulerEvents
	dispatcher *state.Dispatcher
	lock       *sync.Mutex
}

func NewShimScheduler(p *rmproxy.RMProxy, configs *conf.SchedulerConf) *ShimScheduler {
	context := state.NewContext(p, configs)
	callback := callback.NewAsyncRMCallback(context)
	return newShimScheduler(p, context, callback)
}

// this is visible for testing
func newShimScheduler(p *rmproxy.RMProxy, ctx *state.Context, cb api.ResourceManagerCallback) *ShimScheduler {
	var events = InitiateEvents()
	ss := &ShimScheduler{
		rmProxy:  p,
		context:  ctx,
		callback: cb,
		events:   events,
	}

	// init dispatcher
	ss.dispatcher = state.GetDispatcher()
	ss.dispatcher.SetContext(ctx)

	// init state machine
	var states = state.States().Scheduler
	ss.sm = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: events.Register.event,
				Src: []string{states.New},
				Dst: states.Registered},
			{Name: events.RefreshCache.event,
				Src: []string{states.New, states.Running},
				Dst: states.Running},
			//{Name: "reject", Src: []string{"new"}, Dst: "rejected"},
		},
		fsm.Callbacks{
			events.Register.event: ss.register(),
			events.RefreshCache.event: ss.refreshCache(),
		},
	)
	return ss
}

func (ss *ShimScheduler) refreshCache() func(e *fsm.Event) {
	return func(e *fsm.Event) {

	}
}

func (ss *ShimScheduler) register() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		if err := ss.registerShimLayer(); err != nil {
			log.Logger.Fatal("failed to register to yunikorn-core", zap.Error(err))
		}
	}
}

func (ss *ShimScheduler) registerShimLayer() error {
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

func (ss *ShimScheduler) GetSchedulerState() string {
	return ss.sm.Current()
}

func (ss *ShimScheduler) GetContext() *state.Context {
	return ss.context
}

// event handling
func (ss *ShimScheduler) Handle(se SchedulerEvent) error {
	log.Logger.Info("shim-scheduler state transition",
		zap.String("preState", ss.sm.Current()),
		zap.String("pending event", se.event))
	err := ss.sm.Event(se.event)
	log.Logger.Info("shim-scheduler state transition",
		zap.String("postState", ss.sm.Current()),
		zap.String("handled event", se.event))
	return err
}

// each schedule iteration, we scan all apps and triggers app state transition
func (ss *ShimScheduler) schedule() {
	apps := ss.context.SelectApplications(nil)
	for _, app := range apps {
		for _, pendingTask := range app.GetPendingTasks() {
			var states = state.States().Application
			log.Logger.Info("scheduling",
				zap.String("app", app.GetApplicationId()),
				zap.String("pendingTask", pendingTask.GetTaskPod().Name))
			switch app.GetApplicationState() {
			case states.New:
				ev := state.NewSubmitApplicationEvent(app.GetApplicationId())
				state.GetDispatcher().Dispatch(ev)
			case states.Accepted:
				ev := state.NewRunApplicationEvent(app.GetApplicationId(), pendingTask)
				state.GetDispatcher().Dispatch(ev)
			case states.Running:
				ev := state.NewRunApplicationEvent(app.GetApplicationId(), pendingTask)
				state.GetDispatcher().Dispatch(ev)
			default:
				log.Logger.Warn("application is in unexpected state, tasks cannot be scheduled under this state",
					zap.String("appId", app.GetApplicationId()),
					zap.String("appState", app.GetApplicationState()),
					zap.String("desiredState", state.States().Application.Running))
			}
		}
	}

}

func (ss *ShimScheduler) Run(stopChan chan struct{}) {
	// first register to scheduler
	if err := ss.Handle(ss.events.Register); err != nil {
		panic(fmt.Sprintf("state transition failed, error %s", err.Error()))
	}

	// run dispatcher
	ss.dispatcher.Start()

	ss.context.Run(stopChan)
	go wait.Until(ss.schedule, conf.GetSchedulerConf().GetSchedulingInterval(), stopChan)
}
