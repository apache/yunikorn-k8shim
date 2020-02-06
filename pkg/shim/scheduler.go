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
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/callback"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type KubernetesShim struct {
	rmProxy      api.SchedulerAPI
	context      *cache.Context
	callback     api.ResourceManagerCallback
	stateMachine *fsm.FSM
	stopChan     chan struct{}
	lock         *sync.RWMutex
}

func newShimScheduler(api api.SchedulerAPI, configs *conf.SchedulerConf) *KubernetesShim {
	context := cache.NewContext(api, configs)
	rmCallback := callback.NewAsyncRMCallback(context)
	return newShimSchedulerInternal(api, context, rmCallback)
}

// this is visible for testing
func newShimSchedulerInternal(api api.SchedulerAPI, ctx *cache.Context, cb api.ResourceManagerCallback) *KubernetesShim {
	var states = events.States().Scheduler
	ss := &KubernetesShim{
		rmProxy:  api,
		context:  ctx,
		callback: cb,
		stopChan: make(chan struct{}),
		lock:     &sync.RWMutex{},
	}

	// init state machine
	ss.stateMachine = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: string(events.RegisterScheduler),
				Src: []string{states.New},
				Dst: states.Registering},
			{Name: string(events.RegisterSchedulerSucceed),
				Src: []string{states.Registering},
				Dst: states.Registered},
			{Name: string(events.RegisterSchedulerFailed),
				Src: []string{states.Registering},
				Dst: states.Stopped},
			{Name: string(events.RecoverScheduler),
				Src: []string{states.Registered},
				Dst: states.Recovering},
			{Name: string(events.RecoverSchedulerSucceed),
				Src: []string{states.Recovering},
				Dst: states.Running},
			{Name: string(events.RecoverSchedulerFailed),
				Src: []string{states.Recovering},
				Dst: states.Stopped},
		},
		fsm.Callbacks{
			string(events.RegisterScheduler):       ss.register(),                      // trigger registration
			string(events.RegisterSchedulerFailed): ss.handleSchedulerFailure(),        // registration failed, stop the scheduler
			string(states.Registered):              ss.triggerSchedulerStateRecovery(), // if reaches registered, trigger recovering
			string(states.Recovering):              ss.recoverSchedulerState(),         // do recovering
			string(states.Running):                 ss.doScheduling(),                  // do scheduling
		},
	)

	// init dispatcher
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, ctx.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, ctx.TaskEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, ctx.SchedulerNodeEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeScheduler, ss.SchedulerEventHandler())

	return ss
}

func (ss *KubernetesShim) SchedulerEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.SchedulerEvent); ok {
			if ss.canHandle(event) {
				if err := ss.handle(event); err != nil {
					log.Logger.Error("failed to handle scheduler event",
						zap.String("event", string(event.GetEvent())),
						zap.Error(err))
				}
			}
		}
	}
}

func (ss *KubernetesShim) register() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		if err := ss.registerShimLayer(); err != nil {
			dispatcher.Dispatch(ShimSchedulerEvent{
				event: events.RegisterSchedulerFailed,
			})
		} else {
			dispatcher.Dispatch(ShimSchedulerEvent{
				event: events.RegisterSchedulerSucceed,
			})
		}
	}
}

func (ss *KubernetesShim) handleSchedulerFailure() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		ss.stop()
	}
}

func (ss *KubernetesShim) triggerSchedulerStateRecovery() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		dispatcher.Dispatch(ShimSchedulerEvent{
			event: events.RecoverScheduler,
		})
	}
}

func (ss *KubernetesShim) recoverSchedulerState() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		// run recovery process in a go routine
		// do not block main thread
		go func() {
			log.Logger.Info("recovering scheduler states")
			// wait for recovery, max timeout 3 minutes
			if err := ss.context.WaitForRecovery(3 * time.Minute); err != nil {
				// failed
				log.Logger.Fatal("scheduler recovery failed", zap.Error(err))
				dispatcher.Dispatch(ShimSchedulerEvent{
					event: events.RecoverSchedulerFailed,
				})
			} else {
				// success
				log.Logger.Info("scheduler recovery succeed")
				dispatcher.Dispatch(ShimSchedulerEvent{
					event: events.RecoverSchedulerSucceed,
				})
			}
		}()
	}
}

func (ss *KubernetesShim) doScheduling() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		// add event handlers to the context
		ss.context.AddSchedulingEventHandlers()

		// run main scheduling loop
		go wait.Until(ss.schedule, conf.GetSchedulerConf().GetSchedulingInterval(), ss.stopChan)
	}
}

func (ss *KubernetesShim) registerShimLayer() error {
	configuration := conf.GetSchedulerConf()
	registerMessage := si.RegisterResourceManagerRequest{
		RmID:        configuration.ClusterID,
		Version:     configuration.ClusterVersion,
		PolicyGroup: configuration.PolicyGroup,
	}

	log.Logger.Info("register RM to the scheduler",
		zap.String("clusterID", configuration.ClusterID),
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
func (ss *KubernetesShim) handle(se events.SchedulerEvent) error {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	log.Logger.Info("shim-scheduler state transition",
		zap.String("preState", ss.stateMachine.Current()),
		zap.String("pending event", string(se.GetEvent())))
	err := ss.stateMachine.Event(string(se.GetEvent()))
	if err != nil && err.Error() == "no transition" {
		return err
	}
	log.Logger.Info("shim-scheduler state transition",
		zap.String("postState", ss.stateMachine.Current()))
	return nil
}

func (ss *KubernetesShim) canHandle(se events.SchedulerEvent) bool {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.stateMachine.Can(string(se.GetEvent()))
}

// each schedule iteration, we scan all apps and triggers app state transition
func (ss *KubernetesShim) schedule() {
	apps := ss.context.SelectApplications(nil)
	for _, app := range apps {
		app.Schedule()
	}
}

func (ss *KubernetesShim) run() {
	// run dispatcher
	dispatcher.Start()

	// register scheduler with scheduler core
	dispatcher.Dispatch(newRegisterSchedulerEvent())

	// run context
	ss.context.Run(ss.stopChan)
}

func (ss *KubernetesShim) stop() {
	log.Logger.Info("stopping scheduler")
	select {
	case ss.stopChan <- struct{}{}:
		// stop the dispatcher
		dispatcher.Stop()
	default:
		log.Logger.Info("scheduler is already stopped")
	}
}
