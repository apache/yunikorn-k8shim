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
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/callback"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type KubernetesShim struct {
	apiFactory   client.APIProvider
	context      *cache.Context
	appManager   *appmgmt.AppManagementService
	callback     api.ResourceManagerCallback
	stateMachine *fsm.FSM
	stopChan     chan struct{}
	lock         *sync.RWMutex
}

func newShimScheduler(scheduler api.SchedulerAPI, configs *conf.SchedulerConf) *KubernetesShim {
	apiFactory := client.NewAPIFactory(scheduler, configs, false)
	context := cache.NewContext(apiFactory)
	rmCallback := callback.NewAsyncRMCallback(context)
	appManager := appmgmt.NewAMService(context, apiFactory)
	return newShimSchedulerInternal(context, apiFactory, appManager, rmCallback)
}

// this is visible for testing
func newShimSchedulerInternal(ctx *cache.Context, apiFactory client.APIProvider,
	am *appmgmt.AppManagementService, cb api.ResourceManagerCallback) *KubernetesShim {
	var states = events.States().Scheduler
	ss := &KubernetesShim{
		apiFactory: apiFactory,
		context:    ctx,
		appManager: am,
		callback:   cb,
		stopChan:   make(chan struct{}),
		lock:       &sync.RWMutex{},
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
			string(events.RegisterScheduler):       ss.register,                      // trigger registration
			string(events.RegisterSchedulerFailed): ss.handleSchedulerFailure,        // registration failed, stop the scheduler
			string(events.RecoverSchedulerFailed):  ss.handleSchedulerFailure,        // recovery failed
			string(states.Registered):              ss.triggerSchedulerStateRecovery, // if reaches registered, trigger recovering
			string(states.Recovering):              ss.recoverSchedulerState,         // do recovering
			string(states.Running):                 ss.doScheduling,                  // do scheduling
			events.EnterState:                      ss.enterState,
		},
	)

	// init dispatcher
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, ctx.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, ctx.TaskEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, ctx.SchedulerNodeEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeScheduler, ss.SchedulerEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeAppStatus, am.ApplicationStateUpdateEventHandler())

	return ss
}

func (ss *KubernetesShim) SchedulerEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.SchedulerEvent); ok {
			if ss.canHandle(event) {
				if err := ss.handle(event); err != nil {
					log.Logger().Error("failed to handle scheduler event",
						zap.String("event", string(event.GetEvent())),
						zap.Error(err))
				}
			}
		}
	}
}

func (ss *KubernetesShim) register(e *fsm.Event) {
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

func (ss *KubernetesShim) handleSchedulerFailure(e *fsm.Event) {
	ss.stop()
}

func (ss *KubernetesShim) triggerSchedulerStateRecovery(e *fsm.Event) {
	dispatcher.Dispatch(ShimSchedulerEvent{
		event: events.RecoverScheduler,
	})
}

func (ss *KubernetesShim) recoverSchedulerState(e *fsm.Event) {
	// run recovery process in a go routine
	// do not block main thread
	go func() {
		log.Logger().Info("recovering scheduler states")
		// step 1: recover all applications
		// this step, we collect all the existing allocated pods from api-server,
		// identify the scheduling identity (aka applicationInfo) from the pod,
		// and then add these applications to the scheduler.
		if err := ss.appManager.WaitForRecovery(3 * time.Minute); err != nil {
			// failed
			log.Logger().Error("scheduler recovery failed", zap.Error(err))
			dispatcher.Dispatch(ShimSchedulerEvent{
				event: events.RecoverSchedulerFailed,
			})
			return
		}

		// step 2: recover existing allocations
		// this step, we collect all existing allocations (allocated pods) from api-server,
		// rerun the scheduling for these allocations in order to restore scheduler-state,
		// the rerun is like a replay, not a actual scheduling procedure.
		recoverableAppManagers := make([]interfaces.Recoverable, 0)
		for _, appMgr := range ss.appManager.GetAllManagers() {
			if m, ok := appMgr.(interfaces.Recoverable); ok {
				recoverableAppManagers = append(recoverableAppManagers, m)
			}
		}
		if err := ss.context.WaitForRecovery(recoverableAppManagers, 3*time.Minute); err != nil {
			// failed
			log.Logger().Error("scheduler recovery failed", zap.Error(err))
			dispatcher.Dispatch(ShimSchedulerEvent{
				event: events.RecoverSchedulerFailed,
			})
			return
		}

		// success
		log.Logger().Info("scheduler recovery succeed")
		dispatcher.Dispatch(ShimSchedulerEvent{
			event: events.RecoverSchedulerSucceed,
		})
	}()
}

func (ss *KubernetesShim) doScheduling(e *fsm.Event) {
	// add event handlers to the context
	ss.context.AddSchedulingEventHandlers()

	// run main scheduling loop
	go wait.Until(ss.schedule, conf.GetSchedulerConf().GetSchedulingInterval(), ss.stopChan)
}

func (ss *KubernetesShim) registerShimLayer() error {
	configuration := conf.GetSchedulerConf()
	registerMessage := si.RegisterResourceManagerRequest{
		RmID:        configuration.ClusterID,
		Version:     configuration.ClusterVersion,
		PolicyGroup: configuration.PolicyGroup,
	}

	log.Logger().Info("register RM to the scheduler",
		zap.String("clusterID", configuration.ClusterID),
		zap.String("clusterVersion", configuration.ClusterVersion),
		zap.String("policyGroup", configuration.PolicyGroup))
	if _, err := ss.apiFactory.GetAPIs().SchedulerAPI.
		RegisterResourceManager(&registerMessage, ss.callback); err != nil {
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
	err := ss.stateMachine.Event(string(se.GetEvent()))
	if err != nil && err.Error() == "no transition" {
		return err
	}
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

	// run app managers
	if err := ss.appManager.Start(); err != nil {
		log.Logger().Fatal("failed to start app manager", zap.Error(err))
		ss.stop()
	}

	ss.apiFactory.Start()

	// run the placeholder manager
	cache.NewPlaceholderManager(ss.apiFactory.GetAPIs()).Start()
}

func (ss *KubernetesShim) enterState(event *fsm.Event) {
	log.Logger().Debug("scheduler shim state transition",
		zap.String("source", event.Src),
		zap.String("destination", event.Dst),
		zap.String("event", event.Event))
}

func (ss *KubernetesShim) stop() {
	log.Logger().Info("stopping scheduler")
	select {
	case ss.stopChan <- struct{}{}:
		// stop the dispatcher
		dispatcher.Stop()
		// stop the app manager
		ss.appManager.Stop()
		// stop the placeholder manager
		cache.GetPlaceholderManager().Stop()
	default:
		log.Logger().Info("scheduler is already stopped")
	}
}
