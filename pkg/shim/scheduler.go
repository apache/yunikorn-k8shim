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

package shim

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/callback"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type KubernetesShim struct {
	apiFactory           client.APIProvider
	context              *cache.Context
	appManager           *appmgmt.AppManagementService
	phManager            *cache.PlaceholderManager
	callback             api.ResourceManagerCallback
	stateMachine         *fsm.FSM
	stopChan             chan struct{}
	lock                 *sync.RWMutex
	outstandingAppsFound bool
}

var (
	// timeout for logging a message if no outstanding apps were found for scheduling
	outstandingAppLogTimeout = 2 * time.Minute
)

func NewShimScheduler(scheduler api.SchedulerAPI, configs *conf.SchedulerConf, bootstrapConfigMaps []*v1.ConfigMap) *KubernetesShim {
	kubeClient := client.NewKubeClient(configs.KubeConfig)

	// we have disabled re-sync to keep ourselves up-to-date
	informerFactory := informers.NewSharedInformerFactory(kubeClient.GetClientSet(), 0)

	apiFactory := client.NewAPIFactory(scheduler, informerFactory, configs, false)
	context := cache.NewContextWithBootstrapConfigMaps(apiFactory, bootstrapConfigMaps)
	rmCallback := callback.NewAsyncRMCallback(context)
	appManager := appmgmt.NewAMService(context, apiFactory)
	return newShimSchedulerInternal(context, apiFactory, appManager, rmCallback)
}

func NewShimSchedulerForPlugin(scheduler api.SchedulerAPI, informerFactory informers.SharedInformerFactory, configs *conf.SchedulerConf, bootstrapConfigMaps []*v1.ConfigMap) *KubernetesShim {
	apiFactory := client.NewAPIFactory(scheduler, informerFactory, configs, false)
	context := cache.NewContextWithBootstrapConfigMaps(apiFactory, bootstrapConfigMaps)
	context.SetPluginMode(true)
	rmCallback := callback.NewAsyncRMCallback(context)
	appManager := appmgmt.NewAMService(context, apiFactory)
	return newShimSchedulerInternal(context, apiFactory, appManager, rmCallback)
}

// this is visible for testing
func newShimSchedulerInternal(ctx *cache.Context, apiFactory client.APIProvider,
	am *appmgmt.AppManagementService, cb api.ResourceManagerCallback) *KubernetesShim {
	ss := &KubernetesShim{
		apiFactory:           apiFactory,
		context:              ctx,
		appManager:           am,
		phManager:            cache.NewPlaceholderManager(apiFactory.GetAPIs()),
		callback:             cb,
		stopChan:             make(chan struct{}),
		lock:                 &sync.RWMutex{},
		outstandingAppsFound: false,
		stateMachine:         newSchedulerState(),
	}
	// init dispatcher
	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, ctx.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeTask, ctx.TaskEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeNode, ctx.SchedulerNodeEventHandler())
	dispatcher.RegisterEventHandler(dispatcher.EventTypeScheduler, ss.SchedulerEventHandler())

	return ss
}

func (ss *KubernetesShim) GetContext() *cache.Context {
	return ss.context
}

func (ss *KubernetesShim) SchedulerEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.SchedulerEvent); ok {
			if ss.canHandle(event) {
				if err := ss.handle(event); err != nil {
					log.Log(log.ShimScheduler).Error("failed to handle scheduler event",
						zap.String("event", event.GetEvent()),
						zap.Error(err))
				}
			}
		}
	}
}

func (ss *KubernetesShim) register() {
	if err := ss.registerShimLayer(); err != nil {
		dispatcher.Dispatch(ShimSchedulerEvent{
			event: RegisterSchedulerFailed,
		})
	} else {
		dispatcher.Dispatch(ShimSchedulerEvent{
			event: RegisterSchedulerSucceed,
		})
	}
}

func (ss *KubernetesShim) handleSchedulerFailure() {
	ss.Stop()
	// testmode will be true when mock scheduler intailize
	if !conf.GetSchedulerConf().IsTestMode() {
		os.Exit(1)
	}
}

func (ss *KubernetesShim) triggerSchedulerStateRecovery() {
	dispatcher.Dispatch(ShimSchedulerEvent{
		event: RecoverScheduler,
	})
}

func (ss *KubernetesShim) recoverSchedulerState() {
	// run recovery process in a go routine
	// do not block main thread
	go func() {
		log.Log(log.ShimScheduler).Info("recovering scheduler states")
		// step 1: recover all applications
		// this step, we collect all the existing allocated pods from api-server,
		// identify the scheduling identity (aka applicationInfo) from the pod,
		// and then add these applications to the scheduler.
		if err := ss.appManager.WaitForRecovery(); err != nil {
			// failed
			log.Log(log.ShimScheduler).Error("scheduler recovery failed", zap.Error(err))
			dispatcher.Dispatch(ShimSchedulerEvent{
				event: RecoverSchedulerFailed,
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
		if err := ss.context.WaitForRecovery(recoverableAppManagers, 5*time.Minute); err != nil {
			// failed
			log.Log(log.ShimScheduler).Error("scheduler recovery failed", zap.Error(err))
			dispatcher.Dispatch(ShimSchedulerEvent{
				event: RecoverSchedulerFailed,
			})
			return
		}

		// success
		log.Log(log.ShimScheduler).Info("scheduler recovery succeed")
		dispatcher.Dispatch(ShimSchedulerEvent{
			event: RecoverSchedulerSucceed,
		})
	}()
}

func (ss *KubernetesShim) doScheduling() {
	// add event handlers to the context
	ss.context.AddSchedulingEventHandlers()

	// run main scheduling loop
	go wait.Until(ss.schedule, conf.GetSchedulerConf().GetSchedulingInterval(), ss.stopChan)
	// log a message if no outstanding requests were found for a while
	go wait.Until(ss.checkOutstandingApps, outstandingAppLogTimeout, ss.stopChan)
}

func (ss *KubernetesShim) registerShimLayer() error {
	configuration := conf.GetSchedulerConf()

	buildInfoMap := conf.GetBuildInfoMap()

	configMaps, err := ss.context.LoadConfigMaps()
	if err != nil {
		log.Log(log.ShimScheduler).Error("failed to load yunikorn configmaps", zap.Error(err))
		return err
	}

	confMap := conf.FlattenConfigMaps(configMaps)
	config := utils.GetCoreSchedulerConfigFromConfigMap(confMap)
	extraConfig := utils.GetExtraConfigFromConfigMap(confMap)

	registerMessage := si.RegisterResourceManagerRequest{
		RmID:        configuration.ClusterID,
		Version:     configuration.ClusterVersion,
		PolicyGroup: configuration.PolicyGroup,
		BuildInfo:   buildInfoMap,
		Config:      config,
		ExtraConfig: extraConfig,
	}

	log.Log(log.ShimScheduler).Info("register RM to the scheduler",
		zap.String("clusterID", configuration.ClusterID),
		zap.String("clusterVersion", configuration.ClusterVersion),
		zap.String("policyGroup", configuration.PolicyGroup),
		zap.Any("buildInfo", buildInfoMap))
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
	err := ss.stateMachine.Event(context.Background(), se.GetEvent(), ss)
	if err != nil && err.Error() == "no transition" {
		return err
	}
	return nil
}

func (ss *KubernetesShim) canHandle(se events.SchedulerEvent) bool {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.stateMachine.Can(se.GetEvent())
}

// each schedule iteration, we scan all apps and triggers app state transition
func (ss *KubernetesShim) schedule() {
	apps := ss.context.GetAllApplications()
	for _, app := range apps {
		if app.Schedule() {
			ss.setOutstandingAppsFound(true)
		}
	}
}

func (ss *KubernetesShim) Run() {
	// NOTE: the order of starting these services matter,
	// please look at the comments before modifying the orders

	// run dispatcher
	// the dispatcher handles the basic event dispatching,
	// it needs to be started at first
	dispatcher.Start()

	// run the placeholder manager
	ss.phManager.Start()

	// run the client library code that communicates with Kubernetes
	ss.apiFactory.Start()

	// register scheduler with scheduler core
	// this triggers the scheduler state transition
	// it first registers with the core, then start to do recovery,
	// after the recovery is succeed, it goes to the normal scheduling routine
	dispatcher.Dispatch(newRegisterSchedulerEvent())

	// run app managers
	// the app manager launches the pod event handlers
	// it needs to be started after the shim is registered with the core
	if err := ss.appManager.Start(); err != nil {
		log.Log(log.ShimScheduler).Fatal("failed to start app manager", zap.Error(err))
		ss.Stop()
	}
}

func (ss *KubernetesShim) Stop() {
	log.Log(log.ShimScheduler).Info("stopping scheduler")
	select {
	case ss.stopChan <- struct{}{}:
		// stop the dispatcher
		dispatcher.Stop()
		// stop the app manager
		ss.appManager.Stop()
		// stop the placeholder manager
		ss.phManager.Stop()
	default:
		log.Log(log.ShimScheduler).Info("scheduler is already stopped")
	}
}

func (ss *KubernetesShim) checkOutstandingApps() {
	if !ss.getOutstandingAppsFound() {
		log.Log(log.ShimScheduler).Info("No outstanding apps found for a while", zap.Duration("timeout", outstandingAppLogTimeout))
		return
	}
	ss.setOutstandingAppsFound(false)
}

func (ss *KubernetesShim) getOutstandingAppsFound() bool {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.outstandingAppsFound
}

func (ss *KubernetesShim) setOutstandingAppsFound(value bool) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	ss.outstandingAppsFound = value
}
