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
	ctx "context"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	k8events "k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type KubernetesShim struct {
	apiFactory           client.APIProvider
	context              *cache.Context
	phManager            *cache.PlaceholderManager
	callback             api.ResourceManagerCallback
	stopChan             chan struct{}
	lock                 *locking.RWMutex
	outstandingAppsFound bool
}

const (
	AppHandler  string = "ShimAppHandler"
	TaskHandler string = "ShimTaskHandler"
)

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
	rmCallback := cache.NewAsyncRMCallback(context)

	eventBroadcaster := k8events.NewBroadcaster(&k8events.EventSinkImpl{
		Interface: kubeClient.GetClientSet().EventsV1()})
	err := eventBroadcaster.StartRecordingToSinkWithContext(ctx.Background())
	if err != nil {
		log.Log(log.Shim).Error("Could not create event broadcaster",
			zap.Error(err))
	} else {
		eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, constants.SchedulerName)
		events.SetRecorder(eventRecorder)
	}

	return newShimSchedulerInternal(context, apiFactory, rmCallback)
}

func NewShimSchedulerForPlugin(scheduler api.SchedulerAPI, informerFactory informers.SharedInformerFactory, configs *conf.SchedulerConf, bootstrapConfigMaps []*v1.ConfigMap) *KubernetesShim {
	apiFactory := client.NewAPIFactory(scheduler, informerFactory, configs, false)
	context := cache.NewContextWithBootstrapConfigMaps(apiFactory, bootstrapConfigMaps)
	utils.SetPluginMode(true)
	rmCallback := cache.NewAsyncRMCallback(context)
	return newShimSchedulerInternal(context, apiFactory, rmCallback)
}

// this is visible for testing
func newShimSchedulerInternal(ctx *cache.Context, apiFactory client.APIProvider, cb api.ResourceManagerCallback) *KubernetesShim {
	ss := &KubernetesShim{
		apiFactory:           apiFactory,
		context:              ctx,
		phManager:            cache.NewPlaceholderManager(apiFactory.GetAPIs()),
		callback:             cb,
		stopChan:             make(chan struct{}),
		lock:                 &locking.RWMutex{},
		outstandingAppsFound: false,
	}
	// init dispatcher
	dispatcher.RegisterEventHandler(AppHandler, dispatcher.EventTypeApp, ctx.ApplicationEventHandler())
	dispatcher.RegisterEventHandler(TaskHandler, dispatcher.EventTypeTask, ctx.TaskEventHandler())

	return ss
}

func (ss *KubernetesShim) GetContext() *cache.Context {
	return ss.context
}

func (ss *KubernetesShim) initSchedulerState() error {
	log.Log(log.ShimScheduler).Info("initializing scheduler state")
	if err := ss.context.InitializeState(); err != nil {
		log.Log(log.ShimScheduler).Error("failed to initialize scheduler state", zap.Error(err))
		return err
	}
	log.Log(log.ShimScheduler).Info("scheduler state initialized")
	return nil
}

func (ss *KubernetesShim) doScheduling() {
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

// each schedule iteration, we scan all apps and triggers app state transition
func (ss *KubernetesShim) schedule() {
	apps := ss.context.GetAllApplications()
	for _, app := range apps {
		if app.GetApplicationState() == cache.ApplicationStates().Failed {
			if app.AreAllTasksTerminated() {
				ss.context.RemoveApplication(app.GetApplicationID())
			}
			continue
		}

		if app.Schedule() {
			ss.setOutstandingAppsFound(true)
		}
	}
}

func (ss *KubernetesShim) Run() error {
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

	// register shim with core
	if err := ss.registerShimLayer(); err != nil {
		log.Log(log.ShimScheduler).Error("failed to register shim with core", zap.Error(err))
		ss.Stop()
		return err
	}

	// initialize scheduler state
	if err := ss.initSchedulerState(); err != nil {
		log.Log(log.ShimScheduler).Error("failed to initialize scheduler state", zap.Error(err))
		ss.Stop()
		return err
	}

	// start scheduling loop
	ss.doScheduling()

	return nil
}

func (ss *KubernetesShim) Stop() {
	log.Log(log.ShimScheduler).Info("stopping scheduler")
	select {
	case ss.stopChan <- struct{}{}:
		// stop the dispatcher
		dispatcher.Stop()
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
