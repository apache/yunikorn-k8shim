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

package state

import (
	"fmt"
	"github.com/cloudera/k8s-shim/pkg/client"
	"github.com/cloudera/k8s-shim/pkg/common"
	"github.com/cloudera/k8s-shim/pkg/conf"
	"github.com/cloudera/k8s-shim/pkg/log"
	plugin "github.com/cloudera/k8s-shim/pkg/predicates"
	"github.com/cloudera/k8s-shim/pkg/scheduler/controller"
	schedulercache "github.com/cloudera/k8s-shim/pkg/state/cache"
	"github.com/cloudera/k8s-shim/pkg/utils"
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-core/pkg/rmproxy"
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	coreInfomerV1 "k8s.io/client-go/informers/core/v1"
	storageInformerV1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"
	"sync"
)

// context maintains scheduling state, like apps and apps' tasks.
type Context struct {
	applications   map[string]*Application
	nodeController *controller.NodeController
	conf           *conf.SchedulerConf
	kubeClient     client.KubeClient
	schedulerApi   api.SchedulerApi

	// informers
	podInformer       coreInfomerV1.PodInformer
	nodeInformer      coreInfomerV1.NodeInformer
	configMapInformer coreInfomerV1.ConfigMapInformer
	// nsInformer       coreInfomerV1.NamespaceInformer
	pvInformer      coreInfomerV1.PersistentVolumeInformer
	pvcInformer     coreInfomerV1.PersistentVolumeClaimInformer
	storageInformer storageInformerV1.StorageClassInformer

	volumeBinder *volumebinder.VolumeBinder

	schedulerCache *schedulercache.SchedulerCache
	predictor      *plugin.Predictor

	testMode bool
	lock     *sync.RWMutex
}

// Create a new context for the scheduler.
// This wraps the internal call which really creates the context.
func NewContext(scheduler api.SchedulerApi, configs *conf.SchedulerConf) *Context {
	kc := client.NewKubeClient(configs.KubeConfig)
	return NewContextInternal(scheduler, configs, kc, false)
}

// Internal create of the scheduler context.
// Only exposed for testing, not to e used for anything else
func NewContextInternal(scheduler api.SchedulerApi, configs *conf.SchedulerConf, client client.KubeClient, testMode bool) *Context {
	// create the context note that order is important:
	// volumebinder needs the informers
	// the cache needs informers and volumebinder
	// nodecontroller needs the cache
	// predictor need the cache, volumebinder and informers
	ctx := &Context{
		applications: make(map[string]*Application),
		conf:         configs,
		kubeClient:   client,
		schedulerApi: scheduler,
		testMode:     testMode,
		lock:         &sync.RWMutex{},
	}

	// we have disabled re-sync to keep ourselves up-to-date
	informerFactory := informers.NewSharedInformerFactory(ctx.kubeClient.GetClientSet(), 0)

	// init informers
	// volume informers are also used to get the Listers for the predicates
	ctx.nodeInformer = informerFactory.Core().V1().Nodes()
	ctx.podInformer = informerFactory.Core().V1().Pods()
	ctx.configMapInformer = informerFactory.Core().V1().ConfigMaps()
	ctx.storageInformer = informerFactory.Storage().V1().StorageClasses()
	ctx.pvInformer = informerFactory.Core().V1().PersistentVolumes()
	ctx.pvcInformer = informerFactory.Core().V1().PersistentVolumeClaims()

	// create a volume binder (needs the informers)
	// TODO timeout is only used in the bind so for now just set a value
	ctx.volumeBinder = volumebinder.NewVolumeBinder(ctx.kubeClient.GetClientSet(), ctx.nodeInformer, ctx.pvcInformer, ctx.pvInformer, ctx.storageInformer, 10)

	// create the cache
	ctx.schedulerCache = schedulercache.NewSchedulerCache(ctx.pvInformer.Lister(), ctx.pvcInformer.Lister(), ctx.storageInformer.Lister(), ctx.volumeBinder)

	// init the controllers and plugins (need the cache)
	ctx.nodeController = controller.NewNodeController(scheduler, ctx.schedulerCache)
	ctx.predictor = plugin.NewPredictor(schedulercache.GetPluginArgs(), testMode)

	// Add the event handling to all informers that need it
	ctx.nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctx.addNode,
			UpdateFunc: ctx.updateNode,
			DeleteFunc: ctx.deleteNode,
		},
		0,
	)

	ctx.podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: ctx.filterPods,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    ctx.addPod,
				UpdateFunc: ctx.updatePod,
				DeleteFunc: ctx.deletePod,
			},
		})

	ctx.podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: ctx.filterAssignedPods,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    ctx.addPodToCache,
				UpdateFunc: ctx.updatePodInCache,
				DeleteFunc: ctx.removePodFromCache,
			},
		},
	)

	ctx.configMapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: ctx.filterConfigMaps,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    ctx.addConfigMaps,
			UpdateFunc: ctx.updateConfigMaps,
			DeleteFunc: ctx.deleteConfigMaps,
		},
	})

	return ctx
}

func (ctx *Context) addNode(obj interface{}) {
	ctx.nodeController.AddNode(obj)
}

func (ctx *Context) updateNode(oldObj, newObj interface{}) {
	ctx.nodeController.UpdateNode(oldObj, newObj)
}

func (ctx *Context) deleteNode(obj interface{}) {
	ctx.nodeController.DeleteNode(obj)
}

// add a pod to the context
// if pod is valid, we convent the pod to a equivalent task,
// if task belongs to a application, we add this task to the application,
// if task is new, we create a application and add this task to that.
func (ctx *Context) addPod(obj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	pod, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Logger.Error("failed to add pod", zap.Error(err))
		return
	}

	log.Logger.Debug("add pod",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)))
	if pod.Status.Phase == v1.PodPending {
		if err := ctx.validatePod(pod); err != nil {
			log.Logger.Error("application is invalid", zap.Error(err))
			return
		}

		if app := ctx.getOrCreateApplication(pod); app != nil {
			task := ctx.getOrAddTask(app, pod)
			app.AddTask(task)
		}
	}

	// add pod to cache
	if err := ctx.schedulerCache.AddPod(pod); err != nil {
		log.Logger.Error("add pod to scheduler cache failed",
			zap.String("podName", pod.Name),
			zap.Error(err))
	}
}

func (ctx *Context) addPodToCache(obj interface{}) {
	pod, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Logger.Error("failed to add pod to cache", zap.Error(err))
		return
	}

	log.Logger.Info("adding pod to cache", zap.String("podName", pod.Name))
	if err := ctx.schedulerCache.AddPod(pod); err != nil {
		log.Logger.Error("add pod to scheduler cache failed",
			zap.String("podName", pod.Name),
			zap.Error(err))
	}
}

func (ctx *Context) removePodFromCache(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			log.Logger.Error("Cannot convert to *v1.Pod", zap.Any("pod", obj))
			return
		}
	default:
		log.Logger.Error("Cannot convert to *v1.Pod", zap.Any("pod", obj))
		return
	}

	log.Logger.Info("removing pod from cache", zap.String("podName", pod.Name))
	if err := ctx.schedulerCache.RemovePod(pod); err != nil {
		log.Logger.Error("failed to remove pod from scheduler cache",
			zap.String("podName", pod.Name),
			zap.Error(err))
	}
}

// create a task if it doesn't exist yet,
// return the task directly if it is already there in the application
func (ctx *Context) getOrAddTask(a *Application, pod *v1.Pod) *Task {
	// using pod UID as taskId
	if task, err := a.GetTask(string(pod.UID)); err == nil {
		return task
	}
	newTask := CreateTaskFromPod(a, ctx.kubeClient, ctx.schedulerApi, pod)
	a.AddTask(newTask)
	return newTask
}

// validate pod see if it is applicable for the scheduler
func (ctx *Context) validatePod(pod *v1.Pod) error {
	if pod.Spec.SchedulerName == "" || pod.Spec.SchedulerName != ctx.conf.SchedulerName {
		// only pod with specific scheduler name is valid to us
		return fmt.Errorf("only pod whose spec has explicitly "+
			"specified schedulerName=%s is a valid scheduling-target, but schedulerName for pod %s(%s) is %s",
			ctx.conf.SchedulerName, pod.Name, pod.UID, pod.Spec.SchedulerName)
	}

	if _, err := GenerateApplicationIdFromPod(pod); err != nil {
		return err
	}

	return nil
}

// this function is called when a pod of a application gets updated,
// currently there is no operation needed for this call.
func (ctx *Context) updatePod(obj, newObj interface{}) {
	log.Logger.Debug("handling UpdatePod")
	old, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Logger.Error("failed to update pod", zap.Error(err))
		return
	}
	pod, err := utils.Convert2Pod(newObj)
	if err != nil {
		log.Logger.Error("failed to update pod", zap.Error(err))
		return
	}

	log.Logger.Debug("updatePod",
		zap.String("podName", old.Name),
		zap.String("oldState", string(old.Status.Phase)),
		zap.String("newState", string(pod.Status.Phase)))
}

func (ctx *Context) updatePodInCache(oldObj, newObj interface{}) {
	oldPod, err := utils.Convert2Pod(oldObj)
	if err != nil {
		log.Logger.Error("failed to update pod in cache", zap.Error(err))
		return
	}
	newPod, err := utils.Convert2Pod(newObj)
	if err != nil {
		log.Logger.Error("failed to update pod in cache", zap.Error(err))
		return
	}
	if err := ctx.schedulerCache.UpdatePod(oldPod, newPod); err != nil {
		log.Logger.Debug("failed to update pod in cache",
			zap.String("podName", oldPod.Name),
			zap.Error(err))
	}
}

// this function is called when a pod is deleted from api-server.
// when a pod is completed, the equivalent task's state will also be completed
// optionally, we run a completionHandler per workload, in order to determine
// if a application is completed along with this pod's completion
func (ctx *Context) deletePod(obj interface{}) {
	// when a pod is deleted, we need to check its role.
	// for spark, if driver pod is deleted, then we consider the app is completed
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var err error
		pod, err = utils.Convert2Pod(t.Obj)
		if err != nil {
			log.Logger.Error(err.Error())
			return
		}
	default:
		log.Logger.Error("cannot convert to pod")
		return
	}

	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	if application := ctx.getOrCreateApplication(pod); application != nil {
		log.Logger.Debug("release allocation")
		GetDispatcher().Dispatch(NewSimpleTaskEvent(
			application.GetApplicationId(), string(pod.UID), Complete))

		log.Logger.Info("delete pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.String("podUID", string(pod.UID)))
		// starts a completion handler to handle the completion of a app on demand
		application.StartCompletionHandler(ctx.kubeClient, pod)
	}

	log.Logger.Debug("remove pod from cache", zap.String("podName", pod.Name))
	if err := ctx.schedulerCache.RemovePod(pod); err != nil {
		log.Logger.Error("failed to remove pod from scheduler cache",
			zap.String("podName", pod.Name),
			zap.Error(err))
	}
}

// filter assigned pods
func (ctx *Context) filterAssignedPods(obj interface{}) bool {
	switch t := obj.(type) {
	case *v1.Pod:
		return utils.IsSchedulablePod(t) && utils.IsAssignedPod(t)
	case cache.DeletedFinalStateUnknown:
		if pod, ok := t.Obj.(*v1.Pod); ok {
			return utils.IsSchedulablePod(pod) && utils.IsAssignedPod(pod)
		}
		return false
	default:
		return false
	}
}

// filter pods by scheduler name and state
func (ctx *Context) filterPods(obj interface{}) bool {
	switch obj.(type) {
	case *v1.Pod:
		pod := obj.(*v1.Pod)
		return utils.IsSchedulablePod(pod)
	default:
		return false
	}
}

// filter configMap for the scheduler
func (ctx *Context) filterConfigMaps(obj interface{}) bool {
	switch obj.(type) {
	case *v1.ConfigMap:
		cm := obj.(*v1.ConfigMap)
		return cm.Name == common.DefaultConfigMapName
	default:
		return false
	}
}

// when detects the configMap for the scheduler is added, trigger hot-refresh
func (ctx *Context) addConfigMaps(obj interface{}) {
	log.Logger.Debug("configMap added")
	ctx.triggerReloadConfig()
}

// when detects the configMap for the scheduler is updated, trigger hot-refresh
func (ctx *Context) updateConfigMaps(obj, newObj interface{}) {
	log.Logger.Debug("trigger scheduler to reload configuration")
	// When update event is received, it is not guaranteed the data mounted to the pod
	// is also updated. This is because the actual update in pod's volume is ensured
	// by kubelet, kubelet is checking whether the mounted ConfigMap is fresh on every
	// periodic sync. As a result, the total delay from the moment when the ConfigMap
	// is updated to the moment when new keys are projected to the pod can be as long
	// as kubelet sync period + ttl of ConfigMaps cache in kubelet.
	// We trigger configuration reload, on YuniKorn core side, it keeps checking config
	// file state once this is called. And the acutal reload happens when it detects
	// actual changes on the content.
	ctx.triggerReloadConfig()
}

// when detects the configMap for the scheduler is deleted, no operation needed here
// we assume there will be a consequent add operation after delete, so we treat it like a update.
func (ctx *Context) deleteConfigMaps(obj interface{}) {
	log.Logger.Debug("configMap deleted")
}

func (ctx *Context) triggerReloadConfig() {
	log.Logger.Info("trigger scheduler configuration reloading")
	// TODO this should be moved to an admin API interface
	switch ctx.schedulerApi.(type) {
	case *rmproxy.RMProxy:
		proxy := ctx.schedulerApi.(*rmproxy.RMProxy)
		if err := proxy.ReloadConfiguration(ctx.conf.ClusterId); err != nil {
			log.Logger.Error("reload configuration failed", zap.Error(err))
		}
	default:
		return
	}
}

// evaluate given predicates based on current context
func (ctx *Context) IsPodFitNode(name string, node string) error {
	// simply skip if predicates are not enabled
	if !ctx.predictor.Enabled() {
		return nil
	}

	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	if pod, ok := ctx.schedulerCache.GetPod(name); ok {
		// if pod exists in cache, try to run predicates
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			meta := ctx.predictor.GetPredicateMeta(pod, ctx.schedulerCache.GetNodesInfoMap())
			return ctx.predictor.Predicates(pod, meta, targetNode)
		}
	}
	return fmt.Errorf("predicates were not running because pod or node was not found in cache")
}

// assume a pod will be running on a node, in scheduler, we maintain
// a cache where stores info for each node what pods are supposed to
// be running on it. And we keep this cache in-sync between core and the shim.
// this way, the core can make allocation decisions with consideration of
// other assumed pods before they are actually bound to the node (bound is slow).
func (ctx *Context) AssumePod(name string, node string) error {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	if pod, ok := ctx.schedulerCache.GetPod(name); ok {
		// when add assumed pod, we make a copy of the pod to avoid
		// modifying its original reference. otherwise, it may have
		// race when some other go-routines accessing it in parallel.
		assumedPod := pod.DeepCopy()
		// assign the node name for pod
		assumedPod.Spec.NodeName = node
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			return ctx.schedulerCache.AssumePod(assumedPod)
		}
	}
	return nil
}

func (ctx *Context) GetSchedulerConf() *conf.SchedulerConf {
	return ctx.conf
}

// if app already exists in the context, directly return the app from context
// if app doesn't exist in the context yet, create a new app instance and add to context
func (ctx *Context) getOrCreateApplication(pod *v1.Pod) *Application {
	appId, err := GenerateApplicationIdFromPod(pod)
	if err != nil {
		log.Logger.Error("unable to get application by given pod", zap.Error(err))
		return nil
	}

	if application, ok := ctx.applications[appId]; ok {
		return application
	} else {
		queueName := common.ApplicationDefaultQueue
		if an, ok := pod.Labels[common.LabelQueueName]; ok {
			queueName = an
		}
		newApp := NewApplication(appId, queueName, ctx.schedulerApi)
		ctx.applications[appId] = newApp
		return ctx.applications[appId]
	}
}

// for testing only
func (ctx *Context) AddApplication(app *Application) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.applications[app.GetApplicationId()] = app
}

func (ctx *Context) GetApplication(appId string) (*Application, error) {
	if app, ok := ctx.applications[appId]; ok {
		return app, nil
	}
	return nil, fmt.Errorf("application %s is not found in context", appId)
}

func (ctx *Context) GetTask(appId string, taskId string) (*Task, error) {
	if app, ok := ctx.applications[appId]; ok {
		if task, err := app.GetTask(taskId); err == nil {
			return task, nil
		}
	}
	return nil, fmt.Errorf("application %s is not found in context", appId)
}

func (ctx *Context) SelectApplications(filter func(app *Application) bool) []*Application {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()

	apps := make([]*Application, 0)
	for _, app := range ctx.applications {
		if filter != nil && !filter(app) {
			continue
		}
		apps = append(apps, app)
	}

	return apps
}

func (ctx *Context) Run(stopCh <-chan struct{}) {
	if !ctx.testMode {
		go ctx.nodeInformer.Informer().Run(stopCh)
		go ctx.podInformer.Informer().Run(stopCh)
		go ctx.pvInformer.Informer().Run(stopCh)
		go ctx.pvcInformer.Informer().Run(stopCh)
		go ctx.storageInformer.Informer().Run(stopCh)
		go ctx.configMapInformer.Informer().Run(stopCh)
	}
}
