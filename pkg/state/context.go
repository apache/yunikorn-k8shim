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

package state

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/conf"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/controller"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	infomerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"strings"
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
	podInformer       infomerv1.PodInformer
	nodeInformer      infomerv1.NodeInformer
	configMapInformer infomerv1.ConfigMapInformer
	// nsInformer       infomerv1.NamespaceInformer
	// pvInformer       infomerv1.PersistentVolumeInformer
	// pvcInformer      infomerv1.PersistentVolumeClaimInformer

	testMode bool
	lock     *sync.RWMutex
}

func NewContext(scheduler api.SchedulerApi, configs *conf.SchedulerConf) *Context {
	kc := client.NewKubeClient(configs.KubeConfig)
	return NewContextInternal(scheduler, configs, kc, false)
}

// only for testing
func NewContextInternal(scheduler api.SchedulerApi, configs *conf.SchedulerConf, client client.KubeClient, testMode bool) *Context {
	ctx := &Context{
		applications: make(map[string]*Application),
		conf:         configs,
		kubeClient:   client,
		schedulerApi: scheduler,
		testMode:     testMode,
		lock:         &sync.RWMutex{},
	}

	// init controllers
	ctx.nodeController = controller.NewNodeController(scheduler)

	// we have disabled re-sync to keep ourselves up-to-date
	informerFactory := informers.NewSharedInformerFactory(ctx.kubeClient.GetClientSet(), 0)

	// init informers
	ctx.nodeInformer = informerFactory.Core().V1().Nodes()
	ctx.nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctx.nodeController.AddNode,
			UpdateFunc: ctx.nodeController.UpdateNode,
			DeleteFunc: ctx.nodeController.DeleteNode,
		},
		0,
	)

	ctx.podInformer = informerFactory.Core().V1().Pods()
	ctx.podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: ctx.filterPods,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    ctx.addPod,
				UpdateFunc: ctx.updatePod,
				DeleteFunc: ctx.deletePod,
			},
		})

	ctx.configMapInformer = informerFactory.Core().V1().ConfigMaps()
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

// add a pod to the context
// if pod is valid, we convent the pod to a equivalent task,
// if task belongs to a application, we add this task to the application,
// if task is new, we create a application and add this task to that.
func (ctx *Context) addPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}

	glog.V(4).Infof("context: handling AddPod podName=%s/%s, podUid=%s", pod.Namespace, pod.Name, pod.UID)
	if err := ctx.validatePod(pod); err != nil {
		glog.V(1).Infof("application is invalid, error: %s", err.Error())
		return
	}

	if app := ctx.getOrCreateApplication(pod); app != nil {
		task := ctx.getOrAddTask(app, pod)
		app.AddTask(task)
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
		return errors.New(fmt.Sprintf("only pod whose spec has explicitly " +
			"specified schedulerName=%s is a valid scheduling-target, but schedulerName for pod %s(%s) is %s",
			pod.Spec.SchedulerName, pod.Name, pod.UID, ctx.conf.SchedulerName))
	}

	if _, err := GenerateApplicationIdFromPod(pod); err != nil {
		return err
	}

	return nil
}

// this function is called when a pod of a application gets updated,
// currently there is no operation needed for this call.
func (ctx *Context) updatePod(obj, newObj interface{}) {
	glog.V(4).Infof("context: handling UpdatePod")
	pod, ok := newObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}
	glog.V(4).Infof("pod %s status %s", pod.Name, pod.Status.Phase)
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
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			glog.V(1).Info("Cannot convert to *v1.Pod: %v", t.Obj)
			return
		}
	default:
		glog.V(1).Info("Cannot convert to *v1.Pod: %v", t)
		return
	}

	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	if application := ctx.getOrCreateApplication(pod); application != nil {
		glog.V(4).Infof("Release allocation")
		GetDispatcher().Dispatch(NewSimpleTaskEvent(
			application.GetApplicationId(), string(pod.UID), Complete))

		glog.V(4).Infof("context: handling DeletePod podName=%s/%s, podUid=%s", pod.Namespace, pod.Name, pod.UID)
		// starts a completion handler to handle the completion of a app on demand
		application.StartCompletionHandler(ctx.kubeClient, pod)
	}
}

// filter pods by scheduler name and state
func (ctx *Context) filterPods(obj interface{}) bool {
	switch obj.(type) {
	case *v1.Pod:
		pod := obj.(*v1.Pod)
		if strings.Compare(pod.Spec.SchedulerName,
			ctx.conf.SchedulerName) == 0 && pod.Status.Phase == v1.PodPending {
			return true
		}
		return false
	default:
		return false
	}
}

// filter configMap for the scheduler
func (ctx *Context) filterConfigMaps(obj interface{}) bool {
	switch obj.(type) {
	case *v1.ConfigMap:
		cm := obj.(*v1.ConfigMap)
		return cm.Name == conf.DefaultConfigMapName
	default:
		return false
	}
}

// when detects the configMap for the scheduler is added, trigger hot-refresh
func (ctx *Context) addConfigMaps(obj interface{}) {
	glog.V(4).Infof("configMap added")
	ctx.triggerReloadConfig()
}

// when detects the configMap for the scheduler is updated, trigger hot-refresh
func (ctx *Context) updateConfigMaps(obj, newObj interface{}) {
	glog.V(4).Infof("configMap updated")
	ctx.triggerReloadConfig()
}

// when detects the configMap for the scheduler is deleted, no operation needed here
// we assume there will be a consequent add operation after delete, so we treat it like a update.
func (ctx *Context) deleteConfigMaps(obj interface{}) {
	glog.V(3).Infof("configMap deleted")
}

func (ctx *Context) triggerReloadConfig() {
	glog.V(3).Infof("trigger scheduler configuration reloading")
	// TODO this should be moved to an admin API interface
	switch ctx.schedulerApi.(type) {
	case *rmproxy.RMProxy:
		proxy := ctx.schedulerApi.(*rmproxy.RMProxy)
		if err := proxy.ReloadConfiguration(); err != nil {
			glog.V(1).Infof("Reload configuration failed with error %s", err.Error())
		}
	default:
		return
	}
}

func (ctx *Context) GetSchedulerConf() *conf.SchedulerConf {
	return ctx.conf
}

// if app already exists in the context, directly return the app from context
// if app doesn't exist in the context yet, create a new app instance and add to context
func (ctx *Context) getOrCreateApplication(pod *v1.Pod) *Application {
	appId, err := GenerateApplicationIdFromPod(pod)
	if err != nil {
		glog.V(1).Infof("unable to get application by given pod, error message: %s", err.Error())
		return nil
	}

	if application, ok := ctx.applications[appId]; ok {
		return application
	} else {
		queueName := conf.ApplicationDefaultQueue
		if an, ok := pod.Labels[conf.LabelQueueName]; ok {
			queueName = an
		}
		newApp := NewApplication(appId, queueName, ctx.schedulerApi)
		ctx.applications[appId] = newApp
		return ctx.applications[appId]
	}
}

// for testing only
func (ctx *Context) AddApplication(app *Application) {
	ctx.applications[app.GetApplicationId()] = app
}

func (ctx *Context) GetApplication(appId string) (*Application, error) {
	if app, ok := ctx.applications[appId]; ok {
		return app, nil
	}
	return nil, errors.New(fmt.Sprintf("application %s is not found in context", appId))
}

func (ctx *Context) GetTask(appId string, taskId string) (*Task, error) {
	if app, ok := ctx.applications[appId]; ok {
		if task, err := app.GetTask(taskId); err == nil {
			return task, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("application %s is not found in context", appId))
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
		go ctx.configMapInformer.Informer().Run(stopCh)
	}
}
