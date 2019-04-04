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
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
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
	applications   map[string]*common.Application
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
		applications: make(map[string]*common.Application),
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

func (ctx *Context) addPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}

	glog.V(4).Infof("context: handling AddPod podName=%s/%s, podUid=%s", pod.Namespace, pod.Name, pod.UID)
	if err := ctx.ValidatePod(pod); err != nil {
		glog.V(1).Infof("application is invalid, error: %s", err.Error())
		return
	}

	if app := ctx.getOrCreateApplication(pod); app != nil {
		task := ctx.getOrAddTask(app, pod)
		app.AddTask(task)
	}
}

func (ctx *Context) updatePod(obj, newObj interface{}) {
	glog.V(4).Infof("context: handling UpdatePod")
	pod, ok := newObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}
	glog.V(4).Infof("pod %s status %s", pod.Name, pod.Status.Phase)
}

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

	glog.V(4).Infof("context: handling DeletePod podName=%s/%s, podUid=%s", pod.Namespace, pod.Name, pod.UID)
	if app := ctx.getOrCreateApplication(pod); app != nil {
		// starts a completion handler to handle the completion of a app on demand
		app.StartCompletionHandler(ctx.kubeClient, pod)
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
		return cm.Name == common.DefaultConfigMapName
	default:
		return false
	}
}

func (ctx *Context) addConfigMaps(obj interface{}) {
	glog.V(4).Infof("configMap added")
	ctx.triggerReloadConfig()
}

func (ctx *Context) updateConfigMaps(obj, newObj interface{}) {
	glog.V(4).Infof("configMap updated")
	ctx.triggerReloadConfig()
}

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

// validate pod see if it is applicable for the scheduler
//
func (ctx *Context) ValidatePod(pod *v1.Pod) error {
	if pod.Spec.SchedulerName == "" || pod.Spec.SchedulerName != ctx.conf.SchedulerName {
		// only pod with specific scheduler name is valid to us
		return errors.New(fmt.Sprintf("only pod whose spec has explicitly " +
			"specified schedulerName=%s is a valid scheduling-target, but schedulerName for pod %s(%s) is %s",
			pod.Spec.SchedulerName, pod.Name, pod.UID, ctx.conf.SchedulerName))
	}

	if _, err := common.GetApplicationId(pod); err != nil {
		return err
	}

	return nil
}

// if app already exists in the context, directly return the app from context
// if app doesn't exist in the context yet, create a new app instance and add to context
func (ctx *Context) getOrCreateApplication(pod *v1.Pod) *common.Application {
	appId, err := common.GetApplicationId(pod)
	if err != nil {
		glog.V(1).Infof("unable to get application by given pod, error message: %s", err.Error())
		return nil
	}

	if app, ok := ctx.applications[appId]; ok {
		return app
	} else {
		queueName := common.ApplicationDefaultQueue
		if an, ok := pod.Labels[common.LabelQueueName]; ok {
			queueName = an
		}
		newApp := common.NewApplication(appId, queueName, ctx.schedulerApi)
		ctx.applications[appId] = newApp
		return ctx.applications[appId]
	}
}

// for testing only
func (ctx *Context) AddApplication(app *common.Application) {
	ctx.applications[app.GetApplicationId()] = app
}

func (ctx *Context) getOrAddTask(app *common.Application, pod *v1.Pod) *common.Task {
	// using pod UID as taskId
	if task := app.GetTask(string(pod.UID)); task != nil {
		return task
	}
	newTask := common.CreateTaskFromPod(app, ctx.kubeClient, ctx.schedulerApi, pod)
	app.AddTask(newTask)
	return newTask
}


func (ctx *Context) ApplicationAccepted(appId string) {
	if app, ok := ctx.applications[appId]; ok {
		app.Handle(common.NewSimpleApplicationEvent(common.AcceptApplication))
		return
	}
	glog.V(2).Infof("app %s is not found ", appId)
}

func (ctx *Context) ApplicationRejected(appId string) {
	if app, ok := ctx.applications[appId]; ok {
		app.Handle(common.NewSimpleApplicationEvent(common.RejectApplication))
		return
	}
	glog.V(2).Infof("app %s is rejected by the scheduler, but it is not found in the context", appId)
}


func (ctx *Context) AllocateTask(appId string, taskId string, nodeId string) error {
	if app := ctx.applications[appId]; app != nil {
		if task := app.GetTask(taskId); task != nil {
			return task.Handle(common.NewAllocateTaskEvent(nodeId))
		}
	}
	return nil
}

func (ctx *Context) OnTaskRejected(appId string, podUid string) error {
	if app, ok := ctx.applications[appId]; ok {
		if task := app.GetTask(podUid); task != nil {
			task.Handle(common.NewRejectTaskEvent(
				fmt.Sprintf("task %s from application %s is rejected by scheduler", podUid, appId)))
			return nil
		}
	}
	return errors.New("pod gets rejected, but application info is not found in context," +
		" something is wrong")
}

func (ctx *Context) SelectApplications(filter func(app *common.Application) bool) []*common.Application {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()

	apps := make([]*common.Application, 0)
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
