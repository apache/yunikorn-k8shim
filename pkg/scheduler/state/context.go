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
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	infomerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"strings"
	"sync"
)

// context maintains scheduling state, like jobs and jobs' pods.
type Context struct {
	jobMap map[string]*common.Job
	jobController *controller.JobController
	nodeController *controller.NodeController
	taskController *controller.TaskController
	conf *conf.SchedulerConf
	kubeClient client.KubeClient

	// informers
	podInformer infomerv1.PodInformer
	nodeInformer infomerv1.NodeInformer
	// nsInformer       infomerv1.NamespaceInformer
	// pvInformer       infomerv1.PersistentVolumeInformer
	// pvcInformer      infomerv1.PersistentVolumeClaimInformer

	lock *sync.RWMutex
}

func NewContext(scheduler api.SchedulerApi, configs *conf.SchedulerConf) *Context {
	ctx := &Context{
		jobMap: make(map[string]*common.Job),
		conf: configs,
		kubeClient: client.NewKubeClient(configs.KubeConfig),
		lock: &sync.RWMutex{},
	}

	// init controllers
    ctx.jobController = controller.NewJobController(scheduler, ctx.kubeClient)
	ctx.nodeController = controller.NewNodeController(scheduler)
	ctx.taskController = controller.NewTaskController(ctx.kubeClient)

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
				AddFunc:    ctx.AddPod,
				UpdateFunc: ctx.UpdatePod,
				DeleteFunc: ctx.DeletePod,
			},
		})

	return ctx
}

func (ctx *Context) AddPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}

	glog.V(4).Infof("context: handling AddPod podName=%s/%s, podUid=%s", pod.Namespace, pod.Name, pod.UID)
	if err := ctx.ValidatePod(pod); err != nil {
		glog.V(1).Infof("job is invalid, error: %s", err.Error())
		return
	}

	job := ctx.getOrCreateJob(pod)
	task := ctx.getOrAddTask(job, pod)
	job.AddTask(task)
}

func (ctx *Context) UpdatePod(obj, newObj interface{}) {
	glog.V(4).Infof("context: handling UpdatePod")
	pod, ok := newObj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}
	glog.V(4).Infof("pod %s status %s", pod.Name, pod.Status.Phase)
}

func (ctx *Context) DeletePod(obj interface{}) {
	// when a pod is deleted, we need to check its role.
	// for spark, if driver pod is deleted, then we consider the job is completed
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
	if job := ctx.getOrCreateJob(pod); job != nil {
		// starts a completion handler to handle the completion of a job on demand
		job.StartCompletionHandler(ctx.kubeClient, pod)
	}
}

func (ctx *Context) GetJobController() *controller.JobController {
	return ctx.jobController
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

func (ctx *Context) GetSchedulerConf() *conf.SchedulerConf {
	return ctx.conf
}

func (ctx *Context) PrintJobMap() {
	glog.V(4).Infof("Job Map:")
	for k, v := range ctx.jobMap {
		glog.V(4).Infof("    %s -> %s", k, v.String())
	}
}

func (context *Context) HasPendingJobs() bool {
	return len(context.jobMap) > 0
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

	jobId := common.GetJobID(pod)
	if jobId == "" {
		return errors.Errorf("invalid pod, cannot retrieve jobId from pod's spec")
	}

	return nil
}

// if job already exists in the context, directly return the job from context
// if job doesn't exist in the context yet, create a new job instance and add to context
func (ctx *Context) getOrCreateJob(pod *v1.Pod) *common.Job {
	jobId := common.GetJobID(pod)
	if job, ok := ctx.jobMap[jobId]; ok {
		return job
	} else {
		newJob := common.NewJob(jobId)
		if queueName, ok := pod.Labels[common.LabelQueueName]; ok {
			newJob.Queue = queueName
		}
		ctx.jobMap[jobId] = newJob
		return ctx.jobMap[jobId]
	}
}

func (ctx *Context) getOrAddTask(job *common.Job, pod *v1.Pod) *common.Task {
	// using pod UID as taskId
	if task := job.GetTask(string(pod.UID)); task != nil {
		return task
	}
	newTask := common.CreateTaskFromPod(job.JobId, ctx.kubeClient, pod)
	job.AddTask(newTask)
	return newTask
}


func (ctx *Context) JobAccepted(jobId string) {
	if job, ok := ctx.jobMap[jobId]; ok {
		ctx.jobController.Accept(job)
		return
	}
	glog.V(2).Infof("job %s is not found ", jobId)
}

func (ctx *Context) JobRejected(jobId string) {
	if job, ok := ctx.jobMap[jobId]; ok {
		ctx.jobController.Reject(job)
		return
	}
	glog.V(2).Infof("job %s is rejected by the scheduler, but it is not found in the context", jobId)
}


func (ctx *Context) AllocateTask(jobId string, taskId string, nodeId string) error {
	if job := ctx.jobMap[jobId]; job != nil {
		if task := job.GetTask(taskId); task != nil {
			return task.Handle(common.NewAllocateTaskEvent(nodeId))
		}
	}
	return nil
}

func (ctx *Context) OnPodRejected(jobId string, podUid string) error {
	if job, ok := ctx.jobMap[jobId]; ok {
		if task := job.GetTask(podUid); task != nil {
			task.Handle(common.NewFailTaskEvent(
				fmt.Sprintf("task %s from job %s is rejected by scheduler", podUid, jobId)))
		}
	}
	return errors.New("pod gets rejected, but job info is not found in context," +
		" something is wrong")
}

func (ctx *Context) SelectJobs(filter func(job *common.Job) bool) []*common.Job {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()

	jobs := make([]*common.Job, 0)
	for _, job := range ctx.jobMap {
		if filter != nil && !filter(job) {
			continue
		}
		// glog.V(4).Infof("pending job %s to job select result", jobId)
		jobs = append(jobs, job)
	}

	return jobs
}

func (ctx *Context) Run(stopCh <-chan struct{}) {
	go ctx.nodeInformer.Informer().Run(stopCh)
	go ctx.podInformer.Informer().Run(stopCh)
}
