package state

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/conf"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/controller"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	infomerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"strings"
	"sync"
)

// context maintains scheduling state, like jobs and jobs' pods.
type Context struct {
	jobMap map[string]*common.Job
	jobController *controller.JobController
	nodeController *controller.NodeController
	podController *controller.PodController
	conf *conf.SchedulerConf
	clientSet *kubernetes.Clientset

	// informers
	podInformer      infomerv1.PodInformer
	nodeInformer     infomerv1.NodeInformer
	// nsInformer       infomerv1.NamespaceInformer
	// pvInformer       infomerv1.PersistentVolumeInformer
	// pvcInformer      infomerv1.PersistentVolumeClaimInformer

	lock *sync.RWMutex
}

func getKubeConfig(master string, kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" && master == "" {
		glog.V(1).Infof("master/kubeconfig not found." +
			" using in-cluster config, this may not work")
		kubeconfig, err := rest.InClusterConfig()
		if err == nil {
			return kubeconfig, nil
		}
	}
	return clientcmd.BuildConfigFromFlags(master, kubeconfig)
}

func NewContext(scheduler api.SchedulerApi, configs *conf.SchedulerConf) *Context {
	kubeconf, err := getKubeConfig("", configs.KubeConfig)
	if err != nil {
		panic("failed to init... FIXME")
	}

	ctx := &Context{
		jobMap: make(map[string]*common.Job),
		conf: configs,
		clientSet: kubernetes.NewForConfigOrDie(kubeconf),
		lock: &sync.RWMutex{},
	}

	// init controllers
    ctx.jobController = controller.NewJobController(scheduler)
	ctx.podController = controller.NewPodController(ctx.clientSet)
	ctx.nodeController = controller.NewNodeController(scheduler)

	// we have disabled re-sync to keep ourselves up-to-date
	informerFactory := informers.NewSharedInformerFactory(ctx.clientSet, 0)

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
	glog.V(4).Infof("context: handling AddPod")
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Pod: %v", obj)
		return
	}

	if err := ctx.ValidatePod(pod); err != nil {
		glog.V(1).Infof("job is invalid, error: %s", err.Error())
		return
	}
	job := ctx.GetOrCreateJob(pod)
	job.PrintJobState()

	glog.V(3).Infof("pending pod found from api-server, job: %s, pod: %s",
		job.JobId, pod.Name)
	ctx.PrintJobMap()
}

func (ctx *Context) UpdatePod(oldObj, newObj interface{}) {
	glog.V(4).Infof("context: handling UpdatePod")
}

func (ctx *Context) DeletePod(obj interface{}) {
	glog.V(4).Infof("context: handling DeletePod")
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

func (context *Context) GetSchedulerConf() *conf.SchedulerConf {
	return context.conf
}

func (context *Context) PrintJobMap() {
	glog.V(4).Infof("Job Map:")
	for k, v := range context.jobMap {
		glog.V(4).Infof("    %s -> %s", k, v.String())
	}
}

func (context *Context) HasPendingJobs() bool {
	return len(context.jobMap) > 0
}

// validate pod see if it is applicable for the scheduler
//
func (context *Context) ValidatePod(pod *v1.Pod) error {
	if pod.Spec.SchedulerName == "" || pod.Spec.SchedulerName != context.conf.SchedulerName {
		// only pod with specific scheduler name is valid to us
		return errors.New(fmt.Sprintf("only pod whose spec has explicitly " +
			"specified schedulerName=%s is a valid scheduling-target, but schedulerName for pod %s(%s) is %s",
			pod.Spec.SchedulerName, pod.Name, pod.UID, context.conf.SchedulerName))
	}

	jobId := common.GetJobID(pod)
	if jobId == "" {
		return errors.Errorf("invalid pod, no label %s is found in metadata", common.LabelJobId)
	}

	return nil
}

func (context *Context) GetOrCreateJob(pod *v1.Pod) *common.Job {
	jobId := common.GetJobID(pod)
	if job, ok := context.jobMap[jobId]; ok {
		if job.AddPod(pod) {
			job.AddPendingPod(pod)
		}
	} else {
		newJob := common.NewJob(jobId)
		if queueName, ok := pod.Labels[common.LabelQueueName]; ok {
			newJob.Queue = queueName
		}
		newJob.AddPod(pod)
		newJob.AddPendingPod(pod)
		context.jobMap[jobId] = newJob
	}

	return context.jobMap[jobId]
}

func (context *Context) JobAccepted(jobId string) {
	if job, ok := context.jobMap[jobId]; ok {
		context.jobController.Accept(job)
		return
	}
	glog.V(2).Infof("job %s is not found ", jobId)
}

func (context *Context) JobRejected(jobId string) {
	if job, ok := context.jobMap[jobId]; ok {
		context.jobController.Reject(job)
		return
	}
	glog.V(2).Infof("job %s is rejected by the scheduler, but it is not found in the context", jobId)
}

func (context *Context) getPod(jobId string, podId string) (pod *v1.Pod, err error) {
	if job, ok := context.jobMap[jobId]; ok {
		for _, elem := range job.PodList {
			if string(elem.UID) == podId {
				return elem, nil
			}
		}
	}
	return nil, errors.New(fmt.Sprintf("pod %s is not found in job %s",podId, jobId))
}

func (context *Context) AllocatePod(jobId string, podId string, nodeId string) error {
	pendingPod, err := context.getPod(jobId, podId)
	if err != nil {
		return err
	}

	if pendingPod == nil {
		glog.V(2).Info("skipping binding job %s pod UID=%s to node %s," +
			" pod is not found in the context", jobId, podId, nodeId)
		return errors.New(fmt.Sprintf("binding pod %s skipped because pod doesn't exist in context", podId))
	}
	glog.V(4).Infof("bind pod target: name: %s, uid: %s", pendingPod.Name, pendingPod.UID)
	err = context.podController.Bind(pendingPod, nodeId)
	if err != nil {
		glog.V(1).Infof("bind pod failed, name: %s, uid: %s, %#v",
			pendingPod.Name, pendingPod.UID, err)
		return err
	}
	return nil
}

func (context *Context) OnPodRejected(jobId string, podUid string) error {
	glog.V(3).Infof("pod request %s from job %s is rejected by the scheduler," +
		" adding back to the pending list", podUid, jobId)
	if job, ok := context.jobMap[jobId]; ok {
		for _, pod := range job.PodList {
			if string(pod.UID) == podUid {
				job.AddPendingPod(pod)
				job.PrintJobState()
				return nil
			}
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
