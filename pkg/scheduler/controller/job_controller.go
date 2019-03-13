package controller

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/api"
	"k8s.io/api/core/v1"
)

// job controller manages a job's lifecycle
type JobController struct {
	proxy api.SchedulerApi
}

func NewJobController(schedulerApi api.SchedulerApi) *JobController {
	return &JobController{
		proxy: schedulerApi,
	}
}

func (jc *JobController) Submit(job *common.Job) {
	//go func() {
		glog.V(3).Infof("submit new job %s to cluster %s", job.String(), common.ClusterId)
		if err := jc.proxy.Update(&si.UpdateRequest{
			NewJobs: []*si.AddJobRequest{
				{
					JobId:         job.JobId,
					QueueName:     job.Queue,
					PartitionName: job.Partition,
				},
			},
			RmId: common.ClusterId,
		}); err == nil {
			job.Handle(job.Events.SUBMIT)
		}
	//}()
}

func (jc *JobController) Accept(job *common.Job) {
	job.Handle(job.Events.ACCEPT)
}

func (jc *JobController) Run(job *common.Job) {
	job.Handle(job.Events.RUN)
}

func (jc *JobController) Reject(job *common.Job) {
	job.Handle(job.Events.REJECT)
}

func (jc *JobController) Schedule(job *common.Job, pod *v1.Pod) {
	// schedule pod
	// job is accepted, now schedule the pod
	if job.IsPendingPod(pod) {
		glog.V(3).Infof("pod %s is pending, send request to the scheduler", pod.UID)
		if err := jc.sendSchedulingRequest(job.JobId, pod); err != nil {
			glog.V(2).Infof("failed to send scheduling request to scheduler, error: %v", err)
			return
		}
		// once pod is scheduled, remove it from the pod list
		// this is to avoid sending duplicate update requests to scheduler
		// don't worry, we will add it back if the pod is rejected by the scheduler
		job.RemovePendingPod(pod)
		job.PrintJobState()
	}
}

func (jc *JobController) sendSchedulingRequest(jobId string, pod *v1.Pod) error {
	glog.V(4).Infof("Trying to schedule pod: %s", pod.Name)
	// convert the request
	rr := common.ConvertRequest(jobId, pod)
	glog.V(4).Infof("send update request %s", rr.String())
	return jc.proxy.Update(&rr)
}