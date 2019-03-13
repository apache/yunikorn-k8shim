package callback

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
)

type SimpleRMCallback struct {
	context *state.Context
}

func NewSimpleRMCallback(ctx *state.Context) *SimpleRMCallback {
	return &SimpleRMCallback{context: ctx}
}

func (callback *SimpleRMCallback) RecvUpdateResponse(response *si.UpdateResponse) error {
	glog.V(4).Infof("callback received response: %s", response.String())

	// handle new accepted jobs
	for _, job := range response.AcceptedJobs {
		// update context
		glog.V(4).Infof("callback: response to accepted job: %s", job.JobId)
		callback.context.JobAccepted(job.JobId)
	}

	for _, job := range response.RejectedJobs {
		// update context
		glog.V(4).Infof("callback: response to rejected job: %s", job.JobId)
		callback.context.JobRejected(job.JobId)
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		// remove pod from pending list
		glog.V(4).Infof("callback: response to new allocation, allocationKey: %s, jobId: %s, nodeId: %s",
			alloc.AllocationKey, alloc.JobId, alloc.NodeId)
		callback.context.AllocatePod(alloc.JobId, alloc.AllocationKey, alloc.NodeId)
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		glog.V(4).Infof("callback: response to rejected allocation, allocationKey: %s",
			reject.AllocationKey)
		// TODO reject response should include jobId
		callback.context.OnPodRejected("jobId", reject.AllocationKey)
	}

	return nil
}



