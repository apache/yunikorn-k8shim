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

package callback

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
)

type AsyncRMCallback struct {
	context *state.Context
}

func NewAsyncRMCallback(ctx *state.Context) *AsyncRMCallback {
	return &AsyncRMCallback{context: ctx}
}

func (callback *AsyncRMCallback) RecvUpdateResponse(response *si.UpdateResponse) error {
	glog.V(4).Infof("callback received response: %s", response.String())

	// handle new accepted apps
	for _, app := range response.AcceptedApplications {
		// update context
		glog.V(4).Infof("callback: response to accepted application: %s", app.ApplicationId)
		callback.context.ApplicationAccepted(app.ApplicationId)
	}

	for _, app := range response.RejectedApplications {
		// update context
		glog.V(4).Infof("callback: response to rejected application: %s", app.ApplicationId)
		callback.context.ApplicationRejected(app.ApplicationId)
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		glog.V(4).Infof("callback: response to new allocation, allocationKey: %s, applicationId: %s, nodeId: %s",
			alloc.AllocationKey, alloc.ApplicationId, alloc.NodeId)
		if err := callback.context.AllocateTask(alloc.ApplicationId, alloc.AllocationKey, alloc.NodeId); err != nil {
			glog.V(1).Infof("failed to allocate task, error %v", err)
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		glog.V(4).Infof("callback: response to rejected allocation, allocationKey: %s",
			reject.AllocationKey)
		callback.context.OnTaskRejected(reject.ApplicationId, reject.AllocationKey)
	}

	return nil
}



