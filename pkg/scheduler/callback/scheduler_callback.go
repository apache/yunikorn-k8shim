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

package callback

import (
	"fmt"
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
)

// RM callback is called from the scheduler core, we need to ensure the response is handled
// asynchronously to avoid blocking the scheduler.
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
		if app, err := callback.context.GetApplication(app.ApplicationId); err == nil {
			ev := state.NewSimpleApplicationEvent(app.GetApplicationId(), state.AcceptApplication)
			state.GetDispatcher().Dispatch(ev)
		}
	}

	for _, app := range response.RejectedApplications {
		// update context
		glog.V(4).Infof("callback: response to rejected application: %s", app.ApplicationId)
		if app, err := callback.context.GetApplication(app.ApplicationId); err == nil {
			ev := state.NewSimpleApplicationEvent(app.GetApplicationId(), state.RejectApplication)
			state.GetDispatcher().Dispatch(ev)
		}
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		glog.V(4).Infof("callback: response to new allocation, allocationKey: %s," +
			" allocation UUID: %s, applicationId: %s, nodeId: %s",
			alloc.AllocationKey, alloc.Uuid, alloc.ApplicationId, alloc.NodeId)

		if app, err := callback.context.GetApplication(alloc.ApplicationId); err == nil {
			ev := state.NewAllocateTaskEvent(app.GetApplicationId(), alloc.AllocationKey, alloc.Uuid, alloc.NodeId)
			state.GetDispatcher().Dispatch(ev)
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		glog.V(4).Infof("callback: response to rejected allocation, allocationKey: %s",
			reject.AllocationKey)
		if app, err := callback.context.GetApplication(reject.ApplicationId); err == nil {
			state.GetDispatcher().Dispatch(state.NewRejectTaskEvent(app.GetApplicationId(), reject.AllocationKey,
				fmt.Sprintf("task %s from application %s is rejected by scheduler",
					reject.AllocationKey, reject.ApplicationId)))
		}
	}

	for _, release := range response.ReleasedAllocations {
		glog.V(4).Infof("callback: response to released allocations, allocationKey: %s",
			release.AllocationUUID)
	}

	return nil
}

// this callback implements scheduler plugin interface PredicatesPlugin/
func (callback *AsyncRMCallback) Predicates(allocationId string, node string) error {
	return callback.context.IsPodFitNode(allocationId, node)
}

// this callback implements scheduler plugin interface ReconcilePlugin.
func (callback *AsyncRMCallback) ReSyncSchedulerCache(allocationId string, node string) error {
	return callback.context.AssumePod(allocationId, node)
}


