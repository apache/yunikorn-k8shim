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
	"github.com/cloudera/k8s-shim/pkg/log"
	"github.com/cloudera/k8s-shim/pkg/state"
	"github.com/cloudera/scheduler-interface/lib/go/si"
	"go.uber.org/zap"
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
	log.Logger.Info("callback received",
		zap.String("updateResponse", response.String()))

	// handle new accepted apps
	for _, app := range response.AcceptedApplications {
		// update context
		log.Logger.Info("callback: response to accepted application",
			zap.String("appId", app.ApplicationId))

		if app, err := callback.context.GetApplication(app.ApplicationId); err == nil {
			ev := state.NewSimpleApplicationEvent(app.GetApplicationId(), state.AcceptApplication)
			state.GetDispatcher().Dispatch(ev)
		}
	}

	for _, app := range response.RejectedApplications {
		// update context
		log.Logger.Info("callback: response to rejected application",
			zap.String("appId", app.ApplicationId))

		if app, err := callback.context.GetApplication(app.ApplicationId); err == nil {
			ev := state.NewSimpleApplicationEvent(app.GetApplicationId(), state.RejectApplication)
			state.GetDispatcher().Dispatch(ev)
		}
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		log.Logger.Info("callback: response to new allocation",
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("allocationUuid", alloc.Uuid),
			zap.String("appId", alloc.ApplicationId),
			zap.String("nodeId", alloc.NodeId))

		if app, err := callback.context.GetApplication(alloc.ApplicationId); err == nil {
			ev := state.NewAllocateTaskEvent(app.GetApplicationId(), alloc.AllocationKey, alloc.Uuid, alloc.NodeId)
			state.GetDispatcher().Dispatch(ev)
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		log.Logger.Info("callback: response to rejected allocation",
			zap.String("allocationKey", reject.AllocationKey))

		if app, err := callback.context.GetApplication(reject.ApplicationId); err == nil {
			state.GetDispatcher().Dispatch(state.NewRejectTaskEvent(app.GetApplicationId(), reject.AllocationKey,
				fmt.Sprintf("task %s from application %s is rejected by scheduler",
					reject.AllocationKey, reject.ApplicationId)))
		}
	}

	for _, release := range response.ReleasedAllocations {
		log.Logger.Info("callback: response to released allocations",
			zap.String("allocationUuid", release.AllocationUUID))
	}

	return nil
}

// this callback implements scheduler plugin interface PredicatesPlugin/
func (callback *AsyncRMCallback) Predicates(args *si.PredicatesArgs) error {
	return callback.context.IsPodFitNode(args.AllocationKey, args.NodeId)
}

// this callback implements scheduler plugin interface ReconcilePlugin.
func (callback *AsyncRMCallback) ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error {
	for _, assumedAlloc := range args.AssumedAllocations {
		if err := callback.context.AssumePod(assumedAlloc.AllocationKey, assumedAlloc.NodeId); err != nil {
			return err
		}
	}
	return nil
}


