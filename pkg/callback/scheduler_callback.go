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

	"go.uber.org/zap"

	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
)

// RM callback is called from the scheduler core, we need to ensure the response is handled
// asynchronously to avoid blocking the scheduler.
type AsyncRMCallback struct {
	context *cache.Context
}

func NewAsyncRMCallback(ctx *cache.Context) *AsyncRMCallback {
	return &AsyncRMCallback{context: ctx}
}

func (callback *AsyncRMCallback) RecvUpdateResponse(response *si.UpdateResponse) error {
	log.Logger.Info("callback received",
		zap.String("updateResponse", response.String()))

	// handle new accepted nodes
	for _, node := range response.AcceptedNodes {
		log.Logger.Info("callback: response to accepted node",
			zap.String("nodeId", node.NodeId))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeId: node.NodeId,
			Event:  events.NodeAccepted,
		})
	}

	for _, node := range response.RejectedNodes {
		log.Logger.Info("callback: response to rejected node",
			zap.String("nodeId", node.NodeId))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeId: node.NodeId,
			Event:  events.NodeRejected,
		})
	}

	// handle new accepted apps
	for _, app := range response.AcceptedApplications {
		// update context
		log.Logger.Info("callback: response to accepted application",
			zap.String("appId", app.ApplicationId))

		if app, err := callback.context.GetApplication(app.ApplicationId); err == nil {
			ev := cache.NewSimpleApplicationEvent(app.GetApplicationId(), events.AcceptApplication)
			dispatcher.Dispatch(ev)
		}
	}

	for _, app := range response.RejectedApplications {
		// update context
		log.Logger.Info("callback: response to rejected application",
			zap.String("appId", app.ApplicationId))

		if app, err := callback.context.GetApplication(app.ApplicationId); err == nil {
			ev := cache.NewSimpleApplicationEvent(app.GetApplicationId(), events.RejectApplication)
			dispatcher.Dispatch(ev)
		}
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		log.Logger.Info("callback: response to new allocation",
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("Uuid", alloc.Uuid),
			zap.String("applicationId", alloc.ApplicationId),
			zap.String("nodeId", alloc.NodeId))

		if app, err := callback.context.GetApplication(alloc.ApplicationId); err == nil {
			ev := cache.NewAllocateTaskEvent(app.GetApplicationId(), alloc.AllocationKey, alloc.Uuid, alloc.NodeId)
			dispatcher.Dispatch(ev)
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		log.Logger.Info("callback: response to rejected allocation",
			zap.String("allocationKey", reject.AllocationKey))

		if app, err := callback.context.GetApplication(reject.ApplicationId); err == nil {
			dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationId(), reject.AllocationKey,
				fmt.Sprintf("task %s from application %s is rejected by scheduler",
					reject.AllocationKey, reject.ApplicationId)))
		}
	}

	for _, release := range response.ReleasedAllocations {
		log.Logger.Info("callback: response to released allocations",
			zap.String("Uuid", release.Uuid))
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

	for _, forgetAlloc := range args.ForgetAllocations {
		if err := callback.context.ForgetPod(forgetAlloc.AllocationKey); err != nil {
			return err
		}
	}
	return nil
}
