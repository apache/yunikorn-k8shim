/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

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

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// RM callback is called from the scheduler core, we need to ensure the response is handled
// asynchronously to avoid blocking the scheduler.
type AsyncRMCallback struct {
	context *cache.Context
}

func NewAsyncRMCallback(ctx *cache.Context) *AsyncRMCallback {
	return &AsyncRMCallback{context: ctx}
}

func (callback *AsyncRMCallback) UpdateAllocation(response *si.AllocationResponse) error {
	log.Logger().Debug("UpdateAllocation callback received",
		zap.String("UpdateAllocationResponse", response.String()))
	// handle new allocations
	for _, alloc := range response.New {
		// got allocation for pod, bind pod to the scheduled node
		log.Logger().Debug("callback: response to new allocation",
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("UUID", alloc.UUID),
			zap.String("applicationID", alloc.ApplicationID),
			zap.String("nodeID", alloc.NodeID))

		// update cache
		if err := callback.context.AssumePod(alloc.AllocationKey, alloc.NodeID); err != nil {
			return err
		}
		if app := callback.context.GetApplication(alloc.ApplicationID); app != nil {
			ev := cache.NewAllocateTaskEvent(app.GetApplicationID(), alloc.AllocationKey, alloc.UUID, alloc.NodeID)
			dispatcher.Dispatch(ev)
		}
	}

	for _, reject := range response.Rejected {
		// request rejected by the scheduler, put it back and try scheduling again
		log.Logger().Debug("callback: response to rejected allocation",
			zap.String("allocationKey", reject.AllocationKey))
		if app := callback.context.GetApplication(reject.ApplicationID); app != nil {
			dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationID(), reject.AllocationKey,
				fmt.Sprintf("task %s from application %s is rejected by scheduler",
					reject.AllocationKey, reject.ApplicationID)))
		}
	}

	for _, release := range response.Released {
		log.Logger().Debug("callback: response to released allocations",
			zap.String("UUID", release.UUID))

		// update cache
		callback.context.ForgetPod(release.GetAllocationKey())

		// TerminationType 0 mean STOPPED_BY_RM
		if release.TerminationType != si.TerminationType_STOPPED_BY_RM {
			// send release app allocation to application states machine
			app := callback.context.GetApplication(release.ApplicationID).(*cache.Application)
			err := app.HandleApplicationEventWithInfo(cache.ReleaseAppAllocation, []string{release.TerminationType.String(), release.AllocationKey})
			if err != nil {
				log.Logger().Warn("BUG: Unexpected failure: Application state change failed",
					zap.String("currentState", app.GetApplicationState()),
					zap.Error(err))
			}
		}
	}

	for _, ask := range response.ReleasedAsks {
		log.Logger().Debug("callback: response to released allocations",
			zap.String("allocation key", ask.AllocationKey))

		if ask.TerminationType == si.TerminationType_TIMEOUT {
			app := callback.context.GetApplication(ask.ApplicationID).(*cache.Application)
			err := app.HandleApplicationEventWithInfo(cache.ReleaseAppAllocationAsk, []string{ask.TerminationType.String(), ask.AllocationKey})
			if err != nil {
				log.Logger().Warn("BUG: Unexpected failure: Application state change failed",
					zap.String("currentState", app.GetApplicationState()),
					zap.Error(err))
			}
		}
	}
	return nil
}

func (callback *AsyncRMCallback) UpdateApplication(response *si.ApplicationResponse) error {
	log.Logger().Debug("UpdateApplication callback received",
		zap.String("UpdateApplicationResponse", response.String()))

	// handle new accepted apps
	for _, app := range response.Accepted {
		// update context
		log.Logger().Debug("callback: response to accepted application",
			zap.String("appID", app.ApplicationID))

		if app := callback.context.GetApplication(app.ApplicationID); app != nil {
			log.Logger().Info("Accepting app", zap.String("appID", app.GetApplicationID()))
			err := app.(*cache.Application).HandleApplicationEvent(cache.AcceptApplication)
			if err != nil {
				log.Logger().Warn("BUG: Unexpected failure: Application state change failed",
					zap.String("currentState", app.GetApplicationState()),
					zap.Error(err))
			}
		}
	}

	for _, rejectedApp := range response.Rejected {
		// update context
		log.Logger().Debug("callback: response to rejected application",
			zap.String("appID", rejectedApp.ApplicationID))

		if app := callback.context.GetApplication(rejectedApp.ApplicationID); app != nil {
			err := app.(*cache.Application).HandleApplicationEventWithInfo(cache.RejectApplication, []string{rejectedApp.Reason})
			if err != nil {
				log.Logger().Warn("BUG: Unexpected failure: Application state change failed",
					zap.String("currentState", app.GetApplicationState()),
					zap.Error(err))
			}
		}
	}

	// handle status changes
	for _, updated := range response.Updated {
		log.Logger().Debug("status update callback received",
			zap.String("appId", updated.ApplicationID),
			zap.String("new status", updated.State))
		switch updated.State {
		case cache.Completed.String():
			callback.context.RemoveApplicationInternal(updated.ApplicationID)
		case cache.Resuming.String():
			app := callback.context.GetApplication(updated.ApplicationID)
			if app != nil && app.GetApplicationState() == cache.Reserving.String() {
				err := app.(*cache.Application).HandleApplicationEvent(cache.ResumingApplication)
				if err != nil {
					log.Logger().Warn("BUG: Unexpected failure: Application state change failed",
						zap.String("currentState", app.GetApplicationState()),
						zap.Error(err))
				}
				// handle status update
				dispatcher.Dispatch(cache.NewApplicationStatusChangeEvent(updated.ApplicationID, cache.AppStateChange.String(), updated.State))
			}
		default:
			if updated.State == cache.Failing.String() || updated.State == cache.Failed.String() {
				app := callback.context.GetApplication(updated.ApplicationID)
				err := app.(*cache.Application).HandleApplicationEventWithInfo(cache.FailApplication, []string{updated.Message})
				if err != nil {
					log.Logger().Warn("BUG: Unexpected failure: Application state change failed",
						zap.String("currentState", app.GetApplicationState()),
						zap.Error(err))
				}
			}
			// handle status update
			dispatcher.Dispatch(cache.NewApplicationStatusChangeEvent(updated.ApplicationID, cache.AppStateChange.String(), updated.State))
		}
	}
	return nil
}

func (callback *AsyncRMCallback) UpdateNode(response *si.NodeResponse) error {
	log.Logger().Debug("UpdateNode callback received",
		zap.String("UpdateNodeResponse", response.String()))
	// handle new accepted nodes
	for _, node := range response.Accepted {
		log.Logger().Debug("callback: response to accepted node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  events.NodeAccepted,
		})
	}

	for _, node := range response.Rejected {
		log.Logger().Debug("callback: response to rejected node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  events.NodeRejected,
		})
	}
	return nil
}

func (callback *AsyncRMCallback) Predicates(args *si.PredicatesArgs) error {
	return callback.context.IsPodFitNode(args.AllocationKey, args.NodeID, args.Allocate)
}

func (callback *AsyncRMCallback) SendEvent(eventRecords []*si.EventRecord) {
	if len(eventRecords) > 0 {
		log.Logger().Debug(fmt.Sprintf("prepare to publish %d events", len(eventRecords)))
		callback.context.PublishEvents(eventRecords)
	}
}

func (callback *AsyncRMCallback) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
	callback.context.HandleContainerStateUpdate(request)
}

func (callback *AsyncRMCallback) UpdateConfiguration(args *si.UpdateConfigurationRequest) *si.UpdateConfigurationResponse {
	return callback.context.SaveConfigmap(args)
}
