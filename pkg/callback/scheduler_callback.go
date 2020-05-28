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
	"strings"

	"go.uber.org/zap"
	"k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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
	log.Logger.Debug("callback received",
		zap.String("updateResponse", response.String()))

	// handle new accepted nodes
	for _, node := range response.AcceptedNodes {
		log.Logger.Debug("callback: response to accepted node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  events.NodeAccepted,
		})
	}

	for _, node := range response.RejectedNodes {
		log.Logger.Debug("callback: response to rejected node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  events.NodeRejected,
		})
	}

	// handle new accepted apps
	for _, app := range response.AcceptedApplications {
		// update context
		log.Logger.Debug("callback: response to accepted application",
			zap.String("appID", app.ApplicationID))

		if app := callback.context.GetApplication(app.ApplicationID); app != nil {
			ev := cache.NewSimpleApplicationEvent(app.GetApplicationID(), events.AcceptApplication)
			dispatcher.Dispatch(ev)
		}
	}

	for _, app := range response.RejectedApplications {
		// update context
		log.Logger.Debug("callback: response to rejected application",
			zap.String("appID", app.ApplicationID))

		if app := callback.context.GetApplication(app.ApplicationID); app != nil {
			ev := cache.NewSimpleApplicationEvent(app.GetApplicationID(), events.RejectApplication)
			dispatcher.Dispatch(ev)
		}
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		log.Logger.Debug("callback: response to new allocation",
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("UUID", alloc.UUID),
			zap.String("applicationID", alloc.ApplicationID),
			zap.String("nodeID", alloc.NodeID))

		if app := callback.context.GetApplication(alloc.ApplicationID); app != nil {
			ev := cache.NewAllocateTaskEvent(app.GetApplicationID(), alloc.AllocationKey, alloc.UUID, alloc.NodeID)
			dispatcher.Dispatch(ev)
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		log.Logger.Debug("callback: response to rejected allocation",
			zap.String("allocationKey", reject.AllocationKey))

		if app := callback.context.GetApplication(reject.ApplicationID); app != nil {
			dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationID(), reject.AllocationKey,
				fmt.Sprintf("task %s from application %s is rejected by scheduler",
					reject.AllocationKey, reject.ApplicationID)))
		}
	}

	for _, release := range response.ReleasedAllocations {
		log.Logger.Debug("callback: response to released allocations",
			zap.String("UUID", release.UUID))
	}

	return nil
}

// this callback implements scheduler plugin interface PredicatesPlugin/
func (callback *AsyncRMCallback) Predicates(args *si.PredicatesArgs) error {
	return callback.context.IsPodFitNode(args.AllocationKey, args.NodeID, args.Allocate)
}

// this callback implements scheduler plugin interface ReconcilePlugin.
func (callback *AsyncRMCallback) ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error {
	for _, assumedAlloc := range args.AssumedAllocations {
		if err := callback.context.AssumePod(assumedAlloc.AllocationKey, assumedAlloc.NodeID); err != nil {
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

// this callback implement scheduler plugin interface EventPlugin.
func (callback *AsyncRMCallback) SendEvent(eventMessages []*si.EventMessage) error {
	errors := make([]string, 0)
	if len(eventMessages) > 0 {
		log.Logger.Debug(fmt.Sprintf("processing %d events", len(eventMessages)))
		for _, event := range eventMessages {
			reason := event.Reason
			msg := event.Message

			switch event.Type {
			case si.EventMessage_REQUEST:
				taskAppID := event.ID
				parts := strings.Split(taskAppID, ",")
				taskID := parts[0]
				// The application might have contained ","
				appID := strings.Join(parts[1:], ",")

				log.Logger.Info("taskID", zap.String("taskID", taskID))
				app := callback.context.GetApplication(appID)
				task, err := app.GetTask(taskID)
				if err != nil {
					errors = append(errors, fmt.Sprintf("could not find %s task belonging to %s app", taskID, appID))
					continue
				}
				pod := task.GetTaskPod()
				if pod == nil {
					errors = append(errors, fmt.Sprintf("could not obtain %s task's pod", taskID))
				}

				// TODO remove this
				log.Logger.Debug("Emitting event", zap.String("pod name", pod.ObjectMeta.Name), zap.String("reason", reason), zap.String("message", msg))
				events.GetRecorder().Event(pod, v1.EventTypeWarning, reason, msg)
				log.Logger.Debug("event emitted")
			case si.EventMessage_APP:
				// until we don't have app CRD let's expose app event to all its pods (asks)
				// pending on YUNIKORN-170
				errors = append(errors, fmt.Sprintf("processing app event is not implemented yet: %s", event))
			default:
				errors = append(errors, fmt.Sprintf("could not process unknown event type: %s", event))
			}
		}
	}
	return fmt.Errorf(strings.Join(errors, "\n"))
}
