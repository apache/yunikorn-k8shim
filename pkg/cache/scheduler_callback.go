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

package cache

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// RM callback is called from the scheduler core, we need to ensure the response is handled
// asynchronously to avoid blocking the scheduler.
type AsyncRMCallback struct {
	context *Context
}

var _ api.ResourceManagerCallback = &AsyncRMCallback{}
var _ api.StateDumpPlugin = &AsyncRMCallback{}

func NewAsyncRMCallback(ctx *Context) *AsyncRMCallback {
	return &AsyncRMCallback{context: ctx}
}

func (callback *AsyncRMCallback) UpdateAllocation(response *si.AllocationResponse) error {
	log.Log(log.ShimRMCallback).Debug("UpdateAllocation callback received",
		zap.Stringer("UpdateAllocationResponse", response))
	// handle new allocations
	for _, alloc := range response.New {
		// got allocation for pod, bind pod to the scheduled node
		log.Log(log.ShimRMCallback).Debug("callback: response to new allocation",
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("applicationID", alloc.ApplicationID),
			zap.String("nodeID", alloc.NodeID))

		// update cache
		task := callback.context.getTask(alloc.ApplicationID, alloc.AllocationKey)
		if task == nil {
			log.Log(log.ShimRMCallback).Warn("Unable to get task", zap.String("taskID", alloc.AllocationKey))
			continue
		}

		task.setAllocationKey(alloc.AllocationKey)

		if err := callback.context.AssumePod(alloc.AllocationKey, alloc.NodeID); err != nil {
			task.FailWithEvent(err.Error(), "AssumePodError")
			return err
		}

		if utils.IsAssignedPod(task.GetTaskPod()) {
			// task is already bound, fixup state and continue
			task.MarkPreviouslyAllocated(alloc.AllocationKey, alloc.NodeID)
		} else {
			ev := NewAllocateTaskEvent(alloc.ApplicationID, task.taskID, alloc.AllocationKey, alloc.NodeID)
			dispatcher.Dispatch(ev)
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, reject it
		log.Log(log.ShimRMCallback).Debug("callback: response to rejected allocation",
			zap.String("allocationKey", reject.AllocationKey))
		dispatcher.Dispatch(NewRejectTaskEvent(reject.ApplicationID, reject.AllocationKey,
			fmt.Sprintf("task %s allocation from application %s is rejected by scheduler",
				reject.AllocationKey, reject.ApplicationID)))
	}

	for _, release := range response.Released {
		log.Log(log.ShimRMCallback).Debug("callback: response to released allocations",
			zap.String("AllocationKey", release.AllocationKey))

		// update cache
		callback.context.ForgetPod(release.GetAllocationKey())

		// TerminationType 0 mean STOPPED_BY_RM, but we also need to do the release when task failed,
		// we also should send release event to application in case task failed but the pod is still pending.
		// send release app allocation to application states machine
		ev := NewReleaseAppAllocationEvent(release.ApplicationID, release.TerminationType, release.AllocationKey)
		dispatcher.Dispatch(ev)
	}

	return nil
}

func (callback *AsyncRMCallback) UpdateApplication(response *si.ApplicationResponse) error {
	log.Log(log.ShimRMCallback).Debug("UpdateApplication callback received",
		zap.Stringer("UpdateApplicationResponse", response))

	// handle new accepted apps
	for _, app := range response.Accepted {
		// update context
		log.Log(log.ShimRMCallback).Debug("callback: response to accepted application",
			zap.String("appID", app.ApplicationID))

		if app := callback.context.GetApplication(app.ApplicationID); app != nil {
			log.Log(log.ShimRMCallback).Info("Accepting app", zap.String("appID", app.GetApplicationID()))
			ev := NewSimpleApplicationEvent(app.GetApplicationID(), AcceptApplication)
			dispatcher.Dispatch(ev)
		}
	}

	for _, rejectedApp := range response.Rejected {
		// update context
		log.Log(log.ShimRMCallback).Debug("callback: response to rejected application",
			zap.String("appID", rejectedApp.ApplicationID))

		if app := callback.context.GetApplication(rejectedApp.ApplicationID); app != nil {
			ev := NewApplicationEvent(app.GetApplicationID(), RejectApplication, rejectedApp.Reason)
			dispatcher.Dispatch(ev)
		}
	}

	// handle status changes
	for _, updated := range response.Updated {
		log.Log(log.ShimRMCallback).Debug("status update callback received",
			zap.String("appId", updated.ApplicationID),
			zap.String("new status", updated.State))
		switch updated.State {
		case ApplicationStates().Completed:
			callback.context.RemoveApplication(updated.ApplicationID)
		case ApplicationStates().Resuming:
			app := callback.context.GetApplication(updated.ApplicationID)
			if app != nil && app.GetApplicationState() == ApplicationStates().Reserving {
				ev := NewResumingApplicationEvent(updated.ApplicationID)
				dispatcher.Dispatch(ev)
			}
		case ApplicationStates().Failing, ApplicationStates().Failed:
			ev := NewFailApplicationEvent(updated.ApplicationID, updated.Message)
			dispatcher.Dispatch(ev)
		}
	}
	return nil
}

func (callback *AsyncRMCallback) UpdateNode(response *si.NodeResponse) error {
	log.Log(log.ShimRMCallback).Debug("UpdateNode callback received",
		zap.Stringer("UpdateNodeResponse", response))
	// handle new accepted nodes
	for _, node := range response.Accepted {
		log.Log(log.ShimRMCallback).Debug("callback: response to accepted node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  NodeAccepted,
		})
	}

	for _, node := range response.Rejected {
		log.Log(log.ShimRMCallback).Debug("callback: response to rejected node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  NodeRejected,
		})
	}
	return nil
}

func (callback *AsyncRMCallback) Predicates(args *si.PredicatesArgs) error {
	return callback.context.IsPodFitNode(args.AllocationKey, args.NodeID, args.Allocate)
}

func (callback *AsyncRMCallback) PreemptionPredicates(args *si.PreemptionPredicatesArgs) *si.PreemptionPredicatesResponse {
	index, ok := callback.context.IsPodFitNodeViaPreemption(args.AllocationKey, args.NodeID, args.PreemptAllocationKeys, int(args.StartIndex))
	if !ok {
		index = -1
	}
	return &si.PreemptionPredicatesResponse{
		Success: ok,
		Index:   int32(index),
	}
}

func (callback *AsyncRMCallback) SendEvent(eventRecords []*si.EventRecord) {
	if len(eventRecords) > 0 {
		log.Log(log.ShimRMCallback).Debug(fmt.Sprintf("prepare to publish %d events", len(eventRecords)))
		callback.context.PublishEvents(eventRecords)
	}
}

func (callback *AsyncRMCallback) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
	callback.context.HandleContainerStateUpdate(request)
}

// StateDumpPlugin implementation

func (callback *AsyncRMCallback) GetStateDump() (string, error) {
	log.Log(log.ShimRMCallback).Debug("Retrieving shim state dump")
	return callback.context.GetStateDump()
}
