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

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/federation"
	"go.uber.org/zap"

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
	log.Logger().Debug("callback received",
		zap.String("updateResponse", response.String()))

	// handle new accepted nodes
	for _, node := range response.AcceptedNodes {
		log.Logger().Debug("callback: response to accepted node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  events.NodeAccepted,
		})

		confirmedNode := callback.context.GetNode(node.NodeID)
		if confirmedNode != nil {
			log.Logger().Info("reporting new node to the head server",
				zap.String("nodeID", node.NodeID))
			dispatcher.Dispatch(federation.AsyncUpdateFederationEvent{
				ConfirmedRequests: []*si.UpdateRequest{
					{
						NewSchedulableNodes:  []*si.NewNodeInfo{
							{
								NodeID: node.NodeID,
								Attributes: map[string]string{ "compute-node": "yes"},
								SchedulableResource: confirmedNode.GetCapacity(),
								OccupiedResource: confirmedNode.GetOccupiedResources(),
							},
						},
						RmID: conf.GetSchedulerConf().ClusterID,
					},
				},
			})
		} else {
			log.Logger().Error("node accepted in sub cluster, but not found in cache",
				zap.String("nodeID", node.NodeID))
		}
	}

	for _, node := range response.RejectedNodes {
		log.Logger().Debug("callback: response to rejected node",
			zap.String("nodeID", node.NodeID))

		dispatcher.Dispatch(cache.CachedSchedulerNodeEvent{
			NodeID: node.NodeID,
			Event:  events.NodeRejected,
		})
	}

	// handle new accepted apps
	for _, app := range response.AcceptedApplications {
		// update context
		log.Logger().Debug("callback: response to accepted application",
			zap.String("appID", app.ApplicationID))

		if app := callback.context.GetApplication(app.ApplicationID); app != nil {
			ev := cache.NewSimpleApplicationEvent(app.GetApplicationID(), events.AcceptApplication)
			dispatcher.Dispatch(ev)
		}

		if appInfo := callback.context.GetApplication(app.ApplicationID); appInfo != nil {
			log.Logger().Info("reporting new application to the head server",
				zap.String("appID", app.ApplicationID))
			dispatcher.Dispatch(federation.AsyncUpdateFederationEvent{
				ConfirmedRequests: []*si.UpdateRequest{
					{
						NewApplications:  []*si.AddApplicationRequest{
							{
								ApplicationID: app.ApplicationID,
								QueueName: appInfo.GetQueue(),
								PartitionName: "default",
								Ugi: &si.UserGroupInformation{
									User: "anonymous",
								},
							},
						},
						RmID: conf.GetSchedulerConf().ClusterID,
					},
				},
			})
		} else {
			log.Logger().Error("app accepted in sub cluster, but not found in cache",
				zap.String("appID", app.ApplicationID))
		}
	}

	for _, app := range response.RejectedApplications {
		// update context
		log.Logger().Debug("callback: response to rejected application",
			zap.String("appID", app.ApplicationID))

		if app := callback.context.GetApplication(app.ApplicationID); app != nil {
			ev := cache.NewSimpleApplicationEvent(app.GetApplicationID(), events.RejectApplication)
			dispatcher.Dispatch(ev)
		}
	}

	// handle new allocations
	for _, alloc := range response.NewAllocations {
		// got allocation for pod, bind pod to the scheduled node
		log.Logger().Debug("callback: response to new allocation",
			zap.String("allocationKey", alloc.AllocationKey),
			zap.String("UUID", alloc.UUID),
			zap.String("applicationID", alloc.ApplicationID),
			zap.String("nodeID", alloc.NodeID))

		if app := callback.context.GetApplication(alloc.ApplicationID); app != nil {
			ev := cache.NewAllocateTaskEvent(app.GetApplicationID(), alloc.AllocationKey, alloc.UUID, alloc.NodeID)
			dispatcher.Dispatch(ev)

			if updatedNode := callback.context.GetNode(alloc.NodeID); updatedNode != nil {
				if task, err := app.GetTask(alloc.AllocationKey); err == nil {
					dispatcher.Dispatch(federation.AsyncUpdateFederationEvent{
						ConfirmedRequests: []*si.UpdateRequest{
							{
								UpdatedNodes: []*si.UpdateNodeInfo{
									{
										NodeID:              alloc.NodeID,
										Attributes:          map[string]string{"compute-node": "yes"},
										SchedulableResource: updatedNode.GetCapacity(),
										OccupiedResource:    updatedNode.GetOccupiedResources(),
										ExistingAllocations: []*si.Allocation{
											{
												AllocationKey:    string(task.GetTaskPod().UID),
												UUID:             string(task.GetTaskPod().UID),
												ResourcePerAlloc: common.GetPodResource(task.GetTaskPod()),
												QueueName:        app.GetQueue(),
												NodeID:           alloc.NodeID,
												ApplicationID:    app.GetApplicationID(),
												PartitionName:    constants.DefaultPartition,
											},
										},
										Action: si.UpdateNodeInfo_UPDATE,
									},
								},
								RmID: conf.GetSchedulerConf().ClusterID,
							},
						},
					})
				}
			}
		}
	}

	for _, reject := range response.RejectedAllocations {
		// request rejected by the scheduler, put it back and try scheduling again
		log.Logger().Debug("callback: response to rejected allocation",
			zap.String("allocationKey", reject.AllocationKey))

		if app := callback.context.GetApplication(reject.ApplicationID); app != nil {
			dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationID(), reject.AllocationKey,
				fmt.Sprintf("task %s from application %s is rejected by scheduler",
					reject.AllocationKey, reject.ApplicationID)))
		}
	}

	for _, release := range response.ReleasedAllocations {
		log.Logger().Debug("callback: response to released allocations",
			zap.String("UUID", release.UUID))
	}

	// handle status changes
	for _, updated := range response.UpdatedApplications {
		log.Logger().Debug("status update callback received",
			zap.String("appId", updated.ApplicationID),
			zap.String("new status", updated.State))

		//handle status update
		dispatcher.Dispatch(cache.NewApplicationStatusChangeEvent(updated.ApplicationID, events.AppStateChange, updated.State))
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
func (callback *AsyncRMCallback) SendEvent(eventRecords []*si.EventRecord) {
	if len(eventRecords) > 0 {
		log.Logger().Debug(fmt.Sprintf("prepare to publish %d events", len(eventRecords)))
		callback.context.PublishEvents(eventRecords)
	}
}

// this callback implements scheduler plugin interface ContainerSchedulingStateUpdater.
func (callback *AsyncRMCallback) Update(request *si.UpdateContainerSchedulingStateRequest) {
	callback.context.HandleContainerStateUpdate(request)
}
