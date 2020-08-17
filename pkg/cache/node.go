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
	"sync"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// stores info about what scheduler cares about a node
type SchedulerNode struct {
	name                string
	uid                 string
	capacity            *si.Resource
	occupied            *si.Resource
	schedulable         bool
	existingAllocations []*si.Allocation
	schedulerAPI        api.SchedulerAPI
	fsm                 *fsm.FSM
	lock                *sync.RWMutex
}

func newSchedulerNode(nodeName string, nodeUID string,
	nodeResource *si.Resource, schedulerAPI api.SchedulerAPI, schedulable bool) *SchedulerNode {
	schedulerNode := &SchedulerNode{
		name:         nodeName,
		uid:          nodeUID,
		capacity:     nodeResource,
		occupied:     common.NewResourceBuilder().Build(),
		schedulerAPI: schedulerAPI,
		schedulable:  schedulable,
		lock:         &sync.RWMutex{},
	}
	schedulerNode.initFSM()
	return schedulerNode
}

func (n *SchedulerNode) initFSM() {
	var states = events.States().Node
	n.fsm = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: string(events.RecoverNode),
				Src: []string{states.New},
				Dst: states.Recovering,
			},
			{Name: string(events.NodeAccepted),
				Src: []string{states.Recovering},
				Dst: states.Accepted,
			},
			{Name: string(events.NodeReady),
				Src: []string{states.Accepted},
				Dst: states.Healthy,
			},
			{Name: string(events.NodeRejected),
				Src: []string{states.New, states.Recovering},
				Dst: states.Rejected,
			},
			{Name: string(events.DrainNode),
				Src: []string{states.Healthy, states.Accepted},
				Dst: states.Draining,
			},
			{Name: string(events.RestoreNode),
				Src: []string{states.Draining},
				Dst: states.Healthy,
			},
		},
		fsm.Callbacks{
			string(states.Recovering):  n.handleNodeRecovery,
			string(events.DrainNode):   n.handleDrainNode,
			string(events.RestoreNode): n.handleRestoreNode,
			string(states.Accepted):    n.postNodeAccepted,
			events.EnterState:          n.enterState,
		})
}

func (n *SchedulerNode) addExistingAllocation(allocation *si.Allocation) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Logger().Info("add existing allocation",
		zap.Any("allocation", allocation))
	n.existingAllocations = append(n.existingAllocations, allocation)
}

func (n *SchedulerNode) setOccupiedResource(resource *si.Resource) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Logger().Info("set node occupied resource",
		zap.String("occupied", resource.String()))
	n.occupied = resource
}

func (n *SchedulerNode) getNodeState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return n.fsm.Current()
}

func (n *SchedulerNode) postNodeAccepted(event *fsm.Event) {
	// when node is accepted, it means the node is already registered to the scheduler,
	// this doesn't mean this node is ready for scheduling, there is a step away.
	// we need to check the K8s node state, if it is not schedulable, then we should notify
	// the scheduler to not schedule new pods onto it.
	if n.schedulable {
		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: n.name,
			Event:  events.NodeReady,
		})
	} else {
		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: n.name,
			Event:  events.DrainNode,
		})
	}
}

func (n *SchedulerNode) handleNodeRecovery(event *fsm.Event) {
	log.Logger().Info("node recovering",
		zap.String("nodeID", n.name),
		zap.Bool("schedulable", n.schedulable))

	request := &si.UpdateRequest{
		Asks:     nil,
		Releases: nil,
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:              n.name,
				SchedulableResource: n.capacity,
				OccupiedResource:    n.occupied,
				Attributes: map[string]string{
					constants.DefaultNodeAttributeHostNameKey: n.name,
					constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
				},
				ExistingAllocations: n.existingAllocations,
			},
		},
		RmID: conf.GetSchedulerConf().ClusterID,
	}

	// send request to scheduler-core
	if err := n.schedulerAPI.Update(request); err != nil {
		log.Logger().Error("failed to send request",
			zap.Any("request", request))
	}
}

func (n *SchedulerNode) handleDrainNode(event *fsm.Event) {
	log.Logger().Info("node enters draining mode",
		zap.String("nodeID", n.name))

	request := &si.UpdateRequest{
		Asks:     nil,
		Releases: nil,
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID: n.name,
				Action: si.UpdateNodeInfo_DRAIN_NODE,
				Attributes: map[string]string{
					constants.DefaultNodeAttributeHostNameKey: n.name,
					constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
				},
			},
		},
		RmID: conf.GetSchedulerConf().ClusterID,
	}

	// send request to scheduler-core
	if err := n.schedulerAPI.Update(request); err != nil {
		log.Logger().Error("failed to send request",
			zap.Any("request", request))
	}
}

func (n *SchedulerNode) handleRestoreNode(event *fsm.Event) {
	log.Logger().Info("restore node from draining mode",
		zap.String("nodeID", n.name))

	request := &si.UpdateRequest{
		Asks:     nil,
		Releases: nil,
		UpdatedNodes: []*si.UpdateNodeInfo{
			{
				NodeID: n.name,
				Action: si.UpdateNodeInfo_DRAIN_TO_SCHEDULABLE,
				Attributes: map[string]string{
					constants.DefaultNodeAttributeHostNameKey: n.name,
					constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
				},
			},
		},
		RmID: conf.GetSchedulerConf().ClusterID,
	}

	// send request to scheduler-core
	if err := n.schedulerAPI.Update(request); err != nil {
		log.Logger().Error("failed to send request",
			zap.Any("request", request))
	}
}

func (n *SchedulerNode) handle(ev events.SchedulerNodeEvent) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	err := n.fsm.Event(string(ev.GetEvent()), ev.GetArgs()...)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	return nil
}

func (n *SchedulerNode) canHandle(ev events.SchedulerNodeEvent) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.fsm.Can(string(ev.GetEvent()))
}

func (n *SchedulerNode) enterState(event *fsm.Event) {
	log.Logger().Debug("shim node state transition",
		zap.String("node", n.name),
		zap.String("source", event.Src),
		zap.String("destination", event.Dst),
		zap.String("event", event.Event))
}
