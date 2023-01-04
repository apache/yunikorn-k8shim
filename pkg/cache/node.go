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

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// stores info about what scheduler cares about a node
type SchedulerNode struct {
	name         string
	uid          string
	labels       string
	schedulable  bool
	schedulerAPI api.SchedulerAPI
	fsm          *fsm.FSM

	// mutable values need locking
	capacity            *si.Resource
	occupied            *si.Resource
	ready               bool
	existingAllocations []*si.Allocation

	lock *sync.RWMutex
}

func newSchedulerNode(nodeName string, nodeUID string, nodeLabels string,
	nodeResource *si.Resource, schedulerAPI api.SchedulerAPI, schedulable bool, ready bool) *SchedulerNode {
	schedulerNode := &SchedulerNode{
		name:         nodeName,
		uid:          nodeUID,
		labels:       nodeLabels,
		capacity:     nodeResource,
		occupied:     common.NewResourceBuilder().Build(),
		schedulerAPI: schedulerAPI,
		schedulable:  schedulable,
		lock:         &sync.RWMutex{},
		ready:        ready,
		fsm:          newSchedulerNodeState(),
	}
	return schedulerNode
}

func (n *SchedulerNode) snapshotState() (capacity *si.Resource, occupied *si.Resource, ready bool) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.capacity, n.occupied, n.ready
}

func (n *SchedulerNode) addExistingAllocation(allocation *si.Allocation) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Logger().Info("add existing allocation",
		zap.String("nodeID", n.name),
		zap.Any("allocation", allocation))
	n.existingAllocations = append(n.existingAllocations, allocation)
}

func (n *SchedulerNode) updateOccupiedResource(resource *si.Resource, opt updateType) (capacity *si.Resource, occupied *si.Resource, ready bool) {
	n.lock.Lock()
	defer n.lock.Unlock()
	switch opt {
	case AddOccupiedResource:
		log.Logger().Info("add node occupied resource",
			zap.String("nodeID", n.name),
			zap.String("occupied", resource.String()))
		n.occupied = common.Add(n.occupied, resource)
	case SubOccupiedResource:
		log.Logger().Info("subtract node occupied resource",
			zap.String("nodeID", n.name),
			zap.String("occupied", resource.String()))
		n.occupied = common.Sub(n.occupied, resource)
	default:
		// noop
	}
	return n.capacity, n.occupied, n.ready
}

func (n *SchedulerNode) setCapacity(capacity *si.Resource) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Logger().Debug("set node capacity",
		zap.String("nodeID", n.name),
		zap.String("capacity", capacity.String()))
	n.capacity = capacity
}

func (n *SchedulerNode) setReadyStatus(ready bool) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Logger().Debug("set node ready status",
		zap.String("nodeID", n.name),
		zap.Bool("ready", ready))
	n.ready = ready
}

func (n *SchedulerNode) getNodeState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return n.fsm.Current()
}

func (n *SchedulerNode) postNodeAccepted() {
	// when node is accepted, it means the node is already registered to the scheduler,
	// this doesn't mean this node is ready for scheduling, there is a step away.
	// we need to check the K8s node state, if it is not schedulable, then we should notify
	// the scheduler to not schedule new pods onto it.
	if n.schedulable {
		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: n.name,
			Event:  NodeReady,
		})
	} else {
		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: n.name,
			Event:  DrainNode,
		})
	}
}

func (n *SchedulerNode) handleNodeRecovery() {
	log.Logger().Info("node recovering",
		zap.String("nodeID", n.name),
		zap.Bool("schedulable", n.schedulable))

	nodeRequest := common.CreateUpdateRequestForNewNode(n.name, n.labels, n.capacity, n.occupied, n.existingAllocations, n.ready)

	// send node request to scheduler-core
	if err := n.schedulerAPI.UpdateNode(&nodeRequest); err != nil {
		log.Logger().Error("failed to send UpdateNode request",
			zap.Any("request", nodeRequest))
	}
}

func (n *SchedulerNode) handleDrainNode() {
	log.Logger().Info("node enters draining mode",
		zap.String("nodeID", n.name))

	nodeRequest := common.CreateUpdateRequestForDeleteOrRestoreNode(n.name, si.NodeInfo_DRAIN_NODE)

	// send request to scheduler-core
	if err := n.schedulerAPI.UpdateNode(&nodeRequest); err != nil {
		log.Logger().Error("failed to send UpdateNode request",
			zap.Any("request", nodeRequest))
	}
}

func (n *SchedulerNode) handleRestoreNode() {
	log.Logger().Info("restore node from draining mode",
		zap.String("nodeID", n.name))

	nodeRequest := common.CreateUpdateRequestForDeleteOrRestoreNode(n.name, si.NodeInfo_DRAIN_TO_SCHEDULABLE)

	// send request to scheduler-core
	if err := n.schedulerAPI.UpdateNode(&nodeRequest); err != nil {
		log.Logger().Error("failed to send UpdateNode request",
			zap.Any("request", nodeRequest))
	}
}

func (n *SchedulerNode) handle(ev events.SchedulerNodeEvent) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	err := n.fsm.Event(ev.GetEvent(), n)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	return nil
}

func (n *SchedulerNode) canHandle(ev events.SchedulerNodeEvent) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.fsm.Can(ev.GetEvent())
}
