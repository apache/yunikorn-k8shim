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

package cache

import (
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
	"sync"
)

// stores info about what scheduler cares about a node
type SchedulerNode struct {
	name                string
	uid                 string
	capacity            *si.Resource
	existingAllocations []*si.Allocation
	schedulerApi        api.SchedulerApi
	fsm                 *fsm.FSM
	lock                *sync.RWMutex
}

func newSchedulerNode(nodeName string, nodeUid string,
	nodeResource *si.Resource, schedulerApi api.SchedulerApi) *SchedulerNode {
	schedulerNode := &SchedulerNode{
		name: nodeName,
		uid:  nodeUid,
		capacity: nodeResource,
		schedulerApi: schedulerApi,
		lock: &sync.RWMutex{},
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
				Dst: states.Healthy,
			},
			{Name: string(events.NodeRejected),
				Src: []string{states.New, states.Recovering},
				Dst: states.Rejected,
			},
		},
		fsm.Callbacks{
			string(states.Recovering): n.handleNodeRecovery,
		})
}

func (n *SchedulerNode) addExistingAllocation(allocation *si.Allocation) {
	n.lock.Lock()
	defer n.lock.Unlock()
	log.Logger.Info("add existing allocation",
		zap.Any("allocation", allocation))
	n.existingAllocations = append(n.existingAllocations, allocation)
}

func (n *SchedulerNode) getNodeState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return n.fsm.Current()
}

func (n *SchedulerNode) handleNodeRecovery(event *fsm.Event) {
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Logger.Info("node recovering",
		zap.String("nodeId", n.name))

	request := &si.UpdateRequest{
		Asks:     nil,
		Releases: nil,
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeId:              n.name,
				SchedulableResource: n.capacity,
				Attributes: map[string]string{
					common.DefaultNodeAttributeHostNameKey: n.name,
					common.DefaultNodeAttributeRackNameKey: common.DefaultRackName,
				},
				ExistingAllocations: n.existingAllocations,
			},
		},
		RmId: conf.GetSchedulerConf().ClusterId,
	}

	// send request to scheduler-core
	if err := n.schedulerApi.Update(request); err != nil {
		log.Logger.Error("failed to send request",
			zap.Any("request", request))
	}
}

func (n *SchedulerNode) handle(ev events.SchedulerNodeEvent) error {
	log.Logger.Debug("scheduler node state transition",
		zap.String("nodeId", ev.GetNodeId()),
		zap.String("preState", n.fsm.Current()),
		zap.String("pendingEvent", string(ev.GetEvent())))
	err := n.fsm.Event(string(ev.GetEvent()), ev.GetArgs()...)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition"{
		return err
	}
	log.Logger.Debug("scheduler node state transition",
		zap.String("nodeId", ev.GetNodeId()),
		zap.String("postState", n.fsm.Current()))
	return nil
}
