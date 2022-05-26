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

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

var nodeStatesOnce sync.Once

//----------------------------------------------
// SchedulerNode events
//----------------------------------------------
type SchedulerNodeEventType int

const (
	RecoverNode SchedulerNodeEventType = iota
	NodeAccepted
	NodeRejected
	DrainNode
	RestoreNode
	NodeReady
)

func (ae SchedulerNodeEventType) String() string {
	return [...]string{"RecoverNode", "NodeAccepted", "NodeRejected", "DrainNode", "RestoreNode", "NodeReady"}[ae]
}

type CachedSchedulerNodeEvent struct {
	NodeID string
	Event  SchedulerNodeEventType
}

func (sn CachedSchedulerNodeEvent) GetEvent() string {
	return sn.Event.String()
}

func (sn CachedSchedulerNodeEvent) GetNodeID() string {
	return sn.NodeID
}

func (sn CachedSchedulerNodeEvent) GetArgs() []interface{} {
	return nil
}

// ----------------------------------
// SchedulerNode states
// ----------------------------------
var storeSchedulerNodeStates *NStates

type NStates struct {
	New        string
	Recovering string
	Accepted   string
	Healthy    string
	Rejected   string
	Draining   string
}

func SchedulerNodeStates() *NStates {
	nodeStatesOnce.Do(func() {
		storeSchedulerNodeStates = &NStates{
			New:        "New",
			Recovering: "Recovering",
			Accepted:   "Accepted",
			Healthy:    "Healthy",
			Rejected:   "Rejected",
			Draining:   "Draining",
		}
	})
	return storeSchedulerNodeStates
}

func newSchedulerNodeState() *fsm.FSM {
	states := SchedulerNodeStates()
	return fsm.NewFSM(
		states.New, fsm.Events{
			{
				Name: RecoverNode.String(),
				Src:  []string{states.New},
				Dst:  states.Recovering,
			},
			{
				Name: NodeAccepted.String(),
				Src:  []string{states.Recovering},
				Dst:  states.Accepted,
			},
			{
				Name: NodeReady.String(),
				Src:  []string{states.Accepted},
				Dst:  states.Healthy,
			},
			{
				Name: NodeRejected.String(),
				Src:  []string{states.New, states.Recovering},
				Dst:  states.Rejected,
			},
			{
				Name: DrainNode.String(),
				Src:  []string{states.Healthy, states.Accepted},
				Dst:  states.Draining,
			},
			{
				Name: RestoreNode.String(),
				Src:  []string{states.Draining},
				Dst:  states.Healthy,
			},
		},
		fsm.Callbacks{
			events.EnterState: func(event *fsm.Event) {
				node := event.Args[0].(*SchedulerNode) //nolint:errcheck
				log.Logger().Debug("shim node state transition",
					zap.String("nodeID", node.name),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
			states.Accepted: func(event *fsm.Event) {
				node := event.Args[0].(*SchedulerNode) //nolint:errcheck
				node.postNodeAccepted()
			},
			states.Recovering: func(event *fsm.Event) {
				node := event.Args[0].(*SchedulerNode) //nolint:errcheck
				node.handleNodeRecovery()
			},
			DrainNode.String(): func(event *fsm.Event) {
				node := event.Args[0].(*SchedulerNode) //nolint:errcheck
				node.handleDrainNode()
			},
			RestoreNode.String(): func(event *fsm.Event) {
				node := event.Args[0].(*SchedulerNode) //nolint:errcheck
				node.handleRestoreNode()
			},
		},
	)
}
