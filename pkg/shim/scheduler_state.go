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

package shim

import (
	"sync"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

var schedulerStatesOnce sync.Once

//----------------------------------------------
// Scheduler events
//----------------------------------------------
type SchedulerEventType int

const (
	RegisterScheduler SchedulerEventType = iota
	RegisterSchedulerSucceed
	RegisterSchedulerFailed
	RecoverScheduler
	RecoverSchedulerSucceed
	RecoverSchedulerFailed
)

func (ae SchedulerEventType) String() string {
	return [...]string{"RegisterScheduler", "RegisterSchedulerSucceed", "RegisterSchedulerFailed", "RecoverScheduler", "RecoverSchedulerSucceed", "RecoverSchedulerFailed"}[ae]
}

type ShimSchedulerEvent struct { //nolint:golint
	event SchedulerEventType
}

func (rs ShimSchedulerEvent) GetEvent() string {
	return rs.event.String()
}

func (rs ShimSchedulerEvent) GetArgs() []interface{} {
	return nil
}

// -------------------------------------------------------------------
// event to trigger scheduler registration
// --------------------------------------------------------------------
type RegisterSchedulerEvent struct {
	event SchedulerEventType
}

func newRegisterSchedulerEvent() RegisterSchedulerEvent {
	return RegisterSchedulerEvent{
		event: RegisterScheduler,
	}
}

func (rs RegisterSchedulerEvent) GetEvent() string {
	return rs.event.String()
}

func (rs RegisterSchedulerEvent) GetArgs() []interface{} {
	return nil
}

// ----------------------------------
// Scheduler states
// ----------------------------------
var storeScheduleStates *SStates

type SStates struct {
	New         string
	Registered  string
	Registering string
	Recovering  string
	Running     string
	Draining    string
	Stopped     string
}

func SchedulerStates() *SStates {
	schedulerStatesOnce.Do(func() {
		storeScheduleStates = &SStates{
			New:         "New",
			Registered:  "Registered",
			Registering: "Registering",
			Recovering:  "Recovering",
			Running:     "Running",
			Draining:    "Draining",
			Stopped:     "Stopped",
		}
	})
	return storeScheduleStates
}

func newSchedulerState() *fsm.FSM {
	states := SchedulerStates()
	return fsm.NewFSM(
		states.New, fsm.Events{
			{
				Name: RegisterScheduler.String(),
				Src:  []string{states.New},
				Dst:  states.Registering,
			},
			{
				Name: RegisterSchedulerSucceed.String(),
				Src:  []string{states.Registering},
				Dst:  states.Registered,
			},
			{
				Name: RegisterSchedulerFailed.String(),
				Src:  []string{states.Registering},
				Dst:  states.Stopped,
			},
			{
				Name: RecoverScheduler.String(),
				Src:  []string{states.Registered},
				Dst:  states.Recovering,
			},
			{
				Name: RecoverSchedulerSucceed.String(),
				Src:  []string{states.Recovering},
				Dst:  states.Running,
			},
			{
				Name: RecoverSchedulerFailed.String(),
				Src:  []string{states.Recovering},
				Dst:  states.Stopped,
			},
		},
		fsm.Callbacks{
			events.EnterState: func(event *fsm.Event) {
				log.Logger().Debug("scheduler shim state transition",
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
			states.Registered: func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.triggerSchedulerStateRecovery()    // if reaches registered, trigger recovering
			},
			states.Recovering: func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.recoverSchedulerState()            // do recovering
			},
			states.Running: func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.doScheduling()                     // do scheduling
			},
			RegisterScheduler.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.register()                         // trigger registration
			},
			RegisterSchedulerFailed.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.handleSchedulerFailure()           // registration failed, stop the scheduler
			},
			RecoverSchedulerFailed.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.handleSchedulerFailure()           // recovery failed
			},
		},
	)
}
