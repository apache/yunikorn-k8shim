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

var taskStatesOnce sync.Once

//----------------------------------------------
// Task events
//----------------------------------------------
type TaskEventType int

const (
	InitTask TaskEventType = iota
	SubmitTask
	TaskAllocated
	TaskRejected
	TaskBound
	CompleteTask
	TaskFail
	KillTask
	TaskKilled
)

func (ae TaskEventType) String() string {
	return [...]string{"InitTask", "SubmitTask", "TaskAllocated", "TaskRejected", "TaskBound", "CompleteTask", "TaskFail", "KillTask", "TaskKilled"}[ae]
}

// ------------------------
// Simple task Event simply moves task to next state, it has no arguments provided
// ------------------------
type SimpleTaskEvent struct {
	applicationID string
	taskID        string
	event         TaskEventType
}

func NewSimpleTaskEvent(appID string, taskID string, taskType TaskEventType) SimpleTaskEvent {
	return SimpleTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         taskType,
	}
}

func (st SimpleTaskEvent) GetEvent() string {
	return st.event.String()
}

func (st SimpleTaskEvent) GetArgs() []interface{} {
	return nil
}

func (st SimpleTaskEvent) GetTaskID() string {
	return st.taskID
}

func (st SimpleTaskEvent) GetApplicationID() string {
	return st.applicationID
}

// ------------------------
// SubmitTask Event
// ------------------------
type SubmitTaskEvent struct {
	applicationID string
	taskID        string
	event         TaskEventType
}

func NewSubmitTaskEvent(appID string, taskID string) SubmitTaskEvent {
	return SubmitTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         SubmitTask,
	}
}

func (st SubmitTaskEvent) GetEvent() string {
	return st.event.String()
}

func (st SubmitTaskEvent) GetArgs() []interface{} {
	return nil
}

func (st SubmitTaskEvent) GetTaskID() string {
	return st.taskID
}

func (st SubmitTaskEvent) GetApplicationID() string {
	return st.applicationID
}

// ------------------------
// Allocate Event
// ------------------------
type AllocatedTaskEvent struct {
	applicationID  string
	taskID         string
	event          TaskEventType
	nodeID         string
	allocationUUID string
}

func NewAllocateTaskEvent(appID string, taskID string, allocUUID string, nid string) AllocatedTaskEvent {
	return AllocatedTaskEvent{
		applicationID:  appID,
		taskID:         taskID,
		event:          TaskAllocated,
		allocationUUID: allocUUID,
		nodeID:         nid,
	}
}

func (ae AllocatedTaskEvent) GetEvent() string {
	return ae.event.String()
}

func (ae AllocatedTaskEvent) GetArgs() []interface{} {
	args := make([]interface{}, 2)
	args[0] = ae.allocationUUID
	args[1] = ae.nodeID
	return args
}

func (ae AllocatedTaskEvent) GetTaskID() string {
	return ae.taskID
}

func (ae AllocatedTaskEvent) GetApplicationID() string {
	return ae.applicationID
}

// ------------------------
// Bound Event
// ------------------------
type BindTaskEvent struct {
	applicationID string
	taskID        string
	event         TaskEventType
}

func NewBindTaskEvent(appID string, taskID string) BindTaskEvent {
	return BindTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         TaskBound,
	}
}

func (bt BindTaskEvent) GetEvent() string {
	return bt.event.String()
}

func (bt BindTaskEvent) GetArgs() []interface{} {
	return nil
}

func (bt BindTaskEvent) GetTaskID() string {
	return bt.taskID
}

func (bt BindTaskEvent) GetApplicationID() string {
	return bt.applicationID
}

// ------------------------
// Fail Event
// ------------------------
type FailTaskEvent struct {
	applicationID string
	taskID        string
	event         TaskEventType
	message       string
}

func NewFailTaskEvent(appID string, taskID string, failedMessage string) FailTaskEvent {
	return FailTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         TaskFail,
		message:       failedMessage,
	}
}

func (fe FailTaskEvent) GetEvent() string {
	return fe.event.String()
}

func (fe FailTaskEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = fe.message
	return args
}

func (fe FailTaskEvent) GetTaskID() string {
	return fe.taskID
}

func (fe FailTaskEvent) GetApplicationID() string {
	return fe.applicationID
}

// ------------------------
// Reject Event
// ------------------------
type RejectTaskEvent struct {
	applicationID string
	taskID        string
	event         TaskEventType
	message       string
}

func NewRejectTaskEvent(appID string, taskID string, rejectedMessage string) RejectTaskEvent {
	return RejectTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         TaskRejected,
		message:       rejectedMessage,
	}
}

func (re RejectTaskEvent) GetEvent() string {
	return re.event.String()
}

func (re RejectTaskEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = re.message
	return args
}

func (re RejectTaskEvent) GetTaskID() string {
	return re.taskID
}

func (re RejectTaskEvent) GetApplicationID() string {
	return re.applicationID
}

// ----------------------------------
// task states
// ----------------------------------
var storeTaskStates *TStates

type TStates struct {
	New        string
	Pending    string
	Scheduling string
	Allocated  string
	Rejected   string
	Bound      string
	Killing    string
	Killed     string
	Failed     string
	Completed  string
	Any        []string // Any refers to all possible states
	Terminated []string // Rejected, Killed, Failed, Completed
}

func TaskStates() *TStates {
	taskStatesOnce.Do(func() {
		storeTaskStates = &TStates{
			New:        "New",
			Pending:    "Pending",
			Scheduling: "Scheduling",
			Allocated:  "Allocated",
			Rejected:   "Rejected",
			Bound:      "Bound",
			Killing:    "Killing",
			Killed:     "Killed",
			Failed:     "Failed",
			Completed:  "Completed",
			Any: []string{
				"New", "Pending", "Scheduling",
				"Allocated", "Rejected",
				"Bound", "Killing", "Killed",
				"Failed", "Completed",
			},
			Terminated: []string{
				"Rejected", "Killed", "Failed",
				"Completed",
			},
		}
	})
	return storeTaskStates
}

func newTaskState() *fsm.FSM {
	states := TaskStates()
	return fsm.NewFSM(
		states.New, fsm.Events{
			{
				Name: InitTask.String(),
				Src:  []string{states.New},
				Dst:  states.Pending,
			},
			{
				Name: SubmitTask.String(),
				Src:  []string{states.Pending},
				Dst:  states.Scheduling,
			},
			{
				Name: TaskAllocated.String(),
				Src:  []string{states.Scheduling},
				Dst:  states.Allocated,
			},
			{
				Name: TaskAllocated.String(),
				Src:  []string{states.Completed},
				Dst:  states.Completed,
			},
			{
				Name: TaskBound.String(),
				Src:  []string{states.Allocated},
				Dst:  states.Bound,
			},
			{
				Name: CompleteTask.String(),
				Src:  states.Any,
				Dst:  states.Completed,
			},
			{
				Name: KillTask.String(),
				Src:  []string{states.Pending, states.Scheduling, states.Allocated, states.Bound},
				Dst:  states.Killing,
			},
			{
				Name: TaskKilled.String(),
				Src:  []string{states.Killing},
				Dst:  states.Killed,
			},
			{
				Name: TaskRejected.String(),
				Src:  []string{states.New, states.Pending, states.Scheduling},
				Dst:  states.Rejected,
			},
			{
				Name: TaskFail.String(),
				Src:  []string{states.Rejected, states.Allocated},
				Dst:  states.Failed,
			},
		},
		fsm.Callbacks{
			events.EnterState: func(event *fsm.Event) {
				log.Logger().Info("object transition",
					zap.Any("object", event.Args[0]),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
			states.Pending: func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				task.postTaskPending()
			},
			states.Allocated: func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				eventArgs := make([]string, 2)
				if err := events.GetEventArgsAsStrings(eventArgs, event.Args[1].([]interface{})); err != nil {
					task.handleFailEvent(err.Error(), false)
					return
				}
				allocUUID := eventArgs[0]
				nodeID := eventArgs[1]
				task.postTaskAllocated(allocUUID, nodeID)
			},
			states.Rejected: func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				task.postTaskRejected()
			},
			states.Failed: func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				task.postTaskFailed()
			},
			states.Bound: func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				task.postTaskBound()
			},
			beforeHook(TaskAllocated): func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				eventArgs := make([]string, 2)
				if err := events.GetEventArgsAsStrings(eventArgs, event.Args[1].([]interface{})); err != nil {
					task.handleFailEvent(err.Error(), false)
					return
				}
				allocUUID := eventArgs[0]
				nodeID := eventArgs[1]
				task.beforeTaskAllocated(event.Src, allocUUID, nodeID)
			},
			beforeHook(CompleteTask): func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				task.beforeTaskCompleted()
			},
			SubmitTask.String(): func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				task.handleSubmitTaskEvent()
			},
			TaskFail.String(): func(event *fsm.Event) {
				task := event.Args[0].(*Task) //nolint:errcheck
				eventArgs := make([]string, 1)
				if err := events.GetEventArgsAsStrings(eventArgs, event.Args[1].([]interface{})); err != nil {
					task.handleFailEvent(err.Error(), true)
					return
				}
				reason := eventArgs[0]
				task.handleFailEvent(reason, false)
			},
		},
	)
}
