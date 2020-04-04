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

package events

const EnterState = "enter_state"

//----------------------------------------------
// General event interface
//----------------------------------------------
type SchedulingEvent interface {
	GetArgs() []interface{}
}

//----------------------------------------------
// Application events
//----------------------------------------------
type ApplicationEventType string

const (
	SubmitApplication   ApplicationEventType = "SubmitApplication"
	RecoverApplication  ApplicationEventType = "RecoverApplication"
	AcceptApplication   ApplicationEventType = "AcceptApplication"
	RunApplication      ApplicationEventType = "RunApplication"
	RejectApplication   ApplicationEventType = "RejectApplication"
	CompleteApplication ApplicationEventType = "CompleteApplication"
	FailApplication     ApplicationEventType = "FailApplication"
	KillApplication     ApplicationEventType = "KillApplication"
	KilledApplication   ApplicationEventType = "KilledApplication"
)

type ApplicationEvent interface {
	// an application event is associated with an application Id,
	// dispatcher finds out actual application based on this id
	// to handle this event
	GetApplicationID() string

	// the type of this event
	GetEvent() ApplicationEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

//----------------------------------------------
// Task events
//----------------------------------------------
type TaskEventType string

const (
	InitTask      TaskEventType = "InitTask"
	SubmitTask    TaskEventType = "SubmitTask"
	TaskAllocated TaskEventType = "TaskAllocated"
	TaskRejected  TaskEventType = "TaskRejected"
	TaskBound     TaskEventType = "TaskBound"
	CompleteTask  TaskEventType = "CompleteTask"
	TaskFail      TaskEventType = "TaskFail"
	KillTask      TaskEventType = "KillTask"
	TaskKilled    TaskEventType = "TaskKilled"
)

type TaskEvent interface {
	// application ID which this task belongs to
	GetApplicationID() string

	// a task event must be associated with an application ID
	// and a task ID, dispatcher need them to dispatch this event
	// to the actual task
	GetTaskID() string

	// type of this event
	GetEvent() TaskEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

// --------------------------------------
// scheduler events
// --------------------------------------
type SchedulerEventType string

const (
	RegisterScheduler        SchedulerEventType = "RegisterScheduler"
	RegisterSchedulerSucceed SchedulerEventType = "RegisterSchedulerSucceed"
	RegisterSchedulerFailed  SchedulerEventType = "RegisterSchedulerFailed"
	RecoverScheduler         SchedulerEventType = "RecoverScheduler"
	RecoverSchedulerSucceed  SchedulerEventType = "RecoverSchedulerSucceed"
	RecoverSchedulerFailed   SchedulerEventType = "RecoverSchedulerFailed"
)

type SchedulerEvent interface {
	// the type of this event
	GetEvent() SchedulerEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

// --------------------------------------
// scheduler node events
// --------------------------------------
type SchedulerNodeEventType string

const (
	RecoverNode  SchedulerNodeEventType = "RecoverNode"
	NodeAccepted SchedulerNodeEventType = "NodeAccepted"
	NodeRejected SchedulerNodeEventType = "NodeRejected"
	DrainNode    SchedulerNodeEventType = "DrainNode"
	RestoreNode  SchedulerNodeEventType = "RestoreNode"
	NodeReady    SchedulerNodeEventType = "NodeReady"
)

type SchedulerNodeEvent interface {
	// returns the node ID
	GetNodeID() string

	// the type of this event
	GetEvent() SchedulerNodeEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}
