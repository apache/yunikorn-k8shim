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

package events

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
	GetApplicationId() string

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
	// application id which this task belongs to
	GetApplicationId() string

	// a task event must be associated with an application id
	// and a task id, dispatcher need them to dispatch this event
	// to the actual task
	GetTaskId() string

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
	RecoverNode        SchedulerNodeEventType = "RecoverNode"
	NodeAccepted       SchedulerNodeEventType = "NodeAccepted"
	NodeRejected       SchedulerNodeEventType = "NodeRejected"
)

type SchedulerNodeEvent interface {
	// returns the node Id
	GetNodeId() string

	// the type of this event
	GetEvent() SchedulerNodeEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}
