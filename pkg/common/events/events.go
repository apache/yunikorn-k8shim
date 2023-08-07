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

// ----------------------------------------------
// General event interface
// ----------------------------------------------
type SchedulingEvent interface {
	GetArgs() []interface{}
}

// ----------------------------------------------
// Application events
// ----------------------------------------------
type ApplicationEvent interface {
	// an application event is associated with an application Id,
	// dispatcher finds out actual application based on this id
	// to handle this event
	GetApplicationID() string

	// the type of this event
	GetEvent() string

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

// ----------------------------------------------
// Task events
// ----------------------------------------------
type TaskEvent interface {
	// application ID which this task belongs to
	GetApplicationID() string

	// a task event must be associated with an application ID
	// and a task ID, dispatcher need them to dispatch this event
	// to the actual task
	GetTaskID() string

	// type of this event
	GetEvent() string

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

// --------------------------------------
// scheduler events
// --------------------------------------
type SchedulerEvent interface {
	// the type of this event
	GetEvent() string

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

// --------------------------------------
// scheduler node events
// --------------------------------------

type SchedulerNodeEvent interface {
	// returns the node ID
	GetNodeID() string

	// the type of this event
	GetEvent() string

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}
