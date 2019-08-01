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

package main

// --------------------------------------
// scheduler events
// --------------------------------------
type SchedulerEventType string

const (
	RegisterScheduler SchedulerEventType = "RegisterScheduler"
)

type SchedulerEvent interface {
	// the type of this event
	GetEvent() SchedulerEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	GetArgs() []interface{}
}

type RegisterSchedulerEvent struct {
	event SchedulerEventType
}

func newRegisterSchedulerEvent() RegisterSchedulerEvent {
	return RegisterSchedulerEvent{
		event: RegisterScheduler,
	}
}

func (rs RegisterSchedulerEvent) GetEvent() SchedulerEventType {
	return rs.event
}

func (rs RegisterSchedulerEvent) GetArgs() []interface{} {
	return nil
}