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

package state

type ApplicationEventType string

const (
	SubmitApplication   ApplicationEventType = "SubmitApplication"
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
	getApplicationId() string

	// the type of this event
	getEvent() ApplicationEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	getArgs() []interface{}
}

// ------------------------
// SimpleApplicationEvent simples moves application states
// ------------------------
type SimpleApplicationEvent struct {
	applicationId string
	event         ApplicationEventType
}

func NewSimpleApplicationEvent(appId string, eventType ApplicationEventType) SimpleApplicationEvent {
	return SimpleApplicationEvent{
		applicationId: appId,
		event:         eventType,
	}
}

func (st SimpleApplicationEvent) getEvent() ApplicationEventType {
	return st.event
}

func (st SimpleApplicationEvent) getArgs() []interface{} {
	return nil
}

func (st SimpleApplicationEvent) getApplicationId() string {
	return st.applicationId
}

// ------------------------
// Submit application
// ------------------------
type SubmitApplicationEvent struct {
	applicationId string
	event         ApplicationEventType
}

func NewSubmitApplicationEvent(appId string) SubmitApplicationEvent {
	return SubmitApplicationEvent{
		applicationId: appId,
		event:         SubmitApplication,
	}
}

func (se SubmitApplicationEvent) getEvent() ApplicationEventType {
	return se.event
}

func (se SubmitApplicationEvent) getArgs() []interface{} {
	return nil
}

func (se SubmitApplicationEvent) getApplicationId() string {
	return se.applicationId
}

// ------------------------
// Run application
// ------------------------
type RunApplicationEvent struct {
	applicationId string
	event         ApplicationEventType
	task          *Task
}

func NewRunApplicationEvent(appId string, task *Task) RunApplicationEvent {
	return RunApplicationEvent{
		applicationId: appId,
		event:         RunApplication,
		task:          task,
	}
}

func (re RunApplicationEvent) getEvent() ApplicationEventType {
	return re.event
}

func (re RunApplicationEvent) getArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = re.task
	return args
}

func (re RunApplicationEvent) getApplicationId() string {
	return re.applicationId
}

// ------------------------
// Fail application
// ------------------------
type FailApplicationEvent struct {
	applicationId string
	event         ApplicationEventType
}

func NewFailApplicationEvent(appId string) FailApplicationEvent {
	return FailApplicationEvent{
		applicationId: appId,
		event:         FailApplication,
	}
}

func (fe FailApplicationEvent) getEvent() ApplicationEventType {
	return fe.event
}

func (fe FailApplicationEvent) getArgs() []interface{} {
	return nil
}

func (fe FailApplicationEvent) getApplicationId() string {
	return fe.applicationId
}
