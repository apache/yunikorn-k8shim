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

type TaskEventType string

const (
	Submit    TaskEventType = "Submit"
	Allocated TaskEventType = "Allocated"
	Rejected  TaskEventType = "Rejected"
	Bound     TaskEventType = "Bound"
	Complete  TaskEventType = "Complete"
	Fail      TaskEventType = "Fail"
	Kill      TaskEventType = "Kill"
	Killed    TaskEventType = "Killed"
)

type TaskEvent interface {
	// application id which this task belongs to
	getApplicationId() string

	// a task event must be associated with an application id
	// and a task id, dispatcher need them to dispatch this event
	// to the actual task
	getTaskId() string

	// type of this event
	getEvent() TaskEventType

	// an event can have multiple arguments, these arguments will be passed to
	// state machines' callbacks when doing state transition
	getArgs() []interface{}
}

// ------------------------
// Simple task Event simply moves task to next state, it has no arguments provided
// ------------------------
type SimpleTaskEvent struct {
	applicationId string
	taskId        string
	event         TaskEventType
}

func NewSimpleTaskEvent(appId string, taskId string, taskType TaskEventType) SimpleTaskEvent {
	return SimpleTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         taskType,
	}
}

func (st SimpleTaskEvent) getEvent() TaskEventType {
	return st.event
}

func (st SimpleTaskEvent) getArgs() []interface{} {
	return nil
}

func (st SimpleTaskEvent) getTaskId() string {
	return st.taskId
}

func (st SimpleTaskEvent) getApplicationId() string {
	return st.applicationId
}

// ------------------------
// Submit Event
// ------------------------
type SubmitTaskEvent struct {
	applicationId string
	taskId        string
	event         TaskEventType
	message       string
}

func NewSubmitTaskEvent(appId string, taskId string) SubmitTaskEvent {
	return SubmitTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         Submit,
	}
}

func (st SubmitTaskEvent) getEvent() TaskEventType {
	return st.event
}

func (st SubmitTaskEvent) getArgs() []interface{} {
	return nil
}

func (st SubmitTaskEvent) getTaskId() string {
	return st.taskId
}

func (st SubmitTaskEvent) getApplicationId() string {
	return st.applicationId
}

// ------------------------
// Allocate Event
// ------------------------
type AllocatedTaskEvent struct {
	applicationId  string
	taskId         string
	event          TaskEventType
	nodeId         string
	allocationUuid string
}

func NewAllocateTaskEvent(appId string, taskId string, allocUuid string, nid string) AllocatedTaskEvent {
	return AllocatedTaskEvent{
		applicationId:  appId,
		taskId:         taskId,
		event:          Allocated,
		allocationUuid: allocUuid,
		nodeId:         nid,
	}
}

func (ae AllocatedTaskEvent) getEvent() TaskEventType {
	return ae.event
}

func (ae AllocatedTaskEvent) getArgs() []interface{} {
	args := make([]interface{}, 2)
	args[0] = ae.allocationUuid
	args[1] = ae.nodeId
	return args
}

func (st AllocatedTaskEvent) getTaskId() string {
	return st.taskId
}

func (st AllocatedTaskEvent) getApplicationId() string {
	return st.applicationId
}

// ------------------------
// Bound Event
// ------------------------
type BindTaskEvent struct {
	applicationId string
	taskId        string
	event         TaskEventType
}

func NewBindTaskEvent(appId string, taskId string) BindTaskEvent {
	return BindTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         Bound,
	}
}

func (bt BindTaskEvent) getEvent() TaskEventType {
	return bt.event
}

func (bt BindTaskEvent) getArgs() []interface{} {
	return nil
}

func (st BindTaskEvent) getTaskId() string {
	return st.taskId
}

func (st BindTaskEvent) getApplicationId() string {
	return st.applicationId
}

// ------------------------
// Fail Event
// ------------------------
type FailTaskEvent struct {
	applicationId string
	taskId        string
	event         TaskEventType
	message       string
}

func NewFailTaskEvent(appId string, taskId string, failedMessage string) FailTaskEvent {
	return FailTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         Fail,
		message:       failedMessage,
	}
}

func (ae FailTaskEvent) getEvent() TaskEventType {
	return ae.event
}

func (ae FailTaskEvent) getArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = ae.message
	return args
}

func (st FailTaskEvent) getTaskId() string {
	return st.taskId
}

func (st FailTaskEvent) getApplicationId() string {
	return st.applicationId
}

// ------------------------
// Fail Event
// ------------------------
type RejectTaskEvent struct {
	applicationId string
	taskId        string
	event         TaskEventType
	message       string
}

func NewRejectTaskEvent(appId string, taskId string, rejectedMessage string) RejectTaskEvent {
	return RejectTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         Rejected,
		message:       rejectedMessage,
	}
}

func (re RejectTaskEvent) getEvent() TaskEventType {
	return re.event
}

func (re RejectTaskEvent) getArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = re.message
	return args
}

func (st RejectTaskEvent) getTaskId() string {
	return st.taskId
}

func (st RejectTaskEvent) getApplicationId() string {
	return st.applicationId
}
