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

import "github.com/cloudera/yunikorn-k8shim/pkg/common/events"

// ------------------------
// Simple task Event simply moves task to next state, it has no arguments provided
// ------------------------
type SimpleTaskEvent struct {
	applicationId string
	taskId        string
	event         events.TaskEventType
}

func NewSimpleTaskEvent(appId string, taskId string, taskType events.TaskEventType) SimpleTaskEvent {
	return SimpleTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         taskType,
	}
}

func (st SimpleTaskEvent) GetEvent() events.TaskEventType {
	return st.event
}

func (st SimpleTaskEvent) GetArgs() []interface{} {
	return nil
}

func (st SimpleTaskEvent) GetTaskId() string {
	return st.taskId
}

func (st SimpleTaskEvent) GetApplicationId() string {
	return st.applicationId
}

// ------------------------
// SubmitTask Event
// ------------------------
type SubmitTaskEvent struct {
	applicationId string
	taskId        string
	event         events.TaskEventType
	message       string
}

func NewSubmitTaskEvent(appId string, taskId string) SubmitTaskEvent {
	return SubmitTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         events.SubmitTask,
	}
}

func (st SubmitTaskEvent) GetEvent() events.TaskEventType {
	return st.event
}

func (st SubmitTaskEvent) GetArgs() []interface{} {
	return nil
}

func (st SubmitTaskEvent) GetTaskId() string {
	return st.taskId
}

func (st SubmitTaskEvent) GetApplicationId() string {
	return st.applicationId
}

// ------------------------
// Allocate Event
// ------------------------
type AllocatedTaskEvent struct {
	applicationId  string
	taskId         string
	event          events.TaskEventType
	nodeId         string
	allocationUuid string
}

func NewAllocateTaskEvent(appId string, taskId string, allocUuid string, nid string) AllocatedTaskEvent {
	return AllocatedTaskEvent{
		applicationId:  appId,
		taskId:         taskId,
		event:          events.TaskAllocated,
		allocationUuid: allocUuid,
		nodeId:         nid,
	}
}

func (ae AllocatedTaskEvent) GetEvent() events.TaskEventType {
	return ae.event
}

func (ae AllocatedTaskEvent) GetArgs() []interface{} {
	args := make([]interface{}, 2)
	args[0] = ae.allocationUuid
	args[1] = ae.nodeId
	return args
}

func (ae AllocatedTaskEvent) GetTaskId() string {
	return ae.taskId
}

func (ae AllocatedTaskEvent) GetApplicationId() string {
	return ae.applicationId
}

// ------------------------
// Bound Event
// ------------------------
type BindTaskEvent struct {
	applicationId string
	taskId        string
	event         events.TaskEventType
}

func NewBindTaskEvent(appId string, taskId string) BindTaskEvent {
	return BindTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         events.TaskBound,
	}
}

func (bt BindTaskEvent) GetEvent() events.TaskEventType {
	return bt.event
}

func (bt BindTaskEvent) GetArgs() []interface{} {
	return nil
}

func (bt BindTaskEvent) GetTaskId() string {
	return bt.taskId
}

func (bt BindTaskEvent) GetApplicationId() string {
	return bt.applicationId
}

// ------------------------
// Fail Event
// ------------------------
type FailTaskEvent struct {
	applicationId string
	taskId        string
	event         events.TaskEventType
	message       string
}

func NewFailTaskEvent(appId string, taskId string, failedMessage string) FailTaskEvent {
	return FailTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         events.TaskFail,
		message:       failedMessage,
	}
}

func (fe FailTaskEvent) GetEvent() events.TaskEventType {
	return fe.event
}

func (fe FailTaskEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = fe.message
	return args
}

func (fe FailTaskEvent) GetTaskId() string {
	return fe.taskId
}

func (fe FailTaskEvent) GetApplicationId() string {
	return fe.applicationId
}

// ------------------------
// Reject Event
// ------------------------
type RejectTaskEvent struct {
	applicationId string
	taskId        string
	event         events.TaskEventType
	message       string
}

func NewRejectTaskEvent(appId string, taskId string, rejectedMessage string) RejectTaskEvent {
	return RejectTaskEvent{
		applicationId: appId,
		taskId:        taskId,
		event:         events.TaskRejected,
		message:       rejectedMessage,
	}
}

func (re RejectTaskEvent) GetEvent() events.TaskEventType {
	return re.event
}

func (re RejectTaskEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = re.message
	return args
}

func (re RejectTaskEvent) GetTaskId() string {
	return re.taskId
}

func (re RejectTaskEvent) GetApplicationId() string {
	return re.applicationId
}
