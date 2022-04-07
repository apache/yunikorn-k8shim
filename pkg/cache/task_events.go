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

import "github.com/apache/yunikorn-k8shim/pkg/common/events"

// ------------------------
// Simple task Event simply moves task to next state, it has no arguments provided
// ------------------------
type SimpleTaskEvent struct {
	applicationID string
	taskID        string
	event         events.TaskEventType
}

func NewSimpleTaskEvent(appID string, taskID string, taskType events.TaskEventType) SimpleTaskEvent {
	return SimpleTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         taskType,
	}
}

func (st SimpleTaskEvent) GetEvent() events.TaskEventType {
	return st.event
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
	event         events.TaskEventType
}

func NewSubmitTaskEvent(appID string, taskID string) SubmitTaskEvent {
	return SubmitTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         events.SubmitTask,
	}
}

func (st SubmitTaskEvent) GetEvent() events.TaskEventType {
	return st.event
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
	event          events.TaskEventType
	nodeID         string
	allocationUUID string
}

func NewAllocateTaskEvent(appID string, taskID string, allocUUID string, nid string) AllocatedTaskEvent {
	return AllocatedTaskEvent{
		applicationID:  appID,
		taskID:         taskID,
		event:          events.TaskAllocated,
		allocationUUID: allocUUID,
		nodeID:         nid,
	}
}

func (ae AllocatedTaskEvent) GetEvent() events.TaskEventType {
	return ae.event
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
	event         events.TaskEventType
}

func NewBindTaskEvent(appID string, taskID string) BindTaskEvent {
	return BindTaskEvent{
		applicationID: appID,
		taskID:        taskID,
		event:         events.TaskBound,
	}
}

func (bt BindTaskEvent) GetEvent() events.TaskEventType {
	return bt.event
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
	event         events.TaskEventType
	message       string
}

func NewFailTaskEvent(appID string, taskID string, failedMessage string) FailTaskEvent {
	return FailTaskEvent{
		applicationID: appID,
		taskID:        taskID,
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
	event         events.TaskEventType
	message       string
}

func NewRejectTaskEvent(appID string, taskID string, rejectedMessage string) RejectTaskEvent {
	return RejectTaskEvent{
		applicationID: appID,
		taskID:        taskID,
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

func (re RejectTaskEvent) GetTaskID() string {
	return re.taskID
}

func (re RejectTaskEvent) GetApplicationID() string {
	return re.applicationID
}
