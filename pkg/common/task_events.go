/*
Copyright 2019 The Unity Scheduler Authors

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

package common

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
	getEvent() TaskEventType
	getArgs() []interface{}
}

type TaskEvents struct {
	Submit TaskEvent
	Allocated TaskEvent
	Rejected TaskEvent
	Bind TaskEvent
	Complete TaskEvent
	Fail TaskEvent
	Kill TaskEvent
	Killed TaskEvent
}

// ------------------------
// Submit Event
// ------------------------
type SubmitTaskEvent struct {
	event TaskEventType
	message string
}

func NewSubmitTaskEvent() SubmitTaskEvent {
	return SubmitTaskEvent {
		event: Submit,
	}
}

func (st SubmitTaskEvent) getEvent() TaskEventType {
	return st.event
}

func (st SubmitTaskEvent) getArgs() []interface{} {
	return nil
}

// ------------------------
// Allocate Event
// ------------------------
type AllocatedTaskEvent struct {
	event TaskEventType
	nodeId string
}

func NewAllocateTaskEvent(nid string) AllocatedTaskEvent {
	return AllocatedTaskEvent{
		event: Allocated,
		nodeId:        nid,
	}
}

func (ae AllocatedTaskEvent) getEvent() TaskEventType {
	return ae.event
}

func (ae AllocatedTaskEvent) getArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = ae.nodeId
	return args
}

// ------------------------
// Bound Event
// ------------------------
type BindTaskEvent struct {
	event TaskEventType
}

func NewBindTaskEvent() BindTaskEvent {
	return BindTaskEvent {
		event: Bound,
	}
}

func (bt BindTaskEvent) getEvent() TaskEventType {
	return bt.event
}

func (bt BindTaskEvent) getArgs() []interface{} {
	return nil
}


// ------------------------
// Fail Event
// ------------------------
type FailTaskEvent struct {
	event TaskEventType
	message string
}

func NewFailTaskEvent(failedMessage string) FailTaskEvent {
	return FailTaskEvent {
		event: Fail,
		message: failedMessage,
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