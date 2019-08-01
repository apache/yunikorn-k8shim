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
// SimpleApplicationEvent simples moves application states
// ------------------------
type SimpleApplicationEvent struct {
	applicationId string
	event         events.ApplicationEventType
}

func NewSimpleApplicationEvent(appId string, eventType events.ApplicationEventType) SimpleApplicationEvent {
	return SimpleApplicationEvent{
		applicationId: appId,
		event:         eventType,
	}
}

func (st SimpleApplicationEvent) GetEvent() events.ApplicationEventType {
	return st.event
}

func (st SimpleApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (st SimpleApplicationEvent) GetApplicationId() string {
	return st.applicationId
}

// ------------------------
// SubmitTask application
// ------------------------
type SubmitApplicationEvent struct {
	applicationId string
	event         events.ApplicationEventType
}

func NewSubmitApplicationEvent(appId string) SubmitApplicationEvent {
	return SubmitApplicationEvent{
		applicationId: appId,
		event:         events.SubmitApplication,
	}
}

func (se SubmitApplicationEvent) GetEvent() events.ApplicationEventType {
	return se.event
}

func (se SubmitApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (se SubmitApplicationEvent) GetApplicationId() string {
	return se.applicationId
}

// ------------------------
// Run application
// ------------------------
type RunApplicationEvent struct {
	applicationId string
	event         events.ApplicationEventType
	task          *Task
}

func NewRunApplicationEvent(appId string, task *Task) RunApplicationEvent {
	return RunApplicationEvent{
		applicationId: appId,
		event:         events.RunApplication,
		task:          task,
	}
}

func (re RunApplicationEvent) GetEvent() events.ApplicationEventType {
	return re.event
}

func (re RunApplicationEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = re.task
	return args
}

func (re RunApplicationEvent) GetApplicationId() string {
	return re.applicationId
}

// ------------------------
// Fail application
// ------------------------
type FailApplicationEvent struct {
	applicationId string
	event         events.ApplicationEventType
}

func NewFailApplicationEvent(appId string) FailApplicationEvent {
	return FailApplicationEvent{
		applicationId: appId,
		event:         events.FailApplication,
	}
}

func (fe FailApplicationEvent) GetEvent() events.ApplicationEventType {
	return fe.event
}

func (fe FailApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (fe FailApplicationEvent) GetApplicationId() string {
	return fe.applicationId
}
