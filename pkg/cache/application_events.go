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
	applicationID string
	event         events.ApplicationEventType
}

func NewSimpleApplicationEvent(appID string, eventType events.ApplicationEventType) SimpleApplicationEvent {
	return SimpleApplicationEvent{
		applicationID: appID,
		event:         eventType,
	}
}

func (st SimpleApplicationEvent) GetEvent() events.ApplicationEventType {
	return st.event
}

func (st SimpleApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (st SimpleApplicationEvent) GetApplicationID() string {
	return st.applicationID
}

// ------------------------
// SubmitTask application
// ------------------------
type SubmitApplicationEvent struct {
	applicationID string
	event         events.ApplicationEventType
}

func NewSubmitApplicationEvent(appID string) SubmitApplicationEvent {
	return SubmitApplicationEvent{
		applicationID: appID,
		event:         events.SubmitApplication,
	}
}

func (se SubmitApplicationEvent) GetEvent() events.ApplicationEventType {
	return se.event
}

func (se SubmitApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (se SubmitApplicationEvent) GetApplicationID() string {
	return se.applicationID
}

// ------------------------
// Run application
// ------------------------
type RunApplicationEvent struct {
	applicationID string
	event         events.ApplicationEventType
}

func NewRunApplicationEvent(appID string) RunApplicationEvent {
	return RunApplicationEvent{
		applicationID: appID,
		event:         events.RunApplication,
	}
}

func (re RunApplicationEvent) GetEvent() events.ApplicationEventType {
	return re.event
}

func (re RunApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (re RunApplicationEvent) GetApplicationID() string {
	return re.applicationID
}

// ------------------------
// Fail application
// ------------------------
type FailApplicationEvent struct {
	applicationID string
	event         events.ApplicationEventType
}

func NewFailApplicationEvent(appID string) FailApplicationEvent {
	return FailApplicationEvent{
		applicationID: appID,
		event:         events.FailApplication,
	}
}

func (fe FailApplicationEvent) GetEvent() events.ApplicationEventType {
	return fe.event
}

func (fe FailApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (fe FailApplicationEvent) GetApplicationID() string {
	return fe.applicationID
}
