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

import (
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

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
// ApplicationStatusChangeEvent updates the status in the application CRD
// ------------------------
type ApplicationStatusChangeEvent struct {
	applicationID string
	event         events.ApplicationEventType
	state         string
}

func NewApplicationStatusChangeEvent(appID string, eventType events.ApplicationEventType, state string) ApplicationStatusChangeEvent {
	return ApplicationStatusChangeEvent{
		applicationID: appID,
		event:         eventType,
		state:         state,
	}
}

func (st ApplicationStatusChangeEvent) GetEvent() events.ApplicationEventType {
	return st.event
}

func (st ApplicationStatusChangeEvent) GetArgs() []interface{} {
	return nil
}

func (st ApplicationStatusChangeEvent) GetApplicationID() string {
	return st.applicationID
}

func (st ApplicationStatusChangeEvent) GetState() string {
	return st.state
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

// ------------------------
// Reservation Update Event
// ------------------------
type UpdateApplicationReservationEvent struct {
	applicationID string
	event         events.ApplicationEventType
}

func NewUpdateApplicationReservationEvent(appID string) UpdateApplicationReservationEvent {
	return UpdateApplicationReservationEvent{
		applicationID: appID,
		event:         events.UpdateReservation,
	}
}

func (ue UpdateApplicationReservationEvent) GetEvent() events.ApplicationEventType {
	return ue.event
}

func (ue UpdateApplicationReservationEvent) GetArgs() []interface{} {
	return nil
}

func (ue UpdateApplicationReservationEvent) GetApplicationID() string {
	return ue.applicationID
}

// ------------------------
// Release application allocations
// ------------------------
type ReleaseAppAllocationEvent struct {
	applicationID   string
	allocationUUID  string
	terminationType string
	event           events.ApplicationEventType
}

func NewReleaseAppAllocationEvent(appID string, allocTermination si.AllocationRelease_TerminationType, uuid string) ReleaseAppAllocationEvent {
	return ReleaseAppAllocationEvent{
		applicationID:   appID,
		allocationUUID:  uuid,
		terminationType: si.AllocationRelease_TerminationType_name[int32(allocTermination)],
		event:           events.ReleaseAppAllocation,
	}
}

func (re ReleaseAppAllocationEvent) GetApplicationID() string {
	return re.applicationID
}

func (re ReleaseAppAllocationEvent) GetArgs() []interface{} {
	args := make([]interface{}, 2)
	args[0] = re.allocationUUID
	args[1] = re.terminationType
	return args
}

func (re ReleaseAppAllocationEvent) GetEvent() events.ApplicationEventType {
	return re.event
}
