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
	"context"
	"sync"

	"github.com/looplab/fsm"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var applicationStatesOnce sync.Once

// ----------------------------------------------
// Application events
// ----------------------------------------------
type ApplicationEventType int

const (
	SubmitApplication ApplicationEventType = iota
	AcceptApplication
	TryReserve
	UpdateReservation
	RunApplication
	RejectApplication
	CompleteApplication
	FailApplication
	KillApplication
	KilledApplication
	ReleaseAppAllocation
	ResumingApplication
	AppTaskCompleted
)

func (ae ApplicationEventType) String() string {
	return [...]string{"SubmitApplication", "AcceptApplication", "TryReserve", "UpdateReservation", "RunApplication", "RejectApplication", "CompleteApplication", "FailApplication", "KillApplication", "KilledApplication", "ReleaseAppAllocation", "ResumingApplication", "AppTaskCompleted"}[ae]
}

// ------------------------
// SimpleApplicationEvent simply moves application states
// ------------------------
type SimpleApplicationEvent struct {
	applicationID string
	event         ApplicationEventType
}

func NewSimpleApplicationEvent(appID string, eventType ApplicationEventType) SimpleApplicationEvent {
	return SimpleApplicationEvent{
		applicationID: appID,
		event:         eventType,
	}
}

func (st SimpleApplicationEvent) GetEvent() string {
	return st.event.String()
}

func (st SimpleApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (st SimpleApplicationEvent) GetApplicationID() string {
	return st.applicationID
}

// ------------------------
// ApplicationEvent is used for testing and rejected app's events
// ------------------------
type ApplicationEvent struct {
	applicationID string
	event         ApplicationEventType
	message       string
}

func NewApplicationEvent(appID string, eventType ApplicationEventType, msg string) ApplicationEvent {
	return ApplicationEvent{
		applicationID: appID,
		event:         eventType,
		message:       msg,
	}
}

func (st ApplicationEvent) GetEvent() string {
	return st.event.String()
}

func (st ApplicationEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = st.message
	return args
}

func (st ApplicationEvent) GetApplicationID() string {
	return st.applicationID
}

// ------------------------
// ApplicationStatusChangeEvent updates the status in the application CRD
// ------------------------
type ApplicationStatusChangeEvent struct {
	applicationID string
	event         ApplicationEventType
	state         string
}

func NewApplicationStatusChangeEvent(appID string, eventType ApplicationEventType, state string) ApplicationStatusChangeEvent {
	return ApplicationStatusChangeEvent{
		applicationID: appID,
		event:         eventType,
		state:         state,
	}
}

func (st ApplicationStatusChangeEvent) GetEvent() string {
	return st.event.String()
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
	event         ApplicationEventType
}

func NewSubmitApplicationEvent(appID string) SubmitApplicationEvent {
	return SubmitApplicationEvent{
		applicationID: appID,
		event:         SubmitApplication,
	}
}

func (se SubmitApplicationEvent) GetEvent() string {
	return se.event.String()
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
	event         ApplicationEventType
}

func NewRunApplicationEvent(appID string) RunApplicationEvent {
	return RunApplicationEvent{
		applicationID: appID,
		event:         RunApplication,
	}
}

func (re RunApplicationEvent) GetEvent() string {
	return re.event.String()
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
	event         ApplicationEventType
	errorMessage  string
}

func NewFailApplicationEvent(appID, message string) FailApplicationEvent {
	return FailApplicationEvent{
		applicationID: appID,
		event:         FailApplication,
		errorMessage:  message,
	}
}

func (fe FailApplicationEvent) GetEvent() string {
	return fe.event.String()
}

func (fe FailApplicationEvent) GetArgs() []interface{} {
	args := make([]interface{}, 1)
	args[0] = fe.errorMessage
	return args
}

func (fe FailApplicationEvent) GetApplicationID() string {
	return fe.applicationID
}

// ------------------------
// Reservation Update Event
// ------------------------
type UpdateApplicationReservationEvent struct {
	applicationID string
	event         ApplicationEventType
}

func NewUpdateApplicationReservationEvent(appID string) UpdateApplicationReservationEvent {
	return UpdateApplicationReservationEvent{
		applicationID: appID,
		event:         UpdateReservation,
	}
}

func (ue UpdateApplicationReservationEvent) GetEvent() string {
	return ue.event.String()
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
	allocationKey   string
	terminationType string
	event           ApplicationEventType
}

func NewReleaseAppAllocationEvent(appID string, allocTermination si.TerminationType, allocationKey string) ReleaseAppAllocationEvent {
	return ReleaseAppAllocationEvent{
		applicationID:   appID,
		allocationKey:   allocationKey,
		terminationType: si.TerminationType_name[int32(allocTermination)],
		event:           ReleaseAppAllocation,
	}
}

func (re ReleaseAppAllocationEvent) GetApplicationID() string {
	return re.applicationID
}

func (re ReleaseAppAllocationEvent) GetArgs() []interface{} {
	args := make([]interface{}, 2)
	args[0] = re.allocationKey
	args[1] = re.terminationType
	return args
}

func (re ReleaseAppAllocationEvent) GetEvent() string {
	return re.event.String()
}

// ------------------------
// Resuming application
// ------------------------
type ResumingApplicationEvent struct {
	applicationID string
	event         ApplicationEventType
}

func NewResumingApplicationEvent(appID string) ResumingApplicationEvent {
	return ResumingApplicationEvent{
		applicationID: appID,
		event:         ResumingApplication,
	}
}

func (re ResumingApplicationEvent) GetEvent() string {
	return re.event.String()
}

func (re ResumingApplicationEvent) GetArgs() []interface{} {
	return nil
}

func (re ResumingApplicationEvent) GetApplicationID() string {
	return re.applicationID
}

// ----------------------------------
// Application states
// ----------------------------------
var storeApplicationStates *AStates

type AStates struct {
	New       string
	Submitted string
	Accepted  string
	Reserving string
	Running   string
	Rejected  string
	Completed string
	Killing   string
	Killed    string
	Failing   string
	Failed    string
	Resuming  string
}

func ApplicationStates() *AStates {
	applicationStatesOnce.Do(func() {
		storeApplicationStates = &AStates{
			New:       "New",
			Submitted: "Submitted",
			Accepted:  "Accepted",
			Reserving: "Reserving",
			Running:   "Running",
			Rejected:  "Rejected",
			Completed: "Completed",
			Killing:   "Killing",
			Killed:    "Killed",
			Failed:    "Failed",
			Failing:   "Failing",
			Resuming:  "Resuming",
		}
	})
	return storeApplicationStates
}

func newAppState() *fsm.FSM { //nolint:funlen
	states := ApplicationStates()
	return fsm.NewFSM(
		states.New, fsm.Events{
			{
				Name: SubmitApplication.String(),
				Src:  []string{states.New},
				Dst:  states.Submitted,
			},
			{
				Name: AcceptApplication.String(),
				Src:  []string{states.Submitted},
				Dst:  states.Accepted,
			},
			{
				Name: TryReserve.String(),
				Src:  []string{states.Accepted},
				Dst:  states.Reserving,
			},
			{
				Name: UpdateReservation.String(),
				Src:  []string{states.Reserving},
				Dst:  states.Reserving,
			},
			{
				Name: ResumingApplication.String(),
				Src:  []string{states.Reserving},
				Dst:  states.Resuming,
			},
			{
				Name: AppTaskCompleted.String(),
				Src:  []string{states.Resuming},
				Dst:  states.Resuming,
			},
			{
				Name: RunApplication.String(),
				Src:  []string{states.Accepted, states.Reserving, states.Resuming, states.Running},
				Dst:  states.Running,
			},
			{
				Name: ReleaseAppAllocation.String(),
				Src:  []string{states.Running, states.Accepted, states.Reserving},
				Dst:  states.Running,
			},
			{
				Name: ReleaseAppAllocation.String(),
				Src:  []string{states.Failing},
				Dst:  states.Failing,
			},
			{
				Name: ReleaseAppAllocation.String(),
				Src:  []string{states.Resuming},
				Dst:  states.Resuming,
			},
			{
				Name: CompleteApplication.String(),
				Src:  []string{states.Running},
				Dst:  states.Completed,
			},
			{
				Name: RejectApplication.String(),
				Src:  []string{states.Submitted},
				Dst:  states.Rejected,
			},
			{
				Name: FailApplication.String(),
				Src:  []string{states.Submitted, states.Accepted, states.Running, states.Reserving},
				Dst:  states.Failing,
			},
			{
				Name: FailApplication.String(),
				Src:  []string{states.Failing, states.Rejected},
				Dst:  states.Failed,
			},
			{
				Name: KillApplication.String(),
				Src:  []string{states.Accepted, states.Running, states.Reserving},
				Dst:  states.Killing,
			},
			{
				Name: KilledApplication.String(),
				Src:  []string{states.Killing},
				Dst:  states.Killed,
			},
		},
		fsm.Callbacks{
			events.EnterState: func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				log.Log(log.ShimFSM).Debug("shim app state transition",
					zap.String("app", app.applicationID),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
			states.Reserving: func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.onReserving()
			},
			states.Resuming: func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.onResuming()
			},
			SubmitApplication.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				event.Err = app.handleSubmitApplicationEvent()
			},
			RejectApplication.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				eventArgs := make([]string, 1)
				generic := event.Args[1].([]interface{}) //nolint:errcheck
				if err := events.GetEventArgsAsStrings(eventArgs, generic); err != nil {
					log.Log(log.ShimFSM).Error("fail to parse event arg", zap.Error(err))
					return
				}
				reason := eventArgs[0]
				app.handleRejectApplicationEvent(reason)
			},
			CompleteApplication.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.handleCompleteApplicationEvent()
			},
			FailApplication.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				eventArgs := make([]string, 1)
				generic := event.Args[1].([]interface{}) //nolint:errcheck
				if err := events.GetEventArgsAsStrings(eventArgs, generic); err != nil {
					log.Log(log.ShimFSM).Error("fail to parse event arg", zap.Error(err))
					return
				}
				errMsg := eventArgs[0]
				app.handleFailApplicationEvent(errMsg)
			},
			UpdateReservation.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.onReservationStateChange()
			},
			ReleaseAppAllocation.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				eventArgs := make([]string, 2)
				generic := event.Args[1].([]interface{}) //nolint:errcheck
				if err := events.GetEventArgsAsStrings(eventArgs, generic); err != nil {
					log.Log(log.ShimFSM).Error("fail to parse event arg", zap.Error(err))
					return
				}
				taskID := eventArgs[0]
				terminationType := eventArgs[1]
				app.handleReleaseAppAllocationEvent(taskID, terminationType)
			},
			AppTaskCompleted.String(): func(_ context.Context, event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.handleAppTaskCompletedEvent()
			},
		},
	)
}
