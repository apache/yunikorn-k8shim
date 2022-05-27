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
	"testing"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	TestCreateName = "Create test"
	TestEventName  = "Event return test"
	TestArgsName   = "Default args return test"
	TestAppIDName  = "AppID return test"
	TestStateName  = "State return test"
)

func TestNewSimpleApplicationEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		event     ApplicationEventType
		wantID    string
		wantEvent ApplicationEventType
	}{
		{TestCreateName, "testAppId001", SubmitApplication, "testAppId001", SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewSimpleApplicationEvent(tt.appID, tt.event)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.event != tt.wantEvent {
				t.Errorf("want %s %s, got %s %s",
					tt.wantID, tt.wantEvent,
					instance.applicationID, instance.event)
			}
		})
	}
}

func TestSimpleApplicationEventGetEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		event     ApplicationEventType
		wantEvent ApplicationEventType
	}{
		{TestEventName, "testAppId001", SubmitApplication, SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewSimpleApplicationEvent(tt.appID, tt.event)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestSimpleApplicationEventGetArgs(t *testing.T) {
	tests := []struct {
		name  string
		appID string
		event ApplicationEventType
		want  int
	}{
		{TestArgsName, "testAppId001", SubmitApplication, 0},
	}

	for _, tt := range tests {
		instance := NewSimpleApplicationEvent(tt.appID, tt.event)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.want {
				t.Errorf("want %d, got %d", len(args), tt.want)
			}
		})
	}
}

func TestSimpleApplicationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name   string
		appID  string
		event  ApplicationEventType
		wantID string
	}{
		{TestAppIDName, "testAppId001", SubmitApplication, "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewSimpleApplicationEvent(tt.appID, tt.event)
		id := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if id != tt.wantID {
				t.Errorf("want app id: %s, got app id: %s", id, tt.wantID)
			}
		})
	}
}

func TestNewApplicationEvent(t *testing.T) {
	tests := []struct {
		name            string
		appID, msg      string
		event           ApplicationEventType
		wantID, wantMsg string
		wantEvent       ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testTask001", SubmitApplication, "testAppId001", "testTask001", SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewApplicationEvent(tt.appID, tt.event, tt.msg)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.event != tt.wantEvent || instance.message != tt.wantMsg {
				t.Errorf("want %s %s %s, got %s %s %s",
					tt.wantID, tt.wantEvent, tt.wantMsg,
					instance.applicationID, instance.event, instance.message)
			}
		})
	}
}

func TestApplicationEventGetEvent(t *testing.T) {
	tests := []struct {
		name       string
		appID, msg string
		event      ApplicationEventType
		want       ApplicationEventType
	}{
		{TestEventName, "testAppId001", "testTask001", SubmitApplication, SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewApplicationEvent(tt.appID, tt.event, tt.msg)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.want.String() {
				t.Errorf("want %s, got %s", tt.want, event)
			}
		})
	}
}

func TestApplicationEventGetArgs(t *testing.T) {
	tests := []struct {
		name       string
		appID, msg string
		event      ApplicationEventType
		wantLen    int
		isString   []bool
		wantArg    []string
	}{
		{TestArgsName, "testAppId001", "testTask001", SubmitApplication, 1, []bool{true}, []string{"testTask001"}},
	}

	for _, tt := range tests {
		instance := NewApplicationEvent(tt.appID, tt.event, tt.msg)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("len: want %d, got %d", tt.wantLen, len(args))
			}

			for index, arg := range args {
				info, ok := arg.(string)
				if ok != tt.isString[index] {
					t.Errorf("isString? want %v, got %v", tt.isString[index], ok)
				}

				if info != tt.wantArg[index] {
					t.Errorf("want %s, got %s", tt.wantArg[index], info)
				}
			}
		})
	}
}

func TestApplicationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name       string
		appID, msg string
		event      ApplicationEventType
		wantID     string
	}{
		{TestAppIDName, "testAppId001", "testTask001", SubmitApplication, "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewApplicationEvent(tt.appID, tt.event, tt.msg)
		id := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if id != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, id)
			}
		})
	}
}

func TestNewApplicationStatusChangeEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		event     ApplicationEventType
		state     string
		wantID    string
		wantEvent ApplicationEventType
		wantState string
	}{
		{TestCreateName, "testAppId001", SubmitApplication, "SubmitApplication", "testAppId001", SubmitApplication, "SubmitApplication"},
	}

	for _, tt := range tests {
		instance := NewApplicationStatusChangeEvent(tt.appID, tt.event, tt.state)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s %s %s, got %s %s %s",
					tt.wantID, tt.wantEvent, tt.wantState,
					instance.applicationID, instance.event, instance.state)
			}
		})
	}
}

func TestApplicationStatusChangeEventGetEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		event     ApplicationEventType
		state     string
		wantEvent ApplicationEventType
	}{
		{TestEventName, "testAppId001", SubmitApplication, "SubmitApplication", SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewApplicationStatusChangeEvent(tt.appID, tt.event, tt.state)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestApplicationStatusChangeEventGetArgs(t *testing.T) {
	tests := []struct {
		name    string
		appID   string
		event   ApplicationEventType
		state   string
		wantLen int
	}{
		{TestArgsName, "testAppId001", SubmitApplication, "SubmitApplication", 0},
	}

	for _, tt := range tests {
		instance := NewApplicationStatusChangeEvent(tt.appID, tt.event, tt.state)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))
			}
		})
	}
}

func TestApplicationStatusChangeEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		event     ApplicationEventType
		state     string
		wantAppID string
	}{
		{TestAppIDName, "testAppId001", SubmitApplication, "SubmitApplication", "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewApplicationStatusChangeEvent(tt.appID, tt.event, tt.state)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantAppID {
				t.Errorf("want %s, got %s", tt.wantAppID, appID)
			}
		})
	}
}

func TestApplicationStatusChangeEventGetState(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		event     ApplicationEventType
		state     string
		wantState string
	}{
		{TestStateName, "testAppId001", SubmitApplication, "SubmitApplication", "SubmitApplication"},
	}

	for _, tt := range tests {
		instance := NewApplicationStatusChangeEvent(tt.appID, tt.event, tt.state)
		state := instance.GetState()
		t.Run(tt.name, func(t *testing.T) {
			if state != tt.wantState {
				t.Errorf("want %s, got %s", tt.wantState, state)
			}
		})
	}
}

func TestNewSubmitApplicationEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantID    string
		wantEvent ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testAppId001", SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewSubmitApplicationEvent(tt.appID)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.event != tt.wantEvent {
				t.Errorf("want %s %s, got %s %s",
					tt.wantID, tt.wantEvent,
					instance.applicationID, instance.event)
			}
		})
	}
}

func TestSubmitApplicationEventGetEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantEvent ApplicationEventType
	}{
		{TestEventName, "testAppId001", SubmitApplication},
	}

	for _, tt := range tests {
		instance := NewSubmitApplicationEvent(tt.appID)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestSubmitApplicationEventGetArgs(t *testing.T) {
	tests := []struct {
		name    string
		appID   string
		wantLen int
	}{
		{TestArgsName, "testAppId001", 0},
	}

	for _, tt := range tests {
		instance := NewSubmitApplicationEvent(tt.appID)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))
			}
		})
	}
}

func TestSubmitApplicationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name   string
		appID  string
		wantID string
	}{
		{TestAppIDName, "testAppId001", "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewSubmitApplicationEvent(tt.appID)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, appID)
			}
		})
	}
}

func TestNewRunApplicationEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantID    string
		wantEvent ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testAppId001", RunApplication},
	}

	for _, tt := range tests {
		instance := NewRunApplicationEvent(tt.appID)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.event != tt.wantEvent {
				t.Errorf("want %s %s, got %s %s",
					tt.wantID, tt.wantEvent,
					instance.applicationID, instance.event)
			}
		})
	}
}

func TestRunApplicationEventGetEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantEvent ApplicationEventType
	}{
		{TestEventName, "testAppId001", RunApplication},
	}

	for _, tt := range tests {
		instance := NewRunApplicationEvent(tt.appID)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestRunApplicationEventGetArgs(t *testing.T) {
	tests := []struct {
		name    string
		appID   string
		wantLen int
	}{
		{TestArgsName, "testAppId001", 0},
	}

	for _, tt := range tests {
		instance := NewRunApplicationEvent(tt.appID)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))
			}
		})
	}
}

func TestRunApplicationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name   string
		appID  string
		wantID string
	}{
		{TestAppIDName, "testAppId001", "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewRunApplicationEvent(tt.appID)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, appID)
			}
		})
	}
}

func TestNewFailApplicationEvent(t *testing.T) {
	tests := []struct {
		name                 string
		appID, errorMsg      string
		wantID, wantErrorMsg string
		wantEvent            ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "test error msg", "testAppId001", "test error msg", FailApplication},
	}

	for _, tt := range tests {
		instance := NewFailApplicationEvent(tt.appID, tt.errorMsg)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.errorMessage != tt.wantErrorMsg || instance.event != tt.wantEvent {
				t.Errorf("want %s %s %s, got %s %s %s",
					tt.wantID, tt.wantErrorMsg, tt.wantEvent,
					instance.applicationID, instance.errorMessage, tt.wantEvent)
			}
		})
	}
}

func TestFailApplicationEventGetEvent(t *testing.T) {
	tests := []struct {
		name            string
		appID, errorMsg string
		wantEvent       ApplicationEventType
	}{
		{TestEventName, "testAppId001", "test error msg", FailApplication},
	}

	for _, tt := range tests {
		instance := NewFailApplicationEvent(tt.appID, tt.errorMsg)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if instance.event != tt.wantEvent {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestFailApplicationEventGetArgs(t *testing.T) {
	tests := []struct {
		name            string
		appID, errorMsg string
		wantLen         int
		castOk          []bool
		wantArg         []string
	}{
		{TestArgsName, "testAppId001", "test error msg", 1, []bool{true}, []string{"test error msg"}},
	}

	for _, tt := range tests {
		instance := NewFailApplicationEvent(tt.appID, tt.errorMsg)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))

				for index, arg := range args {
					info, ok := arg.(string)
					if ok != tt.castOk[index] {
						t.Errorf("want %v, got %v", tt.castOk[index], ok)
					}
					if info != tt.wantArg[index] {
						t.Errorf("want %s, got %s", tt.wantArg[index], info)
					}
				}
			}
		})
	}
}

func TestFailApplicationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name            string
		appID, errorMsg string
		wantID          string
	}{
		{TestAppIDName, "testAppId001", "test error msg", "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewFailApplicationEvent(tt.appID, tt.errorMsg)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, appID)
			}
		})
	}
}

func TestNewUpdateApplicationReservationEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantID    string
		wantEvent ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testAppId001", UpdateReservation},
	}

	for _, tt := range tests {
		instance := NewUpdateApplicationReservationEvent(tt.appID)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.event != tt.wantEvent {
				t.Errorf("want %s %s, got %s %s", tt.wantID, tt.wantEvent, tt.wantID, tt.wantEvent)
			}
		})
	}
}

func TestUpdateApplicationReservationEventGetEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantEvent ApplicationEventType
	}{
		{TestEventName, "testAppId001", UpdateReservation},
	}

	for _, tt := range tests {
		instance := NewUpdateApplicationReservationEvent(tt.appID)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", event, tt.wantEvent)
			}
		})
	}
}

func TestUpdateApplicationReservationEventGetArgs(t *testing.T) {
	tests := []struct {
		name    string
		appID   string
		wantLen int
	}{
		{TestArgsName, "testAppId001", 0},
	}

	for _, tt := range tests {
		instance := NewUpdateApplicationReservationEvent(tt.appID)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))
			}
		})
	}
}

func TestUpdateApplicationReservationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name   string
		appID  string
		wantID string
	}{
		{TestAppIDName, "testAppId001", "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewUpdateApplicationReservationEvent(tt.appID)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, appID)
			}
		})
	}
}

func TestNewReleaseAppAllocationEvent(t *testing.T) {
	tests := []struct {
		name                       string
		appID, allocationUUID      string
		terminationType            si.TerminationType
		wantID, wantUUID, wantType string
		wantEvent                  ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testUUID001", si.TerminationType_TIMEOUT, "testAppId001", "testUUID001", "TIMEOUT", ReleaseAppAllocation},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationEvent(tt.appID, tt.terminationType, tt.allocationUUID)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.allocationUUID != tt.wantUUID || instance.terminationType != tt.wantType || instance.event != tt.wantEvent {
				t.Errorf("want %s %s %s %s, got %s %s %s %s",
					tt.wantID, tt.wantUUID, tt.wantType, tt.wantEvent,
					instance.applicationID, instance.allocationUUID, instance.terminationType, instance.event)
			}
		})
	}
}

func TestReleaseAppAllocationEventGetEvent(t *testing.T) {
	tests := []struct {
		name                  string
		appID, allocationUUID string
		terminationType       si.TerminationType
		wantEvent             ApplicationEventType
	}{
		{TestEventName, "testAppId001", "testUUID001", si.TerminationType_TIMEOUT, ReleaseAppAllocation},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationEvent(tt.appID, tt.terminationType, tt.allocationUUID)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestReleaseAppAllocationEventGetArgs(t *testing.T) {
	tests := []struct {
		name                  string
		appID, allocationUUID string
		terminationType       si.TerminationType
		wantLen               int
		castOk                []bool
		wantArg               []string
	}{
		{TestArgsName, "testAppId001", "testUUID001", si.TerminationType_TIMEOUT, 2, []bool{true, true}, []string{"testUUID001", "TIMEOUT"}},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationEvent(tt.appID, tt.terminationType, tt.allocationUUID)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))

				for index, arg := range args {
					info, ok := arg.(string)
					if ok != tt.castOk[index] {
						t.Errorf("want %v, got %v", tt.castOk[index], ok)
					}
					if info != tt.wantArg[index] {
						t.Errorf("want %s, got %s", tt.wantArg[index], info)
					}
				}
			}
		})
	}
}

func TestReleaseAppAllocationEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name                  string
		appID, allocationUUID string
		terminationType       si.TerminationType
		wantID                string
	}{
		{TestAppIDName, "testAppId001", "testUUID001", si.TerminationType_TIMEOUT, "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationEvent(tt.appID, tt.terminationType, tt.allocationUUID)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, appID)
			}
		})
	}
}

func TestNewReleaseAppAllocationAskEvent(t *testing.T) {
	tests := []struct {
		name                         string
		appID, taskID                string
		terminationType              si.TerminationType
		wantID, wantTaskID, wantType string
		wantEvent                    ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testTaskId001", si.TerminationType_TIMEOUT, "testAppId001", "testTaskId001", "TIMEOUT", ReleaseAppAllocationAsk},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationAskEvent(tt.appID, tt.terminationType, tt.taskID)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.taskID != tt.taskID || instance.terminationType != tt.wantType || instance.event != tt.wantEvent {
				t.Errorf("want %s %s %s %s, got %s %s %s %s",
					tt.wantID, tt.taskID, tt.wantType, tt.wantEvent,
					instance.applicationID, instance.taskID, instance.terminationType, instance.event)
			}
		})
	}
}

func TestReleaseAppAllocationAskEventGetEvent(t *testing.T) {
	tests := []struct {
		name            string
		appID, taskID   string
		terminationType si.TerminationType
		wantEvent       ApplicationEventType
	}{
		{TestEventName, "testAppId001", "testTaskId001", si.TerminationType_TIMEOUT, ReleaseAppAllocationAsk},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationAskEvent(tt.appID, tt.terminationType, tt.taskID)
		event := instance.GetEvent()
		t.Run(tt.name, func(t *testing.T) {
			if event != tt.wantEvent.String() {
				t.Errorf("want %s, got %s", tt.wantEvent, event)
			}
		})
	}
}

func TestReleaseAppAllocationAskEventGetArgs(t *testing.T) {
	tests := []struct {
		name                 string
		appID, taskID        string
		terminationType      si.TerminationType
		wantLen              int
		wantTaskID, wantType string
		castOk               []bool
		wantArg              []string
	}{
		{TestArgsName, "testAppId001", "testTaskId001", si.TerminationType_TIMEOUT, 2, "testTaskId001", "TIMEOUT", []bool{true, true}, []string{"testTaskId001", "TIMEOUT"}},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationAskEvent(tt.appID, tt.terminationType, tt.taskID)
		args := instance.GetArgs()
		t.Run(tt.name, func(t *testing.T) {
			if len(args) != tt.wantLen {
				t.Errorf("want %d, got %d", tt.wantLen, len(args))

				for index, arg := range args {
					info, ok := arg.(string)
					if ok != tt.castOk[index] {
						t.Errorf("want %v, got %v", tt.castOk[index], ok)
					}
					if info != tt.wantArg[index] {
						t.Errorf("want %s, got %s", tt.wantArg[index], info)
					}
				}
			}
		})
	}
}

func TestReleaseAppAllocationAskEventGetApplicationID(t *testing.T) {
	tests := []struct {
		name            string
		appID, taskID   string
		terminationType si.TerminationType
		wantID          string
	}{
		{TestAppIDName, "testAppId001", "testTaskId001", si.TerminationType_TIMEOUT, "testAppId001"},
	}

	for _, tt := range tests {
		instance := NewReleaseAppAllocationAskEvent(tt.appID, tt.terminationType, tt.taskID)
		appID := instance.GetApplicationID()
		t.Run(tt.name, func(t *testing.T) {
			if appID != tt.wantID {
				t.Errorf("want %s, got %s", tt.wantID, appID)
			}
		})
	}
}

func TestNewResumingApplicationEvent(t *testing.T) {
	tests := []struct {
		name      string
		appID     string
		wantID    string
		wantEvent ApplicationEventType
	}{
		{TestCreateName, "testAppId001", "testAppId001", ResumingApplication},
	}

	for _, tt := range tests {
		instance := NewResumingApplicationEvent(tt.appID)
		t.Run(tt.name, func(t *testing.T) {
			if instance.applicationID != tt.wantID || instance.event != tt.wantEvent {
				t.Errorf("want %s %s, got %s %s",
					tt.wantID, tt.wantEvent,
					instance.applicationID, instance.event)
			}
		})
	}
}
