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

type JobEventType string

const (
	SubmitJob    JobEventType = "SubmitJob"
	AcceptJob    JobEventType = "AcceptJob"
	RunJob       JobEventType = "RunJob"
	RejectJob    JobEventType = "RejectJob"
	CompleteJob  JobEventType = "CompleteJob"
	FailJob      JobEventType = "FailJob"
	KillJob      JobEventType = "KillJob"
	KilledJob    JobEventType = "KilledJob"
)

type JobEvent interface {
	getEvent() JobEventType
	getArgs() interface{}
}

// ------------------------
// SimpleJobEvent simples moves job states
// ------------------------
type SimpleJobEvent struct {
	event JobEventType
}

func NewSimpleJobEvent(eventType JobEventType) SimpleJobEvent {
	return SimpleJobEvent{
		event: eventType,
	}
}

func (st SimpleJobEvent) getEvent() JobEventType {
	return st.event
}

func (st SimpleJobEvent) getArgs() interface{} {
	return nil
}