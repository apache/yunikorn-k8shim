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

type ApplicationEventType string

const (
	SubmitApplication   ApplicationEventType = "SubmitApplication"
	AcceptApplication   ApplicationEventType = "AcceptApplication"
	RunApplication      ApplicationEventType = "RunApplication"
	RejectApplication   ApplicationEventType = "RejectApplication"
	CompleteApplication ApplicationEventType = "CompleteApplication"
	FailApplication     ApplicationEventType = "FailApplication"
	KillApplication     ApplicationEventType = "KillApplication"
	KilledApplication   ApplicationEventType = "KilledApplication"
)

type ApplicationEvent interface {
	getEvent() ApplicationEventType
	getArgs() []interface{}
}

// ------------------------
// SimpleApplicationEvent simples moves application states
// ------------------------
type SimpleApplicationEvent struct {
	event ApplicationEventType
}

func NewSimpleApplicationEvent(eventType ApplicationEventType) SimpleApplicationEvent {
	return SimpleApplicationEvent{
		event: eventType,
	}
}

func (st SimpleApplicationEvent) getEvent() ApplicationEventType {
	return st.event
}

func (st SimpleApplicationEvent) getArgs() []interface{} {
	return nil
}