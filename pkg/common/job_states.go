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

type JobStates struct {
	NEW JobState
	SUBMITTED JobState
	ACCEPTED JobState
	RUNNING JobState
	REJECTED JobState
	COMPLETED JobState
	KILLING JobState
	KILLED JobState
	FAILED JobState
}

type JobState struct {
	state string
	Value func() string
}

func newJobState(str string) JobState{
	return JobState{
		state: str,
		Value: func() string { return str }}
}

func InitiateJobStates() *JobStates {
	return &JobStates {
		NEW: newJobState("NEW"),
		SUBMITTED: newJobState("SUBMITTED"),
		ACCEPTED: newJobState("ACCEPTED"),
		RUNNING: newJobState("RUNNING"),
		REJECTED: newJobState("REJECTED"),
		COMPLETED: newJobState("COMPLETED"),
		KILLING: newJobState("KILLING"),
		KILLED: newJobState("KILLED"),
		FAILED: newJobState("FAILED"),
	}
}