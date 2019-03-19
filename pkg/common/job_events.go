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

type JobEvents struct {
	SUBMIT JobEvent
	ACCEPT JobEvent
	RUN JobEvent
	REJECT JobEvent
	COMPLETE JobEvent
	FAIL JobEvent
	KILL JobEvent
	KILLED JobEvent
}

type JobEvent struct {
	event string
}

func InitiateJobEvents() *JobEvents {
	return &JobEvents {
		SUBMIT: JobEvent{"SUBMIT"},
		ACCEPT: JobEvent{"ACCEPT"},
		RUN: JobEvent{"RUN"},
		REJECT: JobEvent{"REJECT"},
		COMPLETE: JobEvent{"COMPLETE"},
		FAIL: JobEvent{"FAIL"},
		KILL: JobEvent{"KILL"},
		KILLED: JobEvent{"KILLED"},
	}
}