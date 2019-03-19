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

type TaskStates struct {
	PENDING TaskState
	SCHEDULING TaskState
	ALLOCATED TaskState
	REJECTED TaskState
	BOUND TaskState
	KILLING TaskState
	KILLED TaskState
	FAILED TaskState
	COMPLETED TaskState
}

type TaskState struct {
	state string
	Value func() string
}

func newTaskState(str string) TaskState{
	return TaskState{
		state: str,
		Value: func() string { return str }}
}

func InitiateTaskStates() *TaskStates {
	return &TaskStates {
		PENDING: newTaskState("PENDING"),
		SCHEDULING: newTaskState("SCHEDULING"),
		ALLOCATED: newTaskState("ALLOCATED"),
		REJECTED: newTaskState("REJECTED"),
		BOUND: newTaskState("BOUND"),
		KILLING: newTaskState("KILLING"),
		KILLED: newTaskState("KILLED"),
		FAILED: newTaskState("FAILED"),
		COMPLETED: newTaskState("COMPLETED"),
	}
}