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

package events

var (
	states *AllStates
)

type AllStates struct {
	Application *ApplicationStates
	Task        *TaskStates
	Scheduler   *SchedulerStates
	Node        *NodeStates
}

type SchedulerStates struct {
	New         string
	Registered  string
	Registering string
	Recovering  string
	Running     string
	Draining    string
	Stopped     string
}

type ApplicationStates struct {
	New        string
	Recovering string
	Submitted  string
	Accepted   string
	Running    string
	Rejected   string
	Completed  string
	Killing    string
	Killed     string
	Failed     string
}

type NodeStates struct {
	New        string
	Recovering string
	Healthy    string
	Unhealthy  string
	Rejected   string
}

type TaskStates struct {
	Pending    string
	Scheduling string
	Allocated  string
	Rejected   string
	Bound      string
	Killing    string
	Killed     string
	Failed     string
	Completed  string
}

func States() *AllStates {
	if states == nil {
		states = &AllStates{
			Application: &ApplicationStates{
				New:        "New",
				Recovering: "Recovering",
				Submitted:  "Submitted",
				Accepted:   "Accepted",
				Running:    "Running",
				Rejected:   "Rejected",
				Completed:  "Completed",
				Killing:    "Killing",
				Killed:     "Killed",
				Failed:     "Failed",
			},
			Task: &TaskStates{
				Pending:    "Pending",
				Scheduling: "Scheduling",
				Allocated:  "TaskAllocated",
				Rejected:   "Rejected",
				Bound:      "Bound",
				Killing:    "Killing",
				Killed:     "Killed",
				Failed:     "Failed",
				Completed:  "Completed",
			},
			Scheduler: &SchedulerStates{
				New:         "New",
				Registered:  "Registered",
				Registering: "Registering",
				Recovering:  "Recovering",
				Running:     "Running",
				Draining:    "Draining",
				Stopped:     "Stopped",
			},
			Node: &NodeStates{
				New:        "New",
				Recovering: "Recovering",
				Healthy:    "Healthy",
				Unhealthy:  "Unhealthy",
				Rejected:   "Rejected",
			},
		}
	}
	return states
}
