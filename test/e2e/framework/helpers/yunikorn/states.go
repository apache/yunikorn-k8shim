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

package yunikorn

var (
	states *AllStates
)

type AllStates struct {
	Application *ApplicationStates
	Node        *NodeStates
}

type ApplicationStates struct {
	New        string
	Recovering string
	Submitted  string
	Starting   string
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
	Accepted   string
	Healthy    string
	Rejected   string
	Draining   string
}

func States() *AllStates {
	if states == nil {
		states = &AllStates{
			Application: &ApplicationStates{
				New:        "New",
				Recovering: "Recovering",
				Submitted:  "Submitted",
				Starting:   "Starting",
				Accepted:   "Accepted",
				Running:    "Running",
				Rejected:   "Rejected",
				Completed:  "Completed",
				Killing:    "Killing",
				Killed:     "Killed",
				Failed:     "Failed",
			},
			Node: &NodeStates{
				New:        "New",
				Recovering: "Recovering",
				Accepted:   "Accepted",
				Healthy:    "Healthy",
				Rejected:   "Rejected",
				Draining:   "Draining",
			},
		}
	}
	return states
}
