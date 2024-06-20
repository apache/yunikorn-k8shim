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
	"fmt"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
)

func TestAllocateTaskEventArgs(t *testing.T) {
	alloc := NewAllocateTaskEvent("app-0001", "task-0001", "UID-0001", "node-0001")
	args := alloc.GetArgs()

	assert.Equal(t, len(args), 2)
	assert.Equal(t, fmt.Sprint(args[0]), "UID-0001")
	assert.Equal(t, fmt.Sprint(args[1]), "node-0001")
}

func TestGetAllocateTaskEventArgs(t *testing.T) {
	alloc := NewAllocateTaskEvent("app-0001", "task-0001", "UID-0001", "node-0001")
	args := alloc.GetArgs()
	assert.Equal(t, len(args), 2)
	assert.Equal(t, fmt.Sprint(args[0]), "UID-0001")
	assert.Equal(t, fmt.Sprint(args[1]), "node-0001")

	out := make([]string, 2)
	err := events.GetEventArgsAsStrings(out, args)
	assert.Assert(t, err == nil)
	assert.Equal(t, out[0], "UID-0001")
	assert.Equal(t, out[1], "node-0001")

	out = make([]string, 0)
	err = events.GetEventArgsAsStrings(out, args)
	assert.Assert(t, err != nil)

	out = make([]string, 5)
	err = events.GetEventArgsAsStrings(out, args)
	assert.Assert(t, err != nil)

	err = events.GetEventArgsAsStrings(nil, args)
	assert.Assert(t, err != nil)
}

func TestTaskEventsAsString(t *testing.T) {
	assert.Equal(t, InitTask.String(), "InitTask")
	assert.Equal(t, SubmitTask.String(), "SubmitTask")
	assert.Equal(t, TaskAllocated.String(), "TaskAllocated")
	assert.Equal(t, TaskRejected.String(), "TaskRejected")
	assert.Equal(t, TaskBound.String(), "TaskBound")
	assert.Equal(t, CompleteTask.String(), "CompleteTask")
	assert.Equal(t, TaskFail.String(), "TaskFail")
	assert.Equal(t, KillTask.String(), "KillTask")
	assert.Equal(t, TaskKilled.String(), "TaskKilled")
}
