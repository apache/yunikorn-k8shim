//go:build graphviz
// +build graphviz

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
	"os"
	"testing"

	"github.com/looplab/fsm"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTaskFsmGraph(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)

	// pod has timestamp defined
	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "pod-00",
			UID:  "UID-00",
		},
	}

	task := NewTask("task00", app, mockedContext, pod)
	graph := fsm.Visualize(task.sm)

	err := os.MkdirAll("../../_output/fsm", 0755)
	assert.NilError(t, err, "Creating output dir failed")
	os.WriteFile("../../_output/fsm/k8shim-task-state.dot", []byte(graph), 0644)
	assert.NilError(t, err, "Writing graph failed")
}
