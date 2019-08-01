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

package main

import (
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"testing"
)

func TestApplicationScheduling(t *testing.T) {
	configData := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
`
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init(configData)
	cluster.start()
	defer cluster.stop()

	// ensure scheduler state
	cluster.waitForSchedulerState(t, events.States().Scheduler.Registered)

	// register nodes
	if err := cluster.addNode("test.host.01", 100, 10); err != nil {
		t.Fatalf("add node failed %v", err)
	}
	if err := cluster.addNode("test.host.02", 100, 10); err != nil {
		t.Fatalf("add node failed %v", err)
	}

	// create app and tasks
	app0001 := cluster.newApplication("app0001", "root.a")
	taskResource := common.NewResourceBuilder().
		AddResource(common.Memory, 10).
		AddResource(common.CPU, 1).
		Build()
	cluster.addTask("task0001", taskResource, app0001)
	cluster.addTask("task0002", taskResource, app0001)

	// add app to context
	cluster.addApplication(app0001)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", events.States().Application.Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", events.States().Task.Bound)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", events.States().Task.Bound)
}

func TestRejectApplications(t *testing.T) {
	configData := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a
            resources:
              guaranteed:
                memory: 100
                vcore: 10
              max:
                memory: 150
                vcore: 20
`
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init(configData)
	cluster.start()
	defer cluster.stop()

	// ensure scheduler state
	cluster.waitForSchedulerState(t, events.States().Scheduler.Registered)

	// register nodes
	if err := cluster.addNode("test.host.01", 100, 10); err != nil {
		t.Fatalf("%v", err)
	}
	if err := cluster.addNode("test.host.02", 100, 10); err != nil {
		t.Fatalf("%v", err)
	}

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(common.Memory, 10).
		AddResource(common.CPU, 1).
		Build()
	app0001 := cluster.newApplication("app0001", "root.non_exist_queue")
	cluster.addTask("task0001", taskResource, app0001)

	// add app to context
	cluster.addApplication(app0001)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", events.States().Application.Failed)

	// submit the app again
	app0001 = cluster.newApplication("app0001", "root.a")
	cluster.addTask("task0001", taskResource, app0001)
	cluster.addApplication(app0001)
	cluster.waitAndAssertApplicationState(t, "app0001", events.States().Application.Accepted)
}
