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

package fsm

import (
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
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
        resources:
          guaranteed:
            memory: 200
            vcore: 20
          max:
            memory: 200
            vcore: 20
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
	cluster := FakeCluster{}
	cluster.init(configData)
	cluster.start()
	defer cluster.stop()

	// ensure scheduler state
	cluster.waitForSchedulerState(t, common.States().Scheduler.Registered)

	// register nodes
	cluster.addNode("test.host.01", 100, 10)
	cluster.addNode("test.host.02", 100, 10)

	// create app and tasks
	app0001 := cluster.newApplication("app0001", "root.a")
	cluster.addTask("task0001", common.CreateResource(10, 1), app0001)
	cluster.addTask("task0002", common.CreateResource(10, 1), app0001)

	// add app to context
	cluster.addApplication(app0001)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", common.States().Application.Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", common.States().Task.Bound)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", common.States().Task.Bound)
}