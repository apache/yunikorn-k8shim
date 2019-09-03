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
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
	"testing"
	"time"
)

func TestApplicationScheduling(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
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
	cluster.waitForSchedulerState(t, events.States().Scheduler.Running)

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
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
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
	cluster.waitForSchedulerState(t, events.States().Scheduler.Running)

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
	cluster.waitAndAssertApplicationState(t, "app0001", events.States().Application.Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", events.States().Task.Bound)
}

func TestSchedulerRegistrationFailed(t *testing.T){
	var ctx *cache.Context
	var callback api.ResourceManagerCallback

	schedulerApi := test.NewSchedulerApiMock().RegisterFunction(
		func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
				return nil, fmt.Errorf("some error")
		})

	shim := newShimSchedulerInternal(schedulerApi, ctx, callback)
	shim.run()
	defer shim.stop()

	if err := waitShimSchedulerState(shim, events.States().Scheduler.Stopped, 5 * time.Second); err !=nil {
		t.Fatalf("%v", err)
	}
}

func TestTaskFailures(t *testing.T) {
	configData := `
partitions:
 -
   name: default
   queues:
     -
       name: root
       submitacl: "*"
       queues:
         -
           name: a
           resources:
             guaranteed:
               memory: 100
               vcore: 10
             max:
               memory: 100
               vcore: 10
`
	// init and register scheduler
	cluster := MockScheduler{}
	// mock pod bind failures
	cluster.bindFn = func(pod *v1.Pod, hostId string) error {
		if pod.Name == "task0001" {
			return fmt.Errorf("mocked error when binding the pod")
		}
		return nil
	}
	cluster.init(configData)
	cluster.start()
	defer cluster.stop()

	// ensure scheduler state
	cluster.waitForSchedulerState(t, events.States().Scheduler.Running)

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
		AddResource(common.Memory, 50).
		AddResource(common.CPU, 5).
		Build()
	cluster.addTask("task0001", taskResource, app0001)
	cluster.addTask("task0002", taskResource, app0001)

	// add app to context
	cluster.addApplication(app0001)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", events.States().Application.Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", events.States().Task.Failed)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", events.States().Task.Bound)

	// one task get bound, one ask failed, so we are expecting only 1 allocation in the scheduler
	if err := cluster.waitAndVerifySchedulerAllocations("root.a",
		"[test-cluster]default","app0001", 1); err != nil {
		t.Fatalf("number of allocations is not expected, error: %v", err)
	}
}

func waitShimSchedulerState(shim *KubernetesShim, expectedState string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if shim.GetSchedulerState() == expectedState {
			log.Logger.Info("waiting for state",
				zap.String("expect", expectedState),
				zap.String("current", shim.GetSchedulerState()))
			return nil
		}
		time.Sleep(1*time.Second)
		if time.Now().After(deadline) {
			return fmt.Errorf("scheduler has not reached expected state %s in %d seconds, current state: %s",
				expectedState, deadline.Second(), shim.GetSchedulerState())
		}
	}
}
