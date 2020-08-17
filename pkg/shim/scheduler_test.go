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

package main

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/test"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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
	err := cluster.addNode("test.host.01", 100, 10)
	assert.NilError(t, err, "add node failed")
	err = cluster.addNode("test.host.02", 100, 10)
	assert.NilError(t, err, "add node failed")

	// create app and tasks
	cluster.addApplication("app0001", "root.a")
	taskResource := common.NewResourceBuilder().
		AddResource(constants.Memory, 10).
		AddResource(constants.CPU, 1).
		Build()
	cluster.addTask("app0001", "task0001", taskResource)
	cluster.addTask("app0001", "task0002", taskResource)

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
	err := cluster.addNode("test.host.01", 100, 10)
	assert.NilError(t, err)
	err = cluster.addNode("test.host.02", 100, 10)
	assert.NilError(t, err)

	// add app to context
	appID := "app0001"
	cluster.addApplication(appID, "root.non_exist_queue")

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(constants.Memory, 10).
		AddResource(constants.CPU, 1).
		Build()
	cluster.addTask(appID, "task0001", taskResource)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, appID, events.States().Application.Failed)

	// remove the application
	// remove task first or removeApplication will fail
	err = cluster.context.RemoveTask(appID, "task0001")
	assert.Assert(t, err == nil)
	err = cluster.removeApplication(appID)
	assert.Assert(t, err == nil)

	// submit the app again
	cluster.addApplication(appID, "root.a")
	cluster.addTask(appID, "task0001", taskResource)
	cluster.waitAndAssertApplicationState(t, appID, events.States().Application.Running)
	cluster.waitAndAssertTaskState(t, appID, "task0001", events.States().Task.Bound)
}

func TestSchedulerRegistrationFailed(t *testing.T) {
	var callback api.ResourceManagerCallback

	mockedAMProtocol := cache.NewMockedAMProtocol()
	mockedAPIProvider := client.NewMockedAPIProvider()
	mockedAPIProvider.GetAPIs().SchedulerAPI = test.NewSchedulerAPIMock().RegisterFunction(
		func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, fmt.Errorf("some error")
		})

	ctx := cache.NewContext(mockedAPIProvider)
	shim := newShimSchedulerInternal(ctx, mockedAPIProvider,
		appmgmt.NewAMService(mockedAMProtocol, mockedAPIProvider), callback)
	shim.run()
	defer shim.stop()

	err := waitShimSchedulerState(shim, events.States().Scheduler.Stopped, 5*time.Second)
	assert.NilError(t, err)
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
	cluster.init(configData)
	cluster.start()
	defer cluster.stop()

	// mock pod bind failures
	cluster.apiProvider.MockBindFn(func(pod *v1.Pod, hostID string) error {
		if pod.Name == "task0001" {
			return fmt.Errorf("mocked error when binding the pod")
		}
		return nil
	})

	// ensure scheduler state
	cluster.waitForSchedulerState(t, events.States().Scheduler.Running)

	// register nodes
	err := cluster.addNode("test.host.01", 100, 10)
	assert.NilError(t, err, "add node failed")
	err = cluster.addNode("test.host.02", 100, 10)
	assert.NilError(t, err, "add node failed")

	// create app and tasks
	cluster.addApplication("app0001", "root.a")
	taskResource := common.NewResourceBuilder().
		AddResource(constants.Memory, 50).
		AddResource(constants.CPU, 5).
		Build()
	cluster.addTask("app0001", "task0001", taskResource)
	cluster.addTask("app0001", "task0002", taskResource)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", events.States().Application.Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", events.States().Task.Failed)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", events.States().Task.Bound)

	// one task get bound, one ask failed, so we are expecting only 1 allocation in the scheduler
	err = cluster.waitAndVerifySchedulerAllocations("root.a",
		"[my-kube-cluster]default", "app0001", 1)
	assert.NilError(t, err, "number of allocations is not expected, error")
}

func waitShimSchedulerState(shim *KubernetesShim, expectedState string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if shim.GetSchedulerState() == expectedState {
			log.Logger().Info("waiting for state",
				zap.String("expect", expectedState),
				zap.String("current", shim.GetSchedulerState()))
			return nil
		}
		time.Sleep(1 * time.Second)
		if time.Now().After(deadline) {
			return fmt.Errorf("scheduler has not reached expected state %s in %d seconds, current state: %s",
				expectedState, deadline.Second(), shim.GetSchedulerState())
		}
	}
}
