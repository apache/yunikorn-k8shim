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
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	coreconfigs "github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/callback"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// fake cluster is used for testing
// it uses fake kube client to simulate API calls with k8s, all other code paths are real
type MockScheduler struct {
	context     *cache.Context
	scheduler   *KubernetesShim
	coreContext *entrypoint.ServiceContext
	apiProvider *client.MockedAPIProvider
	stopChan    chan struct{}
}

func (fc *MockScheduler) init(queues string) {
	conf.GetSchedulerConf().SetTestMode(true)
	fc.stopChan = make(chan struct{})

	serviceContext := entrypoint.StartAllServices()
	rmProxy := serviceContext.RMProxy
	coreconfigs.MockSchedulerConfigByData([]byte(queues))
	schedulerAPI, ok := rmProxy.(api.SchedulerAPI)
	if !ok {
		log.Logger().Debug("cast failed unexpected object",
			zap.Any("schedulerAPI", rmProxy))
	}

	mockedAPIProvider := client.NewMockedAPIProvider()
	mockedAPIProvider.GetAPIs().SchedulerAPI = schedulerAPI

	context := cache.NewContext(mockedAPIProvider)
	rmCallback := callback.NewAsyncRMCallback(context)
	amSvc := appmgmt.NewAMService(context, mockedAPIProvider)
	ss := newShimSchedulerInternal(context, mockedAPIProvider, amSvc, rmCallback)

	fc.context = context
	fc.scheduler = ss
	fc.coreContext = serviceContext
	fc.apiProvider = mockedAPIProvider
}

func (fc *MockScheduler) start() {
	fc.scheduler.run()
}

func (fc *MockScheduler) addNode(nodeName string, memory, cpu int64) error {
	nodeResource := common.NewResourceBuilder().
		AddResource(constants.Memory, memory).
		AddResource(constants.CPU, cpu).
		Build()
	node := common.CreateFromNodeSpec(nodeName, nodeName, nodeResource)
	request := common.CreateUpdateRequestForNewNode(node)
	fmt.Printf("report new nodes to scheduler, request: %s", request.String())
	return fc.apiProvider.GetAPIs().SchedulerAPI.Update(&request)
}

func (fc *MockScheduler) addTask(appID string, taskID string, ask *si.Resource) {
	fc.context.AddTask(&interfaces.AddTaskRequest{
		Metadata: interfaces.TaskMetadata{
			ApplicationID: appID,
			TaskID:        taskID,
			Pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: taskID,
				},
			},
		},
		Recovery: false,
	})
}

func (fc *MockScheduler) waitForSchedulerState(t *testing.T, expectedState string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fc.scheduler.GetSchedulerState() == expectedState {
			break
		}
		log.Logger().Info("waiting for scheduler state",
			zap.String("expected", expectedState),
			zap.String("actual", fc.scheduler.GetSchedulerState()))
		time.Sleep(time.Second)
		if time.Now().After(deadline) {
			t.Errorf("wait for scheduler to reach state %s failed, current state %s",
				expectedState, fc.scheduler.GetSchedulerState())
		}
	}
}

func (fc *MockScheduler) waitAndAssertApplicationState(t *testing.T, appID, expectedState string) {
	appList := fc.context.SelectApplications(func(app *cache.Application) bool {
		return app.GetApplicationID() == appID
	})
	assert.Equal(t, len(appList), 1)
	assert.Equal(t, appList[0].GetApplicationID(), appID)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if appList[0].GetApplicationState() == expectedState {
			break
		}
		log.Logger().Info("waiting for app state",
			zap.String("expected", expectedState),
			zap.String("actual", appList[0].GetApplicationState()))
		time.Sleep(time.Second)
		if time.Now().After(deadline) {
			t.Errorf("application %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				appID, expectedState, appList[0].GetApplicationState())
		}
	}
}

func (fc *MockScheduler) addApplication(appId string, queue string) {
	fc.context.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: interfaces.ApplicationMetadata{
			ApplicationID: appId,
			QueueName:     queue,
			User:          "test-user",
			Tags:          map[string]string{"app-type": "test-app"},
		},
		Recovery: false,
	})
}

func (fc *MockScheduler) removeApplication(appId string) error {
	return fc.context.RemoveApplication(appId)
}

func (fc *MockScheduler) newApplication(appID, queueName string) *cache.Application {
	app := cache.NewApplication(appID, queueName, "testuser", map[string]string{}, fc.apiProvider.GetAPIs().SchedulerAPI)
	return app
}

func (fc *MockScheduler) waitAndAssertTaskState(t *testing.T, appID, taskID, expectedState string) {
	appList := fc.context.SelectApplications(func(app *cache.Application) bool {
		return app.GetApplicationID() == appID
	})
	assert.Equal(t, len(appList), 1)
	assert.Equal(t, appList[0].GetApplicationID(), appID)

	task, err := appList[0].GetTask(taskID)
	assert.NilError(t, err, "Task retrieval failed")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if task.GetTaskState() == expectedState {
			break
		}
		log.Logger().Info("waiting for task state",
			zap.String("expected", expectedState),
			zap.String("actual", task.GetTaskState()))
		time.Sleep(time.Second)
		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				taskID, expectedState, task.GetTaskState())
		}
	}
}

func (fc *MockScheduler) waitAndVerifySchedulerAllocations(
	queueName, partitionName, applicationID string, expectedNumOfAllocations int) error {
	partition := fc.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName)
	if partition == nil {
		return fmt.Errorf("partition %s is not found in the scheduler context", partitionName)
	}

	return utils.WaitForCondition(func() bool {
		for _, app := range partition.GetApplications() {
			if app.ApplicationID == applicationID {
				if len(app.GetAllAllocations()) == expectedNumOfAllocations {
					return true
				}
			}
		}
		return false
	}, time.Second, 5*time.Second)
}

func (fc *MockScheduler) stop() {
	close(fc.stopChan)
	fc.scheduler.stop()
}
