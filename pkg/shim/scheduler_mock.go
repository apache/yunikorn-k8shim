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
	utils "github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/entrypoint"
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/callback"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"testing"
	"time"
)

const fakeClusterId = "test-cluster"
const fakeClusterVersion = "0.1.0"
const fakeClusterSchedulerName = "yunikorn-test"
const fakeClusterSchedulingInterval = 1

// fake cluster is used for testing
// it uses fake kube client to simulate API calls with k8s, all other code paths are real
type MockScheduler struct {
	context *cache.Context
	scheduler *KubernetesShim
	proxy  api.SchedulerApi
	client client.KubeClient
	conf string
	bindFn func(pod *v1.Pod, hostId string) error
	deleteFn func(pod *v1.Pod) error
	stopChan chan struct{}
}

func (fc *MockScheduler) init(queues string) {
	configs := conf.SchedulerConf{
		ClusterId:      fakeClusterId,
		ClusterVersion: fakeClusterVersion,
		SchedulerName:  fakeClusterSchedulerName,
		Interval:       fakeClusterSchedulingInterval,
		KubeConfig:     "",
	}

	fc.conf = queues
	fc.stopChan = make(chan struct{})
	// default functions for bind and delete, this can be override if necessary
	fc.deleteFn = func(pod *v1.Pod) error {
		fmt.Printf("pod deleted")
		return nil
	}
	fc.bindFn = func(pod *v1.Pod, hostId string) error {
		fmt.Printf("pod bound")
		return nil
	}

	serviceContext := entrypoint.StartAllServices()
	rmProxy := serviceContext.RMProxy
	utils.MockSchedulerConfigByData([]byte(fc.conf))

	fakeClient := test.NewKubeClientMock()
	fakeClient.MockBindFn(fc.bindFn)
	fakeClient.MockDeleteFn(fc.deleteFn)

	schedulerApi, _ := rmProxy.(api.SchedulerApi)
	context := cache.NewContextInternal(schedulerApi, &configs, fakeClient, true)
	rmCallback := callback.NewAsyncRMCallback(context)

	ss := newShimSchedulerInternal(schedulerApi, context, rmCallback)

	fc.context = context
	fc.scheduler = ss
	fc.proxy = schedulerApi
	fc.client = fakeClient
}

func (fc *MockScheduler) start() {
	fc.scheduler.run()
}

func (fc *MockScheduler) assertSchedulerState(t *testing.T, expectedState string) {
	assert.Equal(t, fc.scheduler.GetSchedulerState(), expectedState)
}

func (fc *MockScheduler) addNode(nodeName string, memory int64, cpu int64) error {
	nodeResource := common.NewResourceBuilder().
		AddResource(common.Memory, memory).
		AddResource(common.CPU, cpu).
		Build()
	node := common.CreateFromNodeSpec(nodeName, nodeName, nodeResource)
	request := common.CreateUpdateRequestForNewNode(node)
	fmt.Printf("report new nodes to scheduler, request: %s", request.String())
	return fc.proxy.Update(&request)
}

func (fc *MockScheduler) addTask(tid string, ask *si.Resource, app *cache.Application) cache.Task{
	task := cache.CreateTaskForTest(tid, app, ask, fc.client, fc.proxy)
	app.AddTask(&task)
	return task
}

func (fc *MockScheduler) waitForSchedulerState(t *testing.T, expectedState string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fc.scheduler.GetSchedulerState() == expectedState {
			time.Sleep(1*time.Second)
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("wait for scheduler to reach state %s failed, current state %s",
				expectedState, fc.scheduler.GetSchedulerState() )
		}
	}
}

func (fc *MockScheduler) waitAndAssertApplicationState(t *testing.T, appId string, expectedState string) {
	appList := fc.context.SelectApplications(func(app *cache.Application) bool {
		return app.GetApplicationId() == appId
	})
	assert.Equal(t, len(appList), 1)
	assert.Equal(t, appList[0].GetApplicationId(), appId)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if appList[0].GetApplicationState() == expectedState {
			time.Sleep(1*time.Second)
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("application %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				appId, expectedState, appList[0].GetApplicationState())
		}
	}
}

func (fc *MockScheduler) addApplication(app *cache.Application) {
	fc.context.AddApplication(app)
}

func (fc *MockScheduler) newApplication(appId string, queueName string) *cache.Application {
	app := cache.NewApplication(appId, queueName, fc.proxy)
	return app
}

func (fc *MockScheduler) waitAndAssertTaskState(t *testing.T, appId string, taskId string, expectedState string) {
	appList := fc.context.SelectApplications(func(app *cache.Application) bool {
		return app.GetApplicationId() == appId
	})
	assert.Equal(t, len(appList), 1)
	assert.Equal(t, appList[0].GetApplicationId(), appId)

	task, err := appList[0].GetTask(taskId)
	assert.Assert(t, err == nil)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if task.GetTaskState() == expectedState {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				taskId, expectedState, task.GetTaskState())
		}
	}
}

func (fc *MockScheduler) stop() {
	close(fc.stopChan)
	fc.scheduler.stop()
}
