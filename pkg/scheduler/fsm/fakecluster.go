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
	"fmt"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/callback"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/conf"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	utils "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/entrypoint"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
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
type FakeCluster struct {
	context *state.Context
	scheduler *ShimScheduler
	proxy *rmproxy.RMProxy
	client client.KubeClient
	conf string
	bindFn func(pod *v1.Pod, hostId string) error
	deleteFn func(pod *v1.Pod) error
	stopChan chan struct{}
}

func (fc *FakeCluster) init(queues string) {
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

	rmProxy, _, _  := entrypoint.StartAllServices()
	utils.MockSchedulerConfigByData([]byte(fc.conf))

	client := &client.FakeKubeClient{
		BindFn:   fc.bindFn,
		DeleteFn: fc.deleteFn,
	}
	context := state.NewContextInternal(rmProxy, &configs, client, true)
	callback := callback.NewAsyncRMCallback(context)
	ss := NewShimScheduler(rmProxy, context, callback)

	fc.context = context
	fc.scheduler = ss
	fc.proxy = rmProxy
	fc.client = client
}

func (fc *FakeCluster) start() {
	fc.scheduler.Run(fc.stopChan)
}

func (fc *FakeCluster) assertSchedulerState(t *testing.T, expectedState string) {
	assert.Equal(t, fc.scheduler.GetSchedulerState(), expectedState)
}

func (fc *FakeCluster) addNode(nodeName string, memory int64, cpu int64) error {
	nodeResource := common.NewResourceBuilder().
		AddResource(common.Memory, memory).
		AddResource(common.CPU, cpu).
		Build()
	node := common.CreateFromNodeSpec(nodeName, nodeName, nodeResource)
	request := common.CreateUpdateRequestForNewNode(node)
	fmt.Printf("report new nodes to scheduler, request: %s", request.String())
	return fc.proxy.Update(&request)
}

func (fc *FakeCluster) addTask(tid string, ask *si.Resource, app *common.Application) common.Task{
	task := common.CreateTaskForTest(tid, app, ask, fc.client, fc.proxy)
	app.AddTask(&task)
	return task
}

func (fc *FakeCluster) waitForSchedulerState(t *testing.T, expectedState string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fc.scheduler.GetSchedulerState() == expectedState {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("wait for scheduler to reach state %s failed, current state %s",
				expectedState, fc.scheduler.GetSchedulerState() )
		}
	}
}

func (fc *FakeCluster) waitAndAssertApplicationState(t *testing.T, appId string, expectedState string) {
	appList := fc.context.SelectApplications(func(app *common.Application) bool {
		return app.GetApplicationId() == appId
	})
	assert.Equal(t, len(appList), 1)
	assert.Equal(t, appList[0].GetApplicationId(), appId)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if appList[0].GetApplicationState() == expectedState {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("application %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				appId, expectedState, appList[0].GetApplicationState())
		}
	}
}

func (fc *FakeCluster) addApplication(app *common.Application) {
	fc.context.AddApplication(app)
}

func (fc *FakeCluster) newApplication(appId string, queueName string) *common.Application {
	return common.NewApplication(appId, queueName, fc.proxy)
}

func (fc *FakeCluster) waitAndAssertTaskState(t *testing.T, appId string, taskId string, expectedState string) {
	appList := fc.context.SelectApplications(func(app *common.Application) bool {
		return app.GetApplicationId() == appId
	})
	assert.Equal(t, len(appList), 1)
	assert.Equal(t, appList[0].GetApplicationId(), appId)
	assert.Assert(t, appList[0].GetTask(taskId) != nil)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if appList[0].GetTask(taskId).GetTaskState() == expectedState {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				taskId, expectedState, appList[0].GetTask(taskId).GetTaskState())
		}
	}
}

func (fc *FakeCluster) stop() {
	close(fc.stopChan)
}