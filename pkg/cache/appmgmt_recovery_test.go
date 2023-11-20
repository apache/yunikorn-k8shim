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
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAppManagerRecoveryState(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider(false)
	amService := NewAMService(amProtocol, apiProvider)
	// amService.register(&mockedAppManager{})

	apps, err := amService.recoverApps()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 2)

	for appId, app := range apps {
		assert.Assert(t, appId == "app01" || appId == "app02")
		assert.Equal(t, app.GetApplicationState(), ApplicationStates().Recovering)
	}
}

func TestAppManagerRecoveryTimeout(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider(false)
	amService := NewAMService(amProtocol, apiProvider)
	// amService.register(&mockedAppManager{})

	apps, err := amService.recoverApps()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 2)

	go func() {
		time.Sleep(3 * time.Second)
		amService.cancelWaitForAppRecovery()
	}()
	ok := amService.waitForAppRecovery(apps)
	assert.Assert(t, !ok, "expected timeout")
}

func TestAppManagerRecoveryExitCondition(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider(false)
	amService := NewAMService(amProtocol, apiProvider)
	// amService.register(&mockedAppManager{})

	apps, err := amService.recoverApps()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 2)

	// simulate app recovery succeed
	for _, app := range apps {
		app.SetState(ApplicationStates().Accepted)
	}

	go func() {
		time.Sleep(3 * time.Second)
		amService.cancelWaitForAppRecovery()
	}()
	ok := amService.waitForAppRecovery(apps)
	assert.Assert(t, ok, "timeout waiting for recovery")
}

func TestAppManagerRecoveryFailureExitCondition(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider(false)
	amService := NewAMService(amProtocol, apiProvider)
	// amService.register(&mockedAppManager{})

	apps, err := amService.recoverApps()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 2)

	// simulate app rejected
	for _, app := range apps {
		app.SetState(ApplicationStates().Rejected)
	}

	go func() {
		time.Sleep(3 * time.Second)
		amService.cancelWaitForAppRecovery()
	}()
	ok := amService.waitForAppRecovery(apps)
	assert.Assert(t, ok, "timeout waiting for recovery")
}

// test app state transition during recovery
func TestAppStatesDuringRecovery(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	apiProvider := client.NewMockedAPIProvider(false)
	ctx := NewContext(apiProvider)
	cb := NewAsyncRMCallback(ctx)

	dispatcher.RegisterEventHandler(dispatcher.EventTypeApp, ctx.ApplicationEventHandler())
	dispatcher.Start()
	defer dispatcher.Stop()

	amService := NewAMService(ctx, apiProvider)
	_ = &mockedAppManager{}
	// amService.register(&mockedAppManager{})

	apps, err := amService.recoverApps()
	assert.NilError(t, err)
	assert.Equal(t, len(apps), 2)

	// when the recovery starts, all apps should be under Recovering state
	app01 := ctx.GetApplication("app01")
	app02 := ctx.GetApplication("app02")

	// waitForAppRecovery call should be blocked
	// because the scheduler is still doing recovery
	go func() {
		time.Sleep(3 * time.Second)
		amService.cancelWaitForAppRecovery()
	}()
	ok := amService.waitForAppRecovery(apps)
	assert.Assert(t, !ok, "expected timeout")
	assert.Equal(t, app01.GetApplicationState(), ApplicationStates().Recovering)
	assert.Equal(t, app02.GetApplicationState(), ApplicationStates().Recovering)

	// mock the responses, simulate app01 has been accepted
	err = cb.UpdateApplication(&si.ApplicationResponse{
		Accepted: []*si.AcceptedApplication{
			{
				ApplicationID: "app01",
			},
		},
	})
	assert.NilError(t, err, "failed to handle UpdateResponse")

	// since app02 is still under recovery
	// waitForRecovery should timeout because the scheduler is still under recovery
	go func() {
		time.Sleep(3 * time.Second)
		amService.cancelWaitForAppRecovery()
	}()
	ok = amService.waitForAppRecovery(apps)
	assert.Assert(t, !ok, "expected timeout")
	assert.Equal(t, app01.GetApplicationState(), ApplicationStates().Accepted)
	assert.Equal(t, app02.GetApplicationState(), ApplicationStates().Recovering)

	// mock the responses, simulate app02 has been accepted
	err = cb.UpdateApplication(&si.ApplicationResponse{
		Accepted: []*si.AcceptedApplication{
			{
				ApplicationID: "app02",
			},
		},
	})
	assert.NilError(t, err, "failed to handle UpdateResponse")

	// the app recovery has finished,
	// this should not timeout anymore
	go func() {
		time.Sleep(3 * time.Second)
		amService.cancelWaitForAppRecovery()
	}()
	ok = amService.waitForAppRecovery(apps)
	assert.Assert(t, ok, "unexpected timeout")
	assert.Equal(t, app01.GetApplicationState(), ApplicationStates().Accepted)
	assert.Equal(t, app02.GetApplicationState(), ApplicationStates().Accepted)
}

func TestPodRecovery(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider(false)
	taskRequests := make([]*AddTaskRequest, 0)
	amProtocol.UseAddTaskFn(func(request *AddTaskRequest) {
		taskRequests = append(taskRequests, request)
	})
	amService := NewAMService(amProtocol, apiProvider)
	_ = &mockedAppManager{}
	// amService.register(&mockedAppManager{})

	apps, err := amService.recoverApps()
	assert.NilError(t, err)
	assert.Equal(t, 4, len(taskRequests))
	assert.Equal(t, 2, len(apps))

	expected := map[string]map[string]bool{
		"app01": {
			"task01": true,
			"task02": true,
		},
		"app02": {
			"task04": true,
			"task05": true,
		},
	}

	for _, tr := range taskRequests {
		check, ok := expected[tr.Metadata.ApplicationID]
		assert.Assert(t, ok, "app should not be recovered: "+tr.Metadata.ApplicationID)
		assert.Assert(t, check[tr.Metadata.TaskID], "task should not be recovered: "+tr.Metadata.TaskID)
	}
}

func TestPodsSortedDuringRecovery(t *testing.T) {
	t.Skip("broken")
	// conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := NewMockedAMProtocol()
	taskRequests := make([]*AddTaskRequest, 0)
	amProtocol.UseAddTaskFn(func(request *AddTaskRequest) {
		taskRequests = append(taskRequests, request)
	})
	apiProvider := client.NewMockedAPIProvider(false)
	amService := NewAMService(amProtocol, apiProvider)
	_ = &mockedAppManager{}
	// amService.register(&mockedAppManager{})

	_, err := amService.recoverApps()
	assert.NilError(t, err)

	assert.Equal(t, 4, len(taskRequests))
	var previous int64
	previous = -1
	for _, req := range taskRequests {
		current := req.Metadata.Pod.CreationTimestamp.Unix()
		assert.Assert(t, current > previous, "Pods were not processed in sorted order")
		previous = current
	}
}

type mockedAppManager struct {
}

func (ma *mockedAppManager) Name() string {
	return "mocked-app-manager"
}

func (ma *mockedAppManager) ServiceInit() error {
	return nil
}

func (ma *mockedAppManager) Start() error {
	return nil
}

func (ma *mockedAppManager) Stop() {
	// noop
}

func (ma *mockedAppManager) ListPods() ([]*v1.Pod, error) {
	pods := make([]*v1.Pod, 8)
	pods[0] = ma.newPod("pod1", "task01", "app01", time.Unix(100, 0), v1.PodRunning)
	pods[1] = ma.newPod("pod2", "task02", "app01", time.Unix(500, 0), v1.PodPending)
	pods[2] = ma.newPod("pod3", "task03", "app01", time.Unix(200, 0), v1.PodSucceeded)
	pods[3] = ma.newPod("pod4", "task04", "app02", time.Unix(400, 0), v1.PodRunning)
	pods[4] = ma.newPod("pod5", "task05", "app02", time.Unix(300, 0), v1.PodPending)
	pods[5] = ma.newPod("pod6", "task06", "app02", time.Unix(600, 0), v1.PodFailed)

	// these pods and apps should never be recovered
	pods[6] = ma.newPod("pod7", "task07", "app03", time.Unix(300, 0), v1.PodFailed)
	pods[7] = ma.newPod("pod8", "task08", "app04", time.Unix(300, 0), v1.PodSucceeded)

	return pods, nil
}

func (ma *mockedAppManager) GetExistingAllocation(pod *v1.Pod) *si.Allocation {
	return nil
}

func (ma *mockedAppManager) newPod(name, podUID, appID string, creationTimeStamp time.Time, phase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      name,
			Namespace: "yk",
			UID:       types.UID(podUID),
			Annotations: map[string]string{
				constants.AnnotationApplicationID: appID,
			},
			CreationTimestamp: apis.NewTime(creationTimeStamp),
		},
		Spec: v1.PodSpec{
			NodeName:      "fake-node",
			SchedulerName: constants.SchedulerName,
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
}
