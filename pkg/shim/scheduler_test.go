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

package shim

import (
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const configData = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100000000
                vcore: 10
              max:
                memory: 150000000
                vcore: 20
`

func TestApplicationScheduling(t *testing.T) {
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	nodeLabels := map[string]string{
		"label1": "key1",
		"label2": "key2",
	}

	// register nodes
	err = cluster.addNode("test.host.01", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")
	err = cluster.addNode("test.host.02", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 10000000).
		AddResource(siCommon.CPU, 1).
		Build()

	task1 := createTestPod("root.a", "app0001", "task0001", taskResource)
	task2 := createTestPod("root.a", "app0001", "task0002", taskResource)

	cluster.AddPod(task1)
	cluster.AddPod(task2)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Bound)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Bound)

	// complete pods
	task1Upd := task1.DeepCopy()
	task1Upd.Status.Phase = v1.PodSucceeded
	cluster.UpdatePod(task1, task1Upd)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Completed)
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	task2Upd := task2.DeepCopy()
	task2Upd.Status.Phase = v1.PodSucceeded
	cluster.UpdatePod(task2, task2Upd)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Completed)
	err = cluster.waitForApplicationStateInCore("app0001", partitionName, "Completing")
	assert.NilError(t, err)
	app := cluster.getApplicationFromCore("app0001", partitionName)
	assert.Equal(t, 0, len(app.GetAllRequests()), "asks were not removed from the application")
	assert.Equal(t, 0, len(app.GetAllAllocations()), "allocations were not removed from the application")
}

func TestRejectApplications(t *testing.T) {
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")

	nodeLabels := map[string]string{
		"label1": "key1",
		"label2": "key2",
	}

	// register nodes
	err = cluster.addNode("test.host.01", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err)
	err = cluster.addNode("test.host.02", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err)

	// create app and tasks
	appID := "app0001"
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 10000000).
		AddResource(siCommon.CPU, 1).
		Build()

	task1 := createTestPod("root.non_exist_queue", appID, "task0001", taskResource)
	cluster.AddPod(task1)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, appID, cache.ApplicationStates().Failed)

	// make the task terminal state
	cluster.DeletePod(task1)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Completed)
	// make sure the shim side has clean up the failed app
	cluster.waitForApplicationDeletion(t, appID)

	// submit again
	task1 = createTestPod("root.a", appID, "task0001", taskResource)
	cluster.AddPod(task1)

	cluster.waitAndAssertApplicationState(t, appID, cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, appID, "task0001", cache.TaskStates().Bound)
}

func TestSchedulerRegistrationFailed(t *testing.T) {
	var callback api.ResourceManagerCallback

	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.GetAPIs().SchedulerAPI = test.NewSchedulerAPIMock().RegisterFunction(
		func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, fmt.Errorf("some error")
		})

	ctx := cache.NewContext(mockedAPIProvider)
	shim := newShimSchedulerInternal(ctx, mockedAPIProvider, callback)
	assert.Error(t, shim.Run(), "some error")
	shim.Stop()
}

func TestTaskFailures(t *testing.T) {
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	// mock pod bind failures
	cluster.apiProvider.MockBindFn(func(pod *v1.Pod, hostID string) error {
		if pod.Name == "task0001" {
			return fmt.Errorf("mocked error when binding the pod")
		}
		return nil
	})

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")

	nodeLabels := map[string]string{
		"label1": "key1",
		"label2": "key2",
	}
	// register nodes
	err = cluster.addNode("test.host.01", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")
	err = cluster.addNode("test.host.02", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 50000000).
		AddResource(siCommon.CPU, 5).
		Build()
	task1 := createTestPod("root.a", "app0001", "task0001", taskResource)
	task2 := createTestPod("root.a", "app0001", "task0002", taskResource)
	cluster.AddPod(task1)
	cluster.AddPod(task2)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Failed)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Bound)

	// one task get bound, one ask failed, so we are expecting only 1 allocation in the scheduler
	err = cluster.waitAndVerifySchedulerAllocations("root.a",
		"[mycluster]default", "app0001", 1)
	assert.NilError(t, err, "number of allocations is not expected, error")
}

// simulate PVC error during Context.AssumePod() call
func TestAssumePodError(t *testing.T) {
	cluster := MockScheduler{}
	cluster.init()
	binder := test.NewVolumeBinderMock()
	binder.EnableVolumeClaimsError("unable to get volume claims")
	cluster.apiProvider.SetVolumeBinder(binder)
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	addNode(&cluster, "node-1")

	// create app and task which will fail due to simulated volume error
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1000).
		AddResource(siCommon.CPU, 1).
		Build()
	pod1 := createTestPod("root.a", "app0001", "task0001", taskResource)
	cluster.AddPod(pod1)

	// expect app to enter Completing state with allocation+ask removed
	err = cluster.waitForApplicationStateInCore("app0001", partitionName, "Completing")
	assert.NilError(t, err)
	app := cluster.getApplicationFromCore("app0001", partitionName)
	assert.Equal(t, 0, len(app.GetAllRequests()), "asks were not removed from the application")
	assert.Equal(t, 0, len(app.GetAllAllocations()), "allocations were not removed from the application")
}

func TestForeignPodTracking(t *testing.T) {
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	addNode(&cluster, "node-1")

	podResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1000).
		AddResource(siCommon.CPU, 1).
		Build()
	pod1 := createTestPod("root.a", "", "foreign-1", podResource)
	pod1.Spec.SchedulerName = ""
	pod1.Spec.NodeName = "node-1"
	pod2 := createTestPod("root.a", "", "foreign-2", podResource)
	pod2.Spec.SchedulerName = ""
	pod2.Spec.NodeName = "node-1"

	cluster.AddPod(pod1)
	cluster.AddPod(pod2)

	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-1", "node-1", true)
	assert.NilError(t, err)
	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-2", "node-1", true)
	assert.NilError(t, err)

	// update pod resources
	pod1Copy := pod1.DeepCopy()
	pod1Copy.Spec.Containers[0].Resources.Requests[siCommon.Memory] = *resource.NewQuantity(500, resource.DecimalSI)
	pod1Copy.Spec.Containers[0].Resources.Requests[siCommon.CPU] = *resource.NewMilliQuantity(2000, resource.DecimalSI)

	cluster.UpdatePod(pod1, pod1Copy)
	expectedUsage := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 500).
		AddResource(siCommon.CPU, 2).
		AddResource("pods", 1).
		Build()
	err = cluster.waitAndAssertForeignAllocationResources(partitionName, "foreign-1", "node-1", expectedUsage)
	assert.NilError(t, err)

	// delete pods
	cluster.DeletePod(pod1)
	cluster.DeletePod(pod2)

	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-1", "node-1", false)
	assert.NilError(t, err)
	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-2", "node-1", false)
	assert.NilError(t, err)
}

func TestSchedulingGates(t *testing.T) {
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	addNode(&cluster, "node-1")

	podResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 50000000).
		AddResource(siCommon.CPU, 5).
		Build()
	pod1 := createTestPod("root.a", "app0001", "task0001", podResource)
	pod1.Spec.SchedulingGates = []v1.PodSchedulingGate{{Name: "gate"}}

	cluster.AddPod(pod1)
	time.Sleep(time.Second)
	app := cluster.context.GetApplication("app0001")
	assert.Assert(t, app == nil, "application should not exist in the shim")
	coreApp := cluster.getApplicationFromCore("app0001", partitionName)
	assert.Assert(t, coreApp == nil, "application should not exist in the core")

	pod1Upd := pod1.DeepCopy()
	pod1Upd.Spec.SchedulingGates = nil
	cluster.UpdatePod(pod1, pod1Upd)
	err = cluster.waitForApplicationStateInCore("app0001", partitionName, "Running")
	assert.NilError(t, err, "application has not transitioned to Running state")
}

func createTestPod(queue string, appID string, taskID string, taskResource *si.Resource) *v1.Pod {
	containers := make([]v1.Container, 0)
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	for k, v := range taskResource.Resources {
		if k == siCommon.CPU {
			c1Resources[v1.ResourceName(k)] = *resource.NewMilliQuantity(v.Value, resource.DecimalSI)
		} else {
			c1Resources[v1.ResourceName(k)] = *resource.NewQuantity(v.Value, resource.DecimalSI)
		}
	}
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})
	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      taskID,
			Namespace: "default",
			UID:       types.UID(taskID),
			Labels: map[string]string{
				constants.LabelApplicationID: appID,
				constants.LabelQueueName:     queue,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			Containers:    containers,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
}
