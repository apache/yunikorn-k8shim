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
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	schedv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// fake cluster is used for testing
// it uses fake kube client to simulate API calls with k8s, all other code paths are real
type MockScheduler struct {
	context     *cache.Context
	rmProxy     api.SchedulerAPI
	scheduler   *KubernetesShim
	coreContext *entrypoint.ServiceContext
	apiProvider *client.MockedAPIProvider
	stopChan    chan struct{}
	started     atomic.Bool
}

func (fc *MockScheduler) init() {
	fc.stopChan = make(chan struct{})
	serviceContext := entrypoint.StartAllServices()
	fc.rmProxy = serviceContext.RMProxy
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.GetAPIs().SchedulerAPI = fc.rmProxy
	events.SetRecorder(events.NewMockedRecorder())

	context := cache.NewContext(mockedAPIProvider)
	rmCallback := cache.NewAsyncRMCallback(context)
	ss := newShimSchedulerInternal(context, mockedAPIProvider, rmCallback)

	fc.context = context
	fc.scheduler = ss
	fc.coreContext = serviceContext
	fc.apiProvider = mockedAPIProvider
}

func (fc *MockScheduler) start() error {
	fc.apiProvider.RunEventHandler() // must be called first
	if err := fc.scheduler.Run(); err != nil {
		fc.started.Store(false)
		return err
	}
	fc.started.Store(true)
	return nil
}

func (fc *MockScheduler) updateConfig(queues string, extraConfig map[string]string) error {
	return fc.rmProxy.UpdateConfiguration(&si.UpdateConfigurationRequest{
		RmID:        conf.GetSchedulerConf().ClusterID,
		PolicyGroup: conf.GetSchedulerConf().PolicyGroup,
		ExtraConfig: extraConfig,
		Config:      queues,
	})
}

// Deprecated: this method only updates the core without the shim. Prefer MockScheduler.AddNode(*v1.Node) instead.
func (fc *MockScheduler) addNode(nodeName string, nodeLabels map[string]string, memory, cpu, pods int64) error {
	cache := fc.context.GetSchedulerCache()
	zero := resource.Scale(0)
	// add node to the cache so that predicates can run properly
	cache.UpdateNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: nodeLabels,
		},
		Status: v1.NodeStatus{
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourcePods:   *resource.NewScaledQuantity(pods, zero),
				v1.ResourceMemory: *resource.NewScaledQuantity(memory, zero),
				v1.ResourceCPU:    *resource.NewScaledQuantity(cpu, zero),
			},
			Capacity: map[v1.ResourceName]resource.Quantity{
				v1.ResourcePods:   *resource.NewScaledQuantity(pods, zero),
				v1.ResourceMemory: *resource.NewScaledQuantity(memory, zero),
				v1.ResourceCPU:    *resource.NewScaledQuantity(cpu, zero),
			},
		},
	})

	nodeResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, memory).
		AddResource(siCommon.CPU, cpu).
		AddResource("pods", pods).
		Build()
	request := createUpdateRequestForNewNode(nodeName, nodeLabels, nodeResource, nil)
	fmt.Printf("report new nodes to scheduler, request: %s", request.String())
	return fc.apiProvider.GetAPIs().SchedulerAPI.UpdateNode(request)
}

func (fc *MockScheduler) waitForApplicationDeletion(t *testing.T, appID string) {
	err := utils.WaitForCondition(func() bool {
		return fc.context.GetApplication(appID) == nil
	}, time.Second, 5*time.Second)
	assert.NilError(t, err, "application has not been deleted")
}

func (fc *MockScheduler) waitAndAssertApplicationState(t *testing.T, appID, expectedState string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		app := fc.context.GetApplication(appID)
		if app != nil {
			assert.Equal(t, app.GetApplicationID(), appID)
		}
		if app != nil && app.GetApplicationState() == expectedState {
			break
		}
		actual := "<none>"
		if app != nil {
			actual = app.GetApplicationState()
		}
		log.Log(log.Test).Info("waiting for app state",
			zap.String("expected", expectedState),
			zap.String("actual", actual))
		time.Sleep(time.Second)
		if time.Now().After(deadline) {
			t.Errorf("application %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				appID, expectedState, actual)
			return
		}
	}
}

func (fc *MockScheduler) waitAndAssertTaskState(t *testing.T, appID, taskID, expectedState string) {
	app := fc.context.GetApplication(appID)
	assert.Equal(t, app != nil, true)
	assert.Equal(t, app.GetApplicationID(), appID)

	task := app.GetTask(taskID)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if task.GetTaskState() == expectedState {
			break
		}
		log.Log(log.Test).Info("waiting for task state",
			zap.String("expected", expectedState),
			zap.String("actual", task.GetTaskState()))
		time.Sleep(time.Second)
		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				taskID, expectedState, task.GetTaskState())
			return
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
	fc.scheduler.Stop()
	fc.apiProvider.Stop()
	fc.started.Store(false)
}

func (fc *MockScheduler) AddPodToLister(pod *v1.Pod) {
	lister := fc.apiProvider.GetPodListerMock()
	if lister == nil {
		panic("lister is unset or it has unsupported type")
	}
	lister.AddPod(pod)
}

func (fc *MockScheduler) RemovePodFromLister(pod *v1.Pod) {
	lister := fc.apiProvider.GetPodListerMock()
	if lister == nil {
		panic("lister is unset or it has unsupported type")
	}
	lister.DeletePod(pod)
}

func (fc *MockScheduler) AddNodeToLister(node *v1.Node) {
	lister := fc.apiProvider.GetNodeListerMock()
	if lister == nil {
		panic("lister is unset or it has unsupported type")
	}
	lister.AddNode(node)
}

func (fc *MockScheduler) AddPod(pod *v1.Pod) {
	fc.ensureStarted()
	fc.apiProvider.AddPod(pod)
}

func (fc *MockScheduler) UpdatePod(oldObj *v1.Pod, newObj *v1.Pod) {
	fc.ensureStarted()
	fc.apiProvider.UpdatePod(oldObj, newObj)
}

func (fc *MockScheduler) DeletePod(obj *v1.Pod) {
	fc.ensureStarted()
	fc.apiProvider.DeletePod(obj)
}

func (fc *MockScheduler) AddNode(obj *v1.Node) {
	fc.ensureStarted()
	fc.apiProvider.AddNode(obj)
}

func (fc *MockScheduler) DeleteNode(obj *v1.Node) {
	fc.ensureStarted()
	fc.apiProvider.DeleteNode(obj)
}

func (fc *MockScheduler) UpdateNode(oldObj *v1.Node, newObj *v1.Node) {
	fc.ensureStarted()
	fc.apiProvider.UpdateNode(oldObj, newObj)
}

func (fc *MockScheduler) AddConfigMap(obj *v1.ConfigMap) {
	fc.ensureStarted()
	fc.apiProvider.AddConfigMap(obj)
}

func (fc *MockScheduler) DeleteConfigMap(obj *v1.ConfigMap) {
	fc.ensureStarted()
	fc.apiProvider.DeleteConfigMap(obj)
}

func (fc *MockScheduler) UpdateConfigMap(oldObj *v1.ConfigMap, newObj *v1.ConfigMap) {
	fc.ensureStarted()
	fc.apiProvider.UpdateConfigMap(oldObj, newObj)
}

func (fc *MockScheduler) AddPriorityClass(obj *schedv1.PriorityClass) {
	fc.ensureStarted()
	fc.apiProvider.AddPriorityClass(obj)
}

func (fc *MockScheduler) DeletePriorityClass(obj *schedv1.PriorityClass) {
	fc.ensureStarted()
	fc.apiProvider.DeletePriorityClass(obj)
}

func (fc *MockScheduler) UpdatePriorityClass(oldObj *schedv1.PriorityClass, newObj *schedv1.PriorityClass) {
	fc.ensureStarted()
	fc.apiProvider.UpdatePriorityClass(oldObj, newObj)
}

func (fc *MockScheduler) GetActiveNodeCountInCore(partition string) int {
	coreNodes := fc.coreContext.Scheduler.GetClusterContext().GetPartition(partition).GetNodes()
	return len(coreNodes)
}

func (fc *MockScheduler) waitForApplicationStateInCore(appID, partition, expectedState string) error {
	return utils.WaitForCondition(func() bool {
		app := fc.coreContext.Scheduler.GetClusterContext().GetApplication(appID, partition)
		if app == nil {
			log.Log(log.Test).Info("Application not found in the scheduler core", zap.String("appID", appID))
			return false
		}
		current := app.CurrentState()
		if current != expectedState {
			log.Log(log.Test).Info("waiting for app state in core",
				zap.String("expected", expectedState),
				zap.String("actual", current))
			return false
		}
		return true
	}, time.Second, 5*time.Second)
}

func (fc *MockScheduler) waitAndAssertForeignAllocationInCore(partition, allocationID, nodeID string, shouldExist bool) error {
	return utils.WaitForCondition(func() bool {
		node := fc.coreContext.Scheduler.GetClusterContext().GetNode(nodeID, partition)
		if node == nil {
			log.Log(log.Test).Warn("Node not found", zap.String("node ID", nodeID))
			return false
		}
		allocs := node.GetForeignAllocations()
		for _, alloc := range allocs {
			if alloc.GetAllocationKey() == allocationID {
				return shouldExist
			}
		}

		return !shouldExist
	}, time.Second, 5*time.Second)
}

func (fc *MockScheduler) waitAndAssertForeignAllocationResources(partition, allocationID, nodeID string, expected *si.Resource) error {
	return utils.WaitForCondition(func() bool {
		node := fc.coreContext.Scheduler.GetClusterContext().GetNode(nodeID, partition)
		if node == nil {
			log.Log(log.Test).Warn("Node not found", zap.String("node ID", nodeID))
			return false
		}
		allocs := node.GetForeignAllocations()
		for _, alloc := range allocs {
			if alloc.GetAllocationKey() == allocationID {
				current := alloc.GetAllocatedResource().ToProto()
				return common.Equals(expected, current)
			}
		}

		return false
	}, time.Second, 5*time.Second)
}

func (fc *MockScheduler) getApplicationFromCore(appID, partition string) *objects.Application {
	return fc.coreContext.Scheduler.GetClusterContext().GetApplication(appID, partition)
}

func (fc *MockScheduler) GetPodBindStats() *client.BindStats {
	return fc.apiProvider.GetPodBindStats()
}

func (fc *MockScheduler) GetBoundPods(clear bool) []client.BoundPod {
	return fc.apiProvider.GetBoundPods(clear)
}

func (fc *MockScheduler) ensureStarted() {
	if !fc.started.Load() {
		panic("mock scheduler is not started - call start() first")
	}
}
func createUpdateRequestForNewNode(nodeID string, nodeLabels map[string]string, capacity *si.Resource, occupied *si.Resource) *si.NodeRequest {
	// Use node's name as the NodeID, this is because when bind pod to node,
	// name of node is required but uid is optional.
	nodeInfo := &si.NodeInfo{
		NodeID:              nodeID,
		SchedulableResource: capacity,
		Attributes: map[string]string{
			constants.DefaultNodeAttributeHostNameKey: nodeID,
			constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
		},
		Action: si.NodeInfo_CREATE,
	}

	// Add nodeLabels key value to Attributes map
	for k, v := range nodeLabels {
		nodeInfo.Attributes[k] = v
	}

	// Add instanceType to Attributes map
	nodeInfo.Attributes[siCommon.InstanceType] = nodeLabels[conf.GetSchedulerConf().InstanceTypeNodeLabelKey]

	nodes := make([]*si.NodeInfo, 1)
	nodes[0] = nodeInfo
	return &si.NodeRequest{
		Nodes: nodes,
		RmID:  conf.GetSchedulerConf().ClusterID,
	}
}
