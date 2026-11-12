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
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const recoveryMaxAppsConfig = `
partitions:
  - name: default
    nodesortpolicy:
      type: binpacking
    placementrules:
      - name: provided
        create: false
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: parent
            submitacl: "*"
            maxapplications: 40
            queues:
              - name: a
                maxapplications: 40
              - name: b
                maxapplications: 40
              - name: c
                maxapplications: 40
              - name: d
                maxapplications: 40
              - name: e
                maxapplications: 40
`

// configKubeClient serves the queue config at registration, because the mock returns no config maps.
type configKubeClient struct {
	*client.KubeClientMock
	queues string
}

func (c configKubeClient) GetConfigMap(_ string, name string) (*v1.ConfigMap, error) {
	if name != constants.ConfigMapName {
		return nil, nil
	}
	return &v1.ConfigMap{Data: map[string]string{"queues.yaml": c.queues}}, nil
}

var recoveryLeaves = []string{"a", "b", "c", "d", "e"}

// startRecoveryLimitCluster starts the scheduler on a cluster that already runs 40 applications of one pod each under
// root.parent and has 250 pending applications, as after a scheduler restart, and waits until the core runs the 40.
// It runs the real shim and core, and mocks only the Kubernetes API.
func startRecoveryLimitCluster(t *testing.T, queues string) *MockScheduler {
	cluster := &MockScheduler{}
	cluster.init()
	kubeClient, ok := cluster.apiProvider.GetAPIs().KubeClient.(*client.KubeClientMock)
	assert.Assert(t, ok, "unexpected kube client type")
	cluster.apiProvider.GetAPIs().KubeClient = configKubeClient{KubeClientMock: kubeClient, queues: queues}

	cluster.AddNodeToLister(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: types.UID("UUID-node-1")},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(10000, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewScaledQuantity(10000, resource.Giga),
				v1.ResourcePods:   *resource.NewQuantity(10000, resource.DecimalSI),
			},
		},
	})
	res := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1000000).
		AddResource(siCommon.CPU, 1000).
		Build()
	created := time.Now().Add(-time.Hour)
	var running []string
	addApp := func(appID, leaf string, bound bool) {
		pod := createTestPod("root.parent."+leaf, appID, appID+"-task", res)
		pod.CreationTimestamp = metav1.NewTime(created)
		created = created.Add(time.Second)
		if bound {
			pod.Spec.NodeName = "node-1"
			pod.Status.Phase = v1.PodRunning
		}
		cluster.AddPodToLister(pod)
	}
	for _, leaf := range recoveryLeaves {
		for j := 0; j < 8; j++ {
			id := fmt.Sprintf("running-%s-%02d", leaf, j)
			running = append(running, id)
			addApp(id, leaf, true)
		}
	}
	for _, leaf := range recoveryLeaves {
		for j := 0; j < 50; j++ {
			addApp(fmt.Sprintf("waiting-%s-%02d", leaf, j), leaf, false)
		}
	}

	assert.NilError(t, cluster.start(), "failed to start cluster")
	t.Cleanup(cluster.stop)

	partition := cluster.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName)
	err := utils.WaitForCondition(func() bool {
		for _, id := range running {
			app := partition.GetApplication(id)
			if app == nil || app.CurrentState() != objects.Running.String() {
				return false
			}
		}
		return true
	}, 10*time.Millisecond, 30*time.Second)
	assert.NilError(t, err, "recovered apps did not reach Running in the core")
	return cluster
}

// settle returns what measure reports once it has not changed for 3 seconds.
func settle(measure func() int64) int64 {
	last, stable := measure(), time.Now()
	for time.Since(stable) < 3*time.Second {
		time.Sleep(100 * time.Millisecond)
		if n := measure(); n != last {
			last, stable = n, time.Now()
		}
	}
	return last
}

// TestRecoveryMaxRunningApps checks that the core runs no more than maxapplications under root.parent after a restart.
func TestRecoveryMaxRunningApps(t *testing.T) {
	const maxApps = 40
	cluster := startRecoveryLimitCluster(t, recoveryMaxAppsConfig)

	partition := cluster.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName)
	last := settle(func() int64 {
		var n int64
		for _, app := range partition.GetApplications() {
			if strings.HasPrefix(app.GetQueuePath(), "root.parent.") && app.CurrentState() == objects.Running.String() {
				n++
			}
		}
		return n
	})

	parent := partition.GetQueue("root.parent").GetPartitionQueueDAOInfo(false).RunningApps
	var childrenSum uint64
	for _, leaf := range recoveryLeaves {
		childrenSum += partition.GetQueue("root.parent." + leaf).GetPartitionQueueDAOInfo(false).RunningApps
	}
	assert.Check(t, last <= maxApps, "%d apps running under root.parent after recovery, maxapplications is %d", last, maxApps)
	assert.Check(t, parent == childrenSum, "root.parent counts %d running apps but its children count %d", parent, childrenSum)
}

const recoveryMaxResConfig = `
partitions:
  - name: default
    nodesortpolicy:
      type: binpacking
    preemption:
      enabled: false
    placementrules:
      - name: provided
        create: false
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: parent
            submitacl: "*"
            resources:
              max:
                memory: 40M
            queues:
              - name: a
              - name: b
              - name: c
              - name: d
              - name: e
`

// TestRecoveryMaxResources checks that root.parent uses no more than its maximum resources after a restart: the 40
// running pods of 1M fill its 40M. Preemption is off because the waiting pods are an hour old, so the core would try to
// preempt for them at once, and its preemption path trips the deadlock detector (YUNIKORN-3567).
func TestRecoveryMaxResources(t *testing.T) {
	const maxMemory = 40000000
	cluster := startRecoveryLimitCluster(t, recoveryMaxResConfig)

	queue := cluster.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName).GetQueue("root.parent")
	used := settle(func() int64 {
		return int64(queue.GetAllocatedResource().Resources[siCommon.Memory])
	})
	assert.Check(t, used <= maxMemory, "root.parent uses %d memory after recovery, its maximum is %d", used, maxMemory)
}

// recordAPI records the applications, allocations and node updates the shim sends to the core, in order.
type recordAPI struct {
	api.SchedulerAPI
	lock  sync.Mutex
	calls []string
}

func (r *recordAPI) add(call string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordAPI) get() []string {
	r.lock.Lock()
	defer r.lock.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recordAPI) UpdateApplication(req *si.ApplicationRequest) error {
	for _, app := range req.New {
		r.add("app " + app.ApplicationID)
	}
	return r.SchedulerAPI.UpdateApplication(req)
}

func (r *recordAPI) UpdateAllocation(req *si.AllocationRequest) error {
	for _, alloc := range req.Allocations {
		r.add(fmt.Sprintf("alloc %s node=%q", alloc.AllocationKey, alloc.NodeID))
	}
	return r.SchedulerAPI.UpdateAllocation(req)
}

func (r *recordAPI) UpdateNode(req *si.NodeRequest) error {
	for _, node := range req.Nodes {
		r.add(fmt.Sprintf("node %s %s", node.NodeID, node.Action))
	}
	return r.SchedulerAPI.UpdateNode(req)
}

// TestRecoveryCoreStateAtInit runs Run() up to the end of InitializeState(), without the scheduling loop, on a cluster
// with two running applications: one with a single pod and one with three. The core must have both applications and
// all four allocations before any node is enabled.
func TestRecoveryCoreStateAtInit(t *testing.T) {
	cluster := MockScheduler{}
	cluster.init()
	kubeClient, ok := cluster.apiProvider.GetAPIs().KubeClient.(*client.KubeClientMock)
	assert.Assert(t, ok, "unexpected kube client type")
	cluster.apiProvider.GetAPIs().KubeClient = configKubeClient{KubeClientMock: kubeClient, queues: recoveryGangConfig}
	rec := &recordAPI{SchedulerAPI: cluster.apiProvider.GetAPIs().SchedulerAPI}
	cluster.apiProvider.GetAPIs().SchedulerAPI = rec

	cluster.AddNodeToLister(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: types.UID("UUID-node-1")},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(100, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewScaledQuantity(100, resource.Giga),
				v1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
	})
	res := common.NewResourceBuilder().AddResource(siCommon.Memory, 1000000).AddResource(siCommon.CPU, 1000).Build()
	created := time.Now().Add(-time.Hour)
	running := map[string][]string{
		"app-one":   {"one-0"},
		"app-three": {"three-0", "three-1", "three-2"},
	}
	for _, appID := range []string{"app-one", "app-three"} {
		for _, name := range running[appID] {
			pod := createTestPod("root.a", appID, name, res)
			pod.CreationTimestamp = metav1.NewTime(created)
			created = created.Add(time.Second)
			pod.Spec.NodeName = "node-1"
			pod.Status.Phase = v1.PodRunning
			cluster.AddPodToLister(pod)
		}
	}

	// Run() without doScheduling()
	cluster.apiProvider.RunEventHandler()
	dispatcher.Start()
	cluster.scheduler.phManager.Start()
	cluster.apiProvider.Start()
	defer cluster.stop()
	assert.NilError(t, cluster.scheduler.registerShimLayer())
	assert.NilError(t, cluster.scheduler.initSchedulerState())

	calls := rec.get()
	firstEnable := -1
	for i, call := range calls {
		if call == "node node-1 DRAIN_TO_SCHEDULABLE" {
			firstEnable = i
			break
		}
	}
	assert.Assert(t, firstEnable >= 0, "node-1 was never enabled: %v", calls)
	before := strings.Join(calls[:firstEnable], "\n")
	for appID, pods := range running {
		assert.Check(t, strings.Contains(before, "app "+appID), "%s was not sent before node-1 was enabled: %v", appID, calls)
		for _, pod := range pods {
			assert.Check(t, strings.Contains(before, fmt.Sprintf("alloc %s node=%q", pod, "node-1")),
				"allocation %s was not sent before node-1 was enabled: %v", pod, calls)
		}
	}

	// the core handles node events apart from application and allocation events, so check what it holds as well
	partition := cluster.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName)
	err := utils.WaitForCondition(func() bool {
		node := partition.GetNode("node-1")
		return node != nil && node.IsSchedulable()
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err, "core never enabled node-1")
	for appID := range running {
		assert.Check(t, partition.GetApplication(appID) != nil, "%s is not in the core when node-1 is schedulable", appID)
	}
	assert.Check(t, partition.GetTotalAllocationCount() == 4, "core holds %d of 4 allocations when node-1 is schedulable", partition.GetTotalAllocationCount())

	cluster.scheduler.doScheduling()
	err = utils.WaitForCondition(func() bool {
		for appID := range running {
			app := partition.GetApplication(appID)
			if app == nil || app.CurrentState() != objects.Running.String() {
				return false
			}
		}
		return partition.GetTotalAllocationCount() == 4
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err, "recovered applications did not reach Running in the core")
}

const recoveryGangConfig = `
partitions:
  - name: default
    placementrules:
      - name: provided
        create: false
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
`

// podsClient records the pods the shim sets to Failed or deletes.
type podsClient struct {
	configKubeClient
	lock    sync.Mutex
	failed  []string
	deleted []string
}

func (c *podsClient) UpdateStatus(ctx context.Context, pod *v1.Pod) (*v1.Pod, error) {
	if pod.Status.Phase == v1.PodFailed {
		c.lock.Lock()
		c.failed = append(c.failed, pod.Name)
		c.lock.Unlock()
	}
	return c.configKubeClient.UpdateStatus(ctx, pod)
}

func (c *podsClient) Delete(ctx context.Context, pod *v1.Pod) error {
	c.lock.Lock()
	c.deleted = append(c.deleted, pod.Name)
	c.lock.Unlock()
	return c.configKubeClient.Delete(ctx, pod)
}

func (c *podsClient) get() ([]string, []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return append([]string(nil), c.failed...), append([]string(nil), c.deleted...)
}

// startGangCluster starts the scheduler on a cluster that runs a gang application: its driver and one executor, and
// three placeholders that were not replaced.
func startGangCluster(t *testing.T, age time.Duration, params string, pending bool) (*MockScheduler, *podsClient) {
	cluster := &MockScheduler{}
	cluster.init()
	kubeClient, ok := cluster.apiProvider.GetAPIs().KubeClient.(*client.KubeClientMock)
	assert.Assert(t, ok, "unexpected kube client type")
	pods := &podsClient{configKubeClient: configKubeClient{KubeClientMock: kubeClient, queues: recoveryGangConfig}}
	cluster.apiProvider.GetAPIs().KubeClient = pods
	cluster.AddNodeToLister(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", UID: types.UID("UUID-node-1")},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("10"),
				v1.ResourceMemory: resource.MustParse("10G"),
				v1.ResourcePods:   resource.MustParse("10"),
			},
		},
	})
	created := time.Now().Add(-age)
	controller := false
	names := []string{"driver", "exec-0", "ph-exec-1", "ph-exec-2", "ph-exec-3"}
	if pending {
		names = append(names, "ph-pending")
	}
	for i, name := range names {
		pod := createTestPod("root.a", "gang", name, common.NewResourceBuilder().Build())
		pod.Spec.Containers[0].Resources.Requests = v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("1M"),
		}
		pod.Annotations = map[string]string{
			constants.AnnotationTaskGroups:            `[{"name":"group","minMember":4,"minResource":{"cpu":"1","memory":"1M"}}]`,
			constants.AnnotationTaskGroupName:         "group",
			constants.AnnotationSchedulingPolicyParam: params,
		}
		if strings.HasPrefix(name, "ph-") {
			pod.Annotations[constants.AnnotationPlaceholderFlag] = constants.True
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "driver", UID: "driver", Controller: &controller}}
		}
		pod.CreationTimestamp = metav1.NewTime(created.Add(time.Duration(i) * time.Second))
		pod.Status.Phase = v1.PodPending
		if name != "ph-pending" {
			pod.Spec.NodeName = "node-1"
			pod.Status.Phase = v1.PodRunning
		}
		cluster.AddPodToLister(pod)
	}
	assert.NilError(t, cluster.start(), "failed to start cluster")
	return cluster, pods
}

// waitForDeleted waits for the shim to delete exactly the given pods, in any order, and checks that it set no pod to
// Failed.
func waitForDeleted(t *testing.T, pods *podsClient, names ...string) {
	err := utils.WaitForCondition(func() bool {
		_, deleted := pods.get()
		return len(deleted) >= len(names)
	}, 100*time.Millisecond, 10*time.Second)
	assert.NilError(t, err, "placeholders not deleted")
	failed, deleted := pods.get()
	sort.Strings(deleted)
	assert.DeepEqual(t, deleted, names)
	assert.Assert(t, len(failed) == 0, "pods set to Failed: %v", failed)
}

// TestRecoveryGangPlaceholderReleased checks that the core releases the placeholders when their timeout passes after
// the restart.
func TestRecoveryGangPlaceholderReleased(t *testing.T) {
	cluster, pods := startGangCluster(t, 0, "placeholderTimeoutInSeconds=3 gangSchedulingStyle=Hard", false)
	defer cluster.stop()

	waitForDeleted(t, pods, "ph-exec-1", "ph-exec-2", "ph-exec-3")
	assert.Equal(t, cluster.getApplicationFromCore("gang", partitionName).CurrentState(), objects.Running.String())
}

// TestRecoveryGangTimeoutPassed checks that a placeholder timeout that passed before the restart releases every
// placeholder without failing the application, with an explicit and with the default timeout. The timer can fire
// before the core has the other placeholders.
func TestRecoveryGangTimeoutPassed(t *testing.T) {
	for _, params := range []string{"placeholderTimeoutInSeconds=900 gangSchedulingStyle=Hard", "gangSchedulingStyle=Hard"} {
		t.Run(params, func(t *testing.T) {
			cluster, pods := startGangCluster(t, time.Hour, params, false)
			defer cluster.stop()

			waitForDeleted(t, pods, "ph-exec-1", "ph-exec-2", "ph-exec-3")
			assert.Equal(t, cluster.getApplicationFromCore("gang", partitionName).CurrentState(), objects.Running.String())
		})
	}
}

// TestRecoveryPendingPlaceholderDeleted checks that a placeholder that was never scheduled is deleted once the
// recovered application runs.
func TestRecoveryPendingPlaceholderDeleted(t *testing.T) {
	cluster, pods := startGangCluster(t, 0, "placeholderTimeoutInSeconds=900 gangSchedulingStyle=Hard", true)
	defer cluster.stop()

	waitForDeleted(t, pods, "ph-pending")
	assert.Equal(t, cluster.getApplicationFromCore("gang", partitionName).CurrentState(), objects.Running.String())
}
