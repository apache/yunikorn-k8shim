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
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
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

// TestRecoveryMaxRunningApps starts the scheduler on a cluster that already runs 40 applications under root.parent
// (maxapplications 40) and has 250 pending applications, as after a scheduler restart. It runs the real shim and core,
// and mocks only the Kubernetes API.
//
//nolint:funlen
func TestRecoveryMaxRunningApps(t *testing.T) {
	const maxApps = 40
	leaves := []string{"a", "b", "c", "d", "e"}
	runningPerLeaf := 8
	waitingPerLeaf := 50

	cluster := MockScheduler{}
	cluster.init()
	kubeClient, ok := cluster.apiProvider.GetAPIs().KubeClient.(*client.KubeClientMock)
	assert.Assert(t, ok, "unexpected kube client type")
	cluster.apiProvider.GetAPIs().KubeClient = configKubeClient{KubeClientMock: kubeClient, queues: recoveryMaxAppsConfig}

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
	for _, leaf := range leaves {
		for j := 0; j < runningPerLeaf; j++ {
			id := fmt.Sprintf("running-%s-%02d", leaf, j)
			running = append(running, id)
			addApp(id, leaf, true)
		}
	}
	for _, leaf := range leaves {
		for j := 0; j < waitingPerLeaf; j++ {
			addApp(fmt.Sprintf("waiting-%s-%02d", leaf, j), leaf, false)
		}
	}

	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	partition := cluster.coreContext.Scheduler.GetClusterContext().GetPartition(partitionName)
	countRunning := func() int {
		n := 0
		for _, app := range partition.GetApplications() {
			if strings.HasPrefix(app.GetQueuePath(), "root.parent.") && app.CurrentState() == objects.Running.String() {
				n++
			}
		}
		return n
	}
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
	last, stable := countRunning(), time.Now()
	for time.Since(stable) < 3*time.Second {
		time.Sleep(100 * time.Millisecond)
		if n := countRunning(); n != last {
			last, stable = n, time.Now()
		}
	}

	parent := partition.GetQueue("root.parent").GetPartitionQueueDAOInfo(false).RunningApps
	var childrenSum uint64
	for _, leaf := range leaves {
		childrenSum += partition.GetQueue("root.parent." + leaf).GetPartitionQueueDAOInfo(false).RunningApps
	}
	assert.Check(t, last <= maxApps, "%d apps running under root.parent after recovery, maxapplications is %d", last, maxApps)
	assert.Check(t, parent == childrenSum, "root.parent counts %d running apps but its children count %d", parent, childrenSum)
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
