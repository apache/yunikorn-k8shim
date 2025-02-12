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
	"io"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

var (
	queueConfig = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
          - name: b
          - name: c
          - name: d
          - name: e
`
	profileCpu      = true
	profileHeap     = true
	numNodes        = 5000
	totalPods       = uint64(50_000)
	nodeCpuMilli    = int64(16_000)
	nodeMemGiB      = int64(16)
	nodeNumPods     = int64(110)
	partitionName   = "[mycluster]default"
	cpuProfilePath  = "/tmp/yunikorn-cpu.pprof"
	heapProfilePath = "/tmp/yunikorn-heap.pprof"
)

// Simple performance test which measures the theoretical throughput of the scheduler core.
func BenchmarkSchedulingThroughPut(b *testing.B) {
	if b.N > 1 {
		b.Skip() // safeguard against multiple runs
	}

	log.UpdateLoggingConfig(map[string]string{
		"log.level": "WARN",
	})

	cluster := &MockScheduler{}
	cluster.init()
	assert.NilError(b, cluster.start(), "failed to initialize cluster")
	defer cluster.stop()

	if profileCpu {
		f, err := os.Create(cpuProfilePath)
		assert.NilError(b, err, "could not create file for cpu profile")
		err = pprof.StartCPUProfile(f)
		assert.NilError(b, err, "could not start cpu profiling")
		defer pprof.StopCPUProfile()
	}

	if profileHeap {
		f, err := os.Create(heapProfilePath)
		assert.NilError(b, err, "could not create file for heap profile")

		defer func(w io.Writer) {
			err := pprof.WriteHeapProfile(w)
			assert.NilError(b, err, "could not write heap profile")
		}(f)
	}

	// update config
	err := cluster.updateConfig(queueConfig, map[string]string{
		"log.level": "WARN",
	})
	assert.NilError(b, err, "update config failed")

	// add nodes to the scheduler & wait until they're registered in the core
	for i := 0; i < numNodes; i++ {
		addNode(cluster, "test.host."+strconv.Itoa(i))
	}
	err = wait.PollUntilContextTimeout(context.Background(), time.Second, time.Second*60, true, func(ctx context.Context) (done bool, err error) {
		return cluster.GetActiveNodeCountInCore(partitionName) == numNodes, nil
	})
	assert.NilError(b, err, "node initialization did not finish in time")

	// add pods, begin collecting allocation metrics & wait until all pods are bound
	pods := addPodsToCluster(cluster)
	collector := &metricsCollector{}
	go collector.collectData()

	// update bound pods to Running then to Succeeded
	ps := newPodStatusUpdater(cluster, pods)
	go ps.updatePodStatus()
	defer ps.stop()

	// await binding of pods
	err = wait.PollUntilContextTimeout(context.Background(), time.Second, time.Second*60, true, func(ctx context.Context) (done bool, err error) {
		c := ps.getCompletedPodsCount()
		fmt.Printf("Number of completed pods: %d\n", c)
		return c == totalPods, nil
	})
	assert.NilError(b, err, "scheduling did not finish in time")

	stat := cluster.GetPodBindStats()
	if stat != nil {
		diff := stat.Last.Sub(stat.First)
		fmt.Printf("Overall throughput: %.0f allocations/s\n", float64(totalPods)/diff.Seconds())
	}
	fmt.Println("Container allocation throughput based on metrics")
	for _, d := range collector.getData() {
		if d != 0 {
			fmt.Printf("%.0f container allocations/s\n", d)
		}
	}
}

func addPodsToCluster(cluster *MockScheduler) map[string]*v1.Pod {
	podsMap := make(map[string]*v1.Pod, totalPods)

	var queues [5]string
	queues[0] = "root.a"
	queues[1] = "root.b"
	queues[2] = "root.c"
	queues[3] = "root.d"
	queues[4] = "root.e"

	// make sure that total number of pods == totalPods
	for _, queue := range queues {
		pods := getTestPods(80, 125, queue)
		for _, pod := range pods {
			cluster.AddPod(pod)
			podsMap[pod.Name] = pod
		}
	}

	return podsMap
}

type metricsCollector struct {
	podsPerSec []float64
	locking.Mutex
}

func (m *metricsCollector) collectData() {
	prev := float64(-1)

	for {
		time.Sleep(time.Second)

		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			panic(err)
		}

		for _, mf := range mfs {
			if strings.EqualFold(mf.GetName(), "yunikorn_scheduler_container_allocation_attempt_total") {
				for _, metric := range mf.Metric {
					for _, label := range metric.Label {
						if *label.Name == "state" && *label.Value == "allocated" {
							if prev == -1 {
								prev = *metric.Counter.Value
								continue
							}
							current := *metric.Counter.Value
							m.update(current - prev)
							prev = current
						}
					}
				}
			}
		}
	}
}

func (m *metricsCollector) update(data float64) {
	m.Lock()
	defer m.Unlock()
	m.podsPerSec = append(m.podsPerSec, data)
}

func (m *metricsCollector) getData() []float64 {
	m.Lock()
	defer m.Unlock()
	var data []float64
	data = append(data, m.podsPerSec...)
	return data
}

func newPodStatusUpdater(cluster *MockScheduler, pods map[string]*v1.Pod) *podStatusUpdater {
	ps := &podStatusUpdater{
		stopCh:  make(chan struct{}),
		cluster: cluster,
	}
	podsCopy := make(map[string]*v1.Pod, len(pods))
	for name, pod := range pods {
		podsCopy[name] = pod
	}
	ps.pods = podsCopy

	return ps
}

// updates the status of bound pods to Running then to Completed
type podStatusUpdater struct {
	completedPods atomic.Uint64
	stopCh        chan struct{}
	cluster       *MockScheduler
	pods          map[string]*v1.Pod
}

func (p *podStatusUpdater) updatePodStatus() {
	for {
		select {
		case <-time.After(time.Second):
			boundPods := p.cluster.GetBoundPods(true)
			for _, boundPod := range boundPods {
				podName := boundPod.Pod

				if pod, ok := p.pods[podName]; ok {
					podRunning := pod.DeepCopy()
					// update to Running
					podRunning.Status.Phase = v1.PodRunning
					p.cluster.UpdatePod(pod, podRunning)

					podSucceeded := pod.DeepCopy()
					// update to Completed
					podSucceeded.Status.Phase = v1.PodSucceeded
					p.cluster.UpdatePod(podRunning, podSucceeded)

					p.completedPods.Add(1)
					continue
				}
				panic("BUG: test pod not found: " + podName)
			}
		case <-p.stopCh:
			return
		}
	}
}

func (p *podStatusUpdater) getCompletedPodsCount() uint64 {
	return p.completedPods.Load()
}

func (p *podStatusUpdater) stop() {
	close(p.stopCh)
}

func getTestPods(noApps, noTasksPerApp int, queue string) []*v1.Pod {
	podCount := noApps * noTasksPerApp
	pods := make([]*v1.Pod, 0, podCount)
	resources := make(map[v1.ResourceName]resource.Quantity)
	resources[v1.ResourceCPU] = *resource.NewMilliQuantity(10, resource.DecimalSI)
	resources[v1.ResourceMemory] = *resource.NewScaledQuantity(1, resource.Mega)

	for i := 0; i < noApps; i++ {
		appId := "app000" + strconv.Itoa(i) + "-" + strconv.FormatInt(time.Now().UnixMilli(), 10)
		for j := 0; j < noTasksPerApp; j++ {
			taskName := "task000" + strconv.Itoa(j)
			podName := appId + "-" + taskName
			pod := &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
					UID:  types.UID("UID-" + podName),
					Annotations: map[string]string{
						constants.AnnotationApplicationID: appId,
						constants.AnnotationQueueName:     queue,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "container-01",
							Resources: v1.ResourceRequirements{
								Requests: resources,
							},
						},
					},
					SchedulerName: constants.SchedulerName,
				},
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			}
			pods = append(pods, pod)
		}
	}

	return pods
}

func addNode(cluster *MockScheduler, name string) {
	node := &v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID("UUID-" + name),
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(nodeCpuMilli, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewScaledQuantity(nodeMemGiB, resource.Giga),
				v1.ResourcePods:   *resource.NewScaledQuantity(nodeNumPods, resource.Scale(0)),
			},
		},
	}

	cluster.AddNode(node)
}
