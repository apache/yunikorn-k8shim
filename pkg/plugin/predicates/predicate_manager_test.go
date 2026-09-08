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

package predicates

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"go.uber.org/zap"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime2 "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	apiConfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodevolumelimits"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/podtopologyspread"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/util/taints"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/support"
)

var (
	extendedResourceA = v1.ResourceName("example.com/aaa")
	hugePageResourceA = v1helper.HugePageResourceName(resource.MustParse("2Mi"))
)

const (
	mockPreFilterPlugin = "mock-prefilter-plugin"
	mockFilterPlugin    = "mock-filter-plugin"
	injectReason        = "injected status"
	injectFilterReason  = "injected filter status"
)

type injectedResult struct {
	PreFilterResult *fwk.PreFilterResult `json:"preFilterResult,omitempty"`
	PreFilterStatus int                  `json:"preFilterStatus,omitempty"`
	FilterStatus    int                  `json:"filterStatus,omitempty"`
}

// MockPreFilterPlugin implements PreFilter interface.
type MockPreFilterPlugin struct {
	name string
	inj  injectedResult
}

func (pl *MockPreFilterPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

func (pl *MockPreFilterPlugin) Name() string {
	return pl.name
}

func (pl *MockPreFilterPlugin) PreFilter(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	return pl.inj.PreFilterResult, fwk.NewStatus(fwk.Code(pl.inj.PreFilterStatus), injectReason)
}

// MockFilterPlugin implements Filter interface.
type MockFilterPlugin struct {
	name string
	inj  injectedResult
}

func (pl *MockFilterPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Code(pl.inj.FilterStatus), injectFilterReason)
}

func (pl *MockFilterPlugin) Name() string {
	return pl.name
}

func TestPreemptionFilterWithVictims(t *testing.T) {
	ep := enabledPlugins(noderesources.Name)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	emptyNode := framework.NewNodeInfo()
	emptyNode.SetNode(&v1.Node{})

	pod := newResourcePod(framework.Resource{MilliCPU: 500, Memory: 5000000})
	pod.Name = "smallpod"
	pod.UID = "smallpod"
	node := framework.NewNodeInfo()
	node.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node0", UID: "node0"},
		Status: v1.NodeStatus{
			Capacity:    makeResources(1000, 100000000, 10, 0, 0, 0),
			Allocatable: makeResources(1000, 100000000, 10, 0, 0, 0),
		},
	})
	victims := []*v1.Pod{
		newResourcePod(framework.Resource{MilliCPU: 100, Memory: 1000000}),
		newResourcePod(framework.Resource{MilliCPU: 100, Memory: 1000000}),
		newResourcePod(framework.Resource{MilliCPU: 300, Memory: 3000000}),
		newResourcePod(framework.Resource{MilliCPU: 500, Memory: 5000000}),
	}
	victims[0].Name = "pod0"
	victims[1].Name = "pod1"
	victims[2].Name = "pod2"
	victims[3].Name = "pod3"
	victims[0].UID = "pod0"
	victims[1].UID = "pod1"
	victims[2].UID = "pod2"
	victims[3].UID = "pod3"
	node.AddPod(victims[0])
	node.AddPod(victims[1])
	node.AddPod(victims[2])
	node.AddPod(victims[3])

	// try again, but with too many resources requested
	largePod := newResourcePod(framework.Resource{MilliCPU: 1500, Memory: 15000000})
	largePod.Name = "largepod"
	largePod.UID = "largepod"

	tests := []struct {
		name          string
		pod           *v1.Pod
		node          *framework.NodeInfo
		victims       []*v1.Pod
		expectedIndex int
	}{
		{"invalid pod and no victims", &v1.Pod{}, emptyNode, make([]*v1.Pod, 0), -1},
		{"valid pod with available victims", pod, node, victims, 2},
		{"valid pod with not suitable victims", largePod, node, victims, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, cycleState, err := predicateManager.PreFilter(tt.pod, true)
			assert.NilError(t, err)
			index := predicateManager.PreemptionFilter(tt.pod, tt.node, cycleState, tt.victims, 1)
			assert.Equal(t, index, tt.expectedIndex, "wrong victim index")
		})
	}
}

func TestEventsToRegister(t *testing.T) {
	ep := enabledPlugins(nodename.Name, interpodaffinity.Name, podtopologyspread.Name)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	var queueingHintFn fwk.QueueingHintFn = func(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
		// illegal sentinel to ensure we called the correct function
		return -1, nil
	}
	events := predicateManager.EventsToRegister(queueingHintFn)
	assert.Equal(t, events[0].Event.Resource, fwk.Node, "wrong resource (0)")
	assert.Equal(t, events[0].Event.ActionType, fwk.Add|fwk.Delete|fwk.UpdateNodeLabel|fwk.UpdateNodeTaint, "wrong action type (0)")
	fn0, err := events[0].QueueingHintFn(klog.NewKlogr(), nil, "", "")
	assert.NilError(t, err)
	assert.Equal(t, int(fn0), -1, "wrong fn (0)")
	assert.Equal(t, events[1].Event.Resource, fwk.Pod, "wrong resource (1)")
	assert.Equal(t, events[1].Event.ActionType, fwk.Add|fwk.Delete|fwk.UpdatePodLabel|fwk.UpdatePodToleration, "wrong action type (1)")
	fn1, err := events[1].QueueingHintFn(klog.NewKlogr(), nil, "", "")
	assert.NilError(t, err)
	assert.Equal(t, int(fn1), -1, "wrong fn (1)")
}

func TestPodFitsHost(t *testing.T) {
	ep := enabledPlugins(nodename.Name)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)
	tests := []struct {
		pod  *v1.Pod
		node *v1.Node
		fits bool
		name string
	}{
		{
			pod:  &v1.Pod{},
			node: &v1.Node{},
			fits: true,
			name: "no host specified",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeName: "foo",
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
			},
			fits: true,
			name: "host matches",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeName: "bar",
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
			},
			fits: false,
			name: "host doesn't match",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(test.node)
			plugin, err := predicateManager.Filter(test.pod, nodeInfo, framework.NewCycleState(), true)
			if (err == nil) != test.fits {
				t.Errorf("%s expected fit state '%t' did not match real state and err = %v, plugin = %v", test.name, test.fits, err, plugin)
			}
		})
	}
}

func newPod(host string, hostPortInfos ...string) *v1.Pod {
	var networkPorts []v1.ContainerPort
	for _, portInfo := range hostPortInfos {
		split := strings.Split(portInfo, "/")
		//nolint:errcheck
		hostPort, _ := strconv.ParseInt(split[2], 10, 32)

		networkPorts = append(networkPorts, v1.ContainerPort{
			HostIP:   split[1],
			HostPort: int32(hostPort),
			Protocol: v1.Protocol(split[0]),
		})
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: host,
			Containers: []v1.Container{
				{
					Ports: networkPorts,
				},
			},
		},
	}
}

func TestPodFitsHostPorts(t *testing.T) {
	ep := enabledPlugins(nodeports.Name)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	tests := []struct {
		pod      *v1.Pod
		nodeInfo *framework.NodeInfo
		fits     bool
		name     string
	}{
		{
			pod:      &v1.Pod{},
			nodeInfo: framework.NewNodeInfo(),
			fits:     true,
			name:     "nothing running",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/9090")),
			fits: true,
			name: "other port",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/8080")),
			fits: false,
			name: "same udp port",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8080")),
			fits: false,
			name: "same tcp port",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.2/8080")),
			fits: true,
			name: "different host ip",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8080")),
			fits: true,
			name: "different protocol",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8000", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/8080")),
			fits: false,
			name: "second udp port conflict",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8001", "UDP/127.0.0.1/8080"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001", "UDP/127.0.0.1/8081")),
			fits: false,
			name: "first tcp port conflict",
		},
		{
			pod: newPod("m1", "TCP/0.0.0.0/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001")),
			fits: false,
			name: "first tcp port conflict due to 0.0.0.0 hostIP",
		},
		{
			pod: newPod("m1", "TCP/10.0.10.10/8001", "TCP/0.0.0.0/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001")),
			fits: false,
			name: "TCP hostPort conflict due to 0.0.0.0 hostIP",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			fits: false,
			name: "second tcp port conflict to 0.0.0.0 hostIP",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			fits: true,
			name: "second different protocol",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8001"),
			nodeInfo: framework.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001", "UDP/0.0.0.0/8001")),
			fits: false,
			name: "UDP hostPort conflict due to 0.0.0.0 hostIP",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, cycleState, err := predicateManager.PreFilter(test.pod, true)
			assert.NilError(t, err)
			assert.Assert(t, cycleState != nil)
			plugin, err := predicateManager.Filter(test.pod, test.nodeInfo, cycleState, true)
			if (err == nil) != test.fits {
				t.Errorf("%s expected fit state '%t' did not match real state and err = %v, plugin = %v", test.name, test.fits, err, plugin)
			}
		})
	}
}

//nolint:funlen
func TestPodFitsSelector(t *testing.T) {
	ep := enabledPlugins(nodeports.Name, nodeaffinity.Name)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	tests := []struct {
		pod      *v1.Pod
		labels   map[string]string
		nodeName string
		fits     bool
		name     string
	}{
		{
			pod:  &v1.Pod{},
			fits: true,
			name: "no selector",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
			},
			fits: false,
			name: "missing labels",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: true,
			name: "same labels",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
				"baz": "blah",
			},
			fits: true,
			name: "node labels are superset",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
						"baz": "blah",
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: false,
			name: "node labels are subset",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: true,
			name: "Pod with matchExpressions using In operator that matches the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "kernel-version",
												Operator: v1.NodeSelectorOpGt,
												Values:   []string{"0204"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				// We use two digit to denote major version and two digit for minor version.
				"kernel-version": "0206",
			},
			fits: true,
			name: "Pod with matchExpressions using Gt operator that matches the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "mem-type",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"DDR", "DDR2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"mem-type": "DDR3",
			},
			fits: true,
			name: "Pod with matchExpressions using NotIn operator that matches the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "GPU",
												Operator: v1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"GPU": "NVIDIA-GRID-K1",
			},
			fits: true,
			name: "Pod with matchExpressions using Exists operator that matches the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"value1", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: false,
			name: "Pod with affinity that don't match node's labels won't schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: nil,
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: false,
			name: "Pod with a nil []NodeSelectorTerm in affinity, can't match the node's labels and won't schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: false,
			name: "Pod with an empty []NodeSelectorTerm in affinity, can't match the node's labels and won't schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: false,
			name: "Pod with empty MatchExpressions is not a valid value will match no objects and won't schedule onto the node",
		},
		{
			pod: &v1.Pod{},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: true,
			name: "Pod with no Affinity will schedule onto a node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: nil,
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: true,
			name: "Pod with Affinity but nil NodeSelector will schedule onto a node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "GPU",
												Operator: v1.NodeSelectorOpExists,
											}, {
												Key:      "GPU",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"AMD", "INTER"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"GPU": "NVIDIA-GRID-K1",
			},
			fits: true,
			name: "Pod with multiple matchExpressions ANDed that matches the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "GPU",
												Operator: v1.NodeSelectorOpExists,
											}, {
												Key:      "GPU",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"AMD", "INTER"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"GPU": "NVIDIA-GRID-K1",
			},
			fits: false,
			name: "Pod with multiple matchExpressions ANDed that doesn't match the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar", "value2"},
											},
										},
									},
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "diffkey",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"wrong", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: true,
			name: "Pod with multiple NodeSelectorTerms ORed in affinity, matches the node's labels and will schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: true,
			name: "Pod with an Affinity and a PodSpec.NodeSelector(the old thing that we are deprecating) " +
				"both are satisfied, will schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"foo": "bar",
					},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpExists,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "barrrrrr",
			},
			fits: false,
			name: "Pod with an Affinity matches node's labels but the PodSpec.NodeSelector(the old thing that we are deprecating) " +
				"is not satisfied, won't schedule onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpNotIn,
												Values:   []string{"invalid value: ___@#$%^"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			labels: map[string]string{
				"foo": "bar",
			},
			fits: false,
			name: "Pod with an invalid value in Affinity term won't be scheduled onto the node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_1",
			fits:     true,
			name:     "Pod with matchFields using In operator that matches the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			fits:     false,
			name:     "Pod with matchFields using In operator that does not match the existing node",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			labels:   map[string]string{"foo": "bar"},
			fits:     true,
			name:     "Pod with two terms: matchFields does not match, but matchExpressions matches",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			labels:   map[string]string{"foo": "bar"},
			fits:     false,
			name:     "Pod with one term: matchFields does not match, but matchExpressions matches",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_1",
			labels:   map[string]string{"foo": "bar"},
			fits:     true,
			name:     "Pod with one term: both matchFields and matchExpressions match",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchFields: []v1.NodeSelectorRequirement{
											{
												Key:      metav1.ObjectNameField,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"node_1"},
											},
										},
									},
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "foo",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"not-match-to-bar"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			nodeName: "node_2",
			labels:   map[string]string{"foo": "bar"},
			fits:     false,
			name:     "Pod with two terms: both matchFields and matchExpressions do not match",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := v1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   test.nodeName,
				Labels: test.labels,
			}}
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(&node)
			_, cycleState, err := predicateManager.PreFilter(test.pod, true)
			assert.NilError(t, err)
			assert.Assert(t, cycleState != nil)
			plugin, err := predicateManager.Filter(test.pod, nodeInfo, cycleState, true)
			if (err == nil) != test.fits {
				t.Errorf("%s expected fit state '%t' did not match real state and err = %v, plugin = %v", test.name, test.fits, err, plugin)
			}
		})
	}
}

func newResourcePod(usage ...framework.Resource) *v1.Pod {
	var containers []v1.Container
	for _, req := range usage {
		rl := v1.ResourceList{}
		if req.MilliCPU > 0 {
			rl[v1.ResourceCPU] = *resource.NewMilliQuantity(req.MilliCPU, resource.DecimalSI)
		}
		if req.Memory > 0 {
			rl[v1.ResourceMemory] = *resource.NewQuantity(req.Memory, resource.DecimalSI)
		}
		if req.EphemeralStorage > 0 {
			rl[v1.ResourceEphemeralStorage] = *resource.NewQuantity(req.EphemeralStorage, resource.DecimalSI)
		}
		if req.AllowedPodNumber > 0 {
			rl[v1.ResourcePods] = *resource.NewQuantity(int64(req.AllowedPodNumber), resource.DecimalSI)
		}
		for k, v := range req.ScalarResources {
			rl[k] = *resource.NewQuantity(v, resource.DecimalSI)
		}
		containers = append(containers, v1.Container{
			Resources: v1.ResourceRequirements{Requests: rl},
		})
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

func makeResources(milliCPU, memory, pods, extendedA, storage, hugePageA int64) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:              *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
		v1.ResourceMemory:           *resource.NewQuantity(memory, resource.BinarySI),
		v1.ResourcePods:             *resource.NewQuantity(pods, resource.DecimalSI),
		extendedResourceA:           *resource.NewQuantity(extendedA, resource.DecimalSI),
		v1.ResourceEphemeralStorage: *resource.NewQuantity(storage, resource.BinarySI),
		hugePageResourceA:           *resource.NewQuantity(hugePageA, resource.BinarySI),
	}
}

func newPodWithPort(hostPorts ...int) *v1.Pod {
	var networkPorts []v1.ContainerPort
	for _, port := range hostPorts {
		networkPorts = append(networkPorts, v1.ContainerPort{HostPort: int32(port)}) // nolint: gosec
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Ports: networkPorts,
				},
			},
		},
	}
}

func TestEnableOptionalKubernetesFeatureGates(t *testing.T) {
	EnableOptionalKubernetesFeatureGates()
	assert.Assert(t, feature.DefaultFeatureGate.Enabled(features.PodLevelResources), "pod level resources not enabled")
	assert.Assert(t, feature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling), "in-place pod vertical scaling not enabled")
}

func TestRunGeneralPredicates(t *testing.T) {
	ep := enabledPlugins(noderesources.Name, nodename.Name, nodeports.Name, nodevolumelimits.CSIName)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	resourceTests := []struct {
		pod      *v1.Pod
		nodeInfo *framework.NodeInfo
		node     *v1.Node
		fits     bool
		name     string
		wErr     error
	}{
		{
			pod: &v1.Pod{},
			nodeInfo: framework.NewNodeInfo(
				newResourcePod(framework.Resource{MilliCPU: 9, Memory: 19})),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0), Allocatable: makeResources(10, 20, 32, 0, 0, 0)},
			},
			fits: true,
			wErr: nil,
			name: "no resources/port/host requested always fits",
		},
		{
			pod: newResourcePod(framework.Resource{MilliCPU: 8, Memory: 10}),
			nodeInfo: framework.NewNodeInfo(
				newResourcePod(framework.Resource{MilliCPU: 5, Memory: 19})),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0), Allocatable: makeResources(10, 20, 32, 0, 0, 0)},
			},
			fits: false,
			wErr: nil,
			name: "not enough cpu and memory resource",
		},
		{
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					NodeName: "machine2",
				},
			},
			nodeInfo: framework.NewNodeInfo(),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0), Allocatable: makeResources(10, 20, 32, 0, 0, 0)},
			},
			fits: false,
			wErr: nil,
			name: "host not match",
		},
		{
			pod:      newPodWithPort(123),
			nodeInfo: framework.NewNodeInfo(newPodWithPort(123)),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0), Allocatable: makeResources(10, 20, 32, 0, 0, 0)},
			},
			fits: false,
			wErr: nil,
			name: "host port conflict",
		},
	}
	for _, test := range resourceTests {
		t.Run(test.name, func(t *testing.T) {
			test.nodeInfo.SetNode(test.node)
			_, cycleState, err := predicateManager.PreFilter(test.pod, true)
			assert.NilError(t, err)
			assert.Assert(t, cycleState != nil)
			plugin, err := predicateManager.Filter(test.pod, test.nodeInfo, cycleState, true)
			if (err == nil) != test.fits {
				t.Errorf("%s expected fit state '%t' did not match real state and err = %v, plugin = %v", test.name, test.fits, err, plugin)
			}
		})
	}
}

//nolint:funlen
func TestInterPodAffinity(t *testing.T) {
	ep := enabledPlugins(interpodaffinity.Name, nodeaffinity.Name)
	handle, lister := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	podLabel := map[string]string{"service": "securityscan"}
	labels1 := map[string]string{
		"region": "r1",
		"zone":   "z11",
	}
	podLabel2 := map[string]string{"security": "S1"}
	node1 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "machine1", Labels: labels1}}
	tests := []struct {
		pod  *v1.Pod
		pods []*v1.Pod
		node *v1.Node
		fits bool
		name string
	}{
		{
			pod:  new(v1.Pod),
			node: &node1,
			fits: true,
			name: "A pod that has no required pod affinity scheduling rules can schedule onto a node with no existing pods",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: true,
			name: "satisfies with requiredDuringSchedulingIgnoredDuringExecution in PodAffinity using In operator that matches the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpNotIn,
												Values:   []string{"securityscan3", "value3"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: true,
			name: "satisfies the pod with requiredDuringSchedulingIgnoredDuringExecution in PodAffinity using not in operator in labelSelector that matches the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									Namespaces: []string{"DiffNameSpace"},
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel, Namespace: "ns"}}},
			node: &node1,
			fits: false,
			name: "Does not satisfy the PodAffinity with labelSelector because of diff Namespace",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"antivirusscan", "value2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: false,
			name: "Doesn't satisfy the PodAffinity because of unmatching labelSelector with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpExists,
											}, {
												Key:      "wrongkey",
												Operator: metav1.LabelSelectorOpDoesNotExist,
											},
										},
									},
									TopologyKey: "region",
								}, {
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan"},
											}, {
												Key:      "service",
												Operator: metav1.LabelSelectorOpNotIn,
												Values:   []string{"WrongValue"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: true,
			name: "satisfies the PodAffinity with different label Operators in multiple RequiredDuringSchedulingIgnoredDuringExecution ",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpExists,
											}, {
												Key:      "wrongkey",
												Operator: metav1.LabelSelectorOpDoesNotExist,
											},
										},
									},
									TopologyKey: "region",
								}, {
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan2"},
											}, {
												Key:      "service",
												Operator: metav1.LabelSelectorOpNotIn,
												Values:   []string{"WrongValue"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: false,
			name: "The labelSelector requirements(items of matchExpressions) are ANDed, the pod cannot schedule onto the node because one of the matchExpression item don't match.",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"antivirusscan", "value2"},
											},
										},
									},
									TopologyKey: "node",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: true,
			name: "satisfies the PodAffinity and PodAntiAffinity with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"antivirusscan", "value2"},
											},
										},
									},
									TopologyKey: "node",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					Spec: v1.PodSpec{
						NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "service",
													Operator: metav1.LabelSelectorOpIn,
													Values:   []string{"antivirusscan", "value2"},
												},
											},
										},
										TopologyKey: "node",
									},
								},
							},
						},
					},
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel},
				},
			},
			node: &node1,
			fits: true,
			name: "satisfies the PodAffinity and PodAntiAffinity and PodAntiAffinity symmetry with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel2,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "zone",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: false,
			name: "satisfies the PodAffinity but doesn't satisfy the PodAntiAffinity with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpIn,
												Values:   []string{"antivirusscan", "value2"},
											},
										},
									},
									TopologyKey: "node",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					Spec: v1.PodSpec{
						NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "service",
													Operator: metav1.LabelSelectorOpIn,
													Values:   []string{"securityscan", "value2"},
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel},
				},
			},
			node: &node1,
			fits: false,
			name: "satisfies the PodAffinity and PodAntiAffinity but doesn't satisfy PodAntiAffinity symmetry with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAffinity: &v1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpNotIn,
												Values:   []string{"securityscan", "value2"},
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node: &node1,
			fits: false,
			name: "pod matches its own Label in PodAffinity and that matches the existing pod Labels",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel,
				},
			},
			pods: []*v1.Pod{
				{
					Spec: v1.PodSpec{NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "service",
													Operator: metav1.LabelSelectorOpIn,
													Values:   []string{"securityscan", "value2"},
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel},
				},
			},
			node: &node1,
			fits: false,
			name: "verify that PodAntiAffinity from existing pod is respected when pod has no AntiAffinity constraints. doesn't satisfy PodAntiAffinity symmetry with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel,
				},
			},
			pods: []*v1.Pod{
				{
					Spec: v1.PodSpec{NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "service",
													Operator: metav1.LabelSelectorOpNotIn,
													Values:   []string{"securityscan", "value2"},
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel},
				},
			},
			node: &node1,
			fits: true,
			name: "verify that PodAntiAffinity from existing pod is respected when pod has no AntiAffinity constraints. satisfy PodAntiAffinity symmetry with the existing pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabel,
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "region",
								},
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "security",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "region",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel2},
					Spec: v1.PodSpec{NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "security",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
				},
			},
			node: &node1,
			fits: false,
			name: "satisfies the PodAntiAffinity with existing pod but doesn't satisfy PodAntiAffinity symmetry with incoming pod",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabel},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "service",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "security",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel2},
					Spec: v1.PodSpec{
						NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "security",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
				},
			},
			node: &node1,
			fits: false,
			name: "PodAntiAffinity symmetry check a1: incoming pod and existing pod partially match each other on AffinityTerms",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabel2},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "security",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Labels: podLabel},
					Spec: v1.PodSpec{
						NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "service",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "security",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
				},
			},
			node: &node1,
			fits: false,
			name: "PodAntiAffinity symmetry check a2: incoming pod and existing pod partially match each other on AffinityTerms",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"abc": "", "xyz": ""}},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "abc",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "def",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"def": "", "xyz": ""}},
					Spec: v1.PodSpec{
						NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "abc",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "def",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
				},
			},
			node: &node1,
			fits: false,
			name: "PodAntiAffinity symmetry check b1: incoming pod and existing pod partially match each other on AffinityTerms",
		},
		{
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"def": "", "xyz": ""}},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "abc",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
								{
									LabelSelector: &metav1.LabelSelector{
										MatchExpressions: []metav1.LabelSelectorRequirement{
											{
												Key:      "def",
												Operator: metav1.LabelSelectorOpExists,
											},
										},
									},
									TopologyKey: "zone",
								},
							},
						},
					},
				},
			},
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"abc": "", "xyz": ""}},
					Spec: v1.PodSpec{
						NodeName: "machine1",
						Affinity: &v1.Affinity{
							PodAntiAffinity: &v1.PodAntiAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "abc",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
									{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "def",
													Operator: metav1.LabelSelectorOpExists,
												},
											},
										},
										TopologyKey: "zone",
									},
								},
							},
						},
					},
				},
			},
			node: &node1,
			fits: false,
			name: "PodAntiAffinity symmetry check b2: incoming pod and existing pod partially match each other on AffinityTerms",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := test.node
			var podsOnNode []*v1.Pod
			for _, pod := range test.pods {
				if pod.Spec.NodeName == node.Name {
					podsOnNode = append(podsOnNode, pod)
				}
			}

			nodeInfo := framework.NewNodeInfo(podsOnNode...)
			nodeInfo.SetNode(test.node)
			lister.NodeLister().Set([]fwk.NodeInfo{nodeInfo})

			_, cycleState, err := predicateManager.PreFilter(test.pod, true)
			assert.NilError(t, err)
			assert.Assert(t, cycleState != nil)
			pl, err := predicateManager.Filter(test.pod, nodeInfo, cycleState, true)
			if (err == nil) != test.fits {
				t.Errorf("%s expected fit state '%t' did not match real state and err = %v, plugin = %v", test.name, test.fits, err, pl)
			}
		})
	}
}

func TestReserveAlloc(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "foo",
		},
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
	}
	nodeInfo := framework.NewNodeInfo(pod)
	nodeInfo.SetNode(node)

	// no predicates configured that are run by reservations
	ep := enabledPlugins()
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)
	_, err = predicateManager.Filter(pod, nodeInfo, framework.NewCycleState(), false)
	assert.NilError(t, err, "error should have been nil, no predicates given")

	// add one predicate also run by reservations
	ep[nodeunschedulable.Name] = true
	predicateManager = newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)
	_, err = predicateManager.Filter(pod, nodeInfo, framework.NewCycleState(), false)
	assert.NilError(t, err, "error should have been nil, node is schedulable")

	// make the node unschedulable
	node, _, err = taints.AddOrUpdateTaint(nodeInfo.Node(), &v1.Taint{
		Key:    v1.TaintNodeUnschedulable,
		Effect: v1.TaintEffectNoSchedule,
	})
	node.Spec.Unschedulable = true
	assert.NilError(t, err, "failed to add taint")
	nodeInfo.SetNode(node)
	_, err = predicateManager.Filter(pod, nodeInfo, framework.NewCycleState(), false)
	if err == nil {
		t.Errorf("error should not have been nil, predicate should have failed")
	}
}

func TestReserveNodeSelector(t *testing.T) {
	labelMap1 := map[string]string{"foo": "bar"}
	labelMap2 := map[string]string{"foo2": "bar2"}
	emptyMap := map[string]string{}
	pod := &v1.Pod{
		Spec: v1.PodSpec{},
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node",
		},
	}
	nodeInfo := framework.NewNodeInfo(pod)
	nodeInfo.SetNode(node)

	ep := enabledPlugins(nodename.Name, nodeports.Name, podtopologyspread.Name, nodeaffinity.Name)
	handle, _ := getFrameworkHandle()
	config, err := DefaultConfig()
	assert.NilError(t, err)
	predicateManager := newPredicateManagerInternal(handle, plugins.NewInTreeRegistry(), config, ep, ep, ep, ep)

	testCases := []struct {
		name          string
		nodeLabels    map[string]string
		nodeSelectors map[string]string
		errorExpected bool
	}{
		{"Match labels", labelMap1, labelMap1, false},
		{"Missing labels", labelMap1, labelMap2, true},
		{"empty node labels", emptyMap, labelMap2, true},
		{"empty node selectors", labelMap1, emptyMap, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod.Spec.NodeSelector = tc.nodeSelectors
			node.Labels = tc.nodeLabels
			_, cycleState, err := predicateManager.PreFilter(pod, true)
			assert.NilError(t, err)
			assert.Assert(t, cycleState != nil)
			plugin, err := predicateManager.Filter(pod, nodeInfo, cycleState, false)
			log.Log(log.Test).Info("reservation predicates called", zap.Error(err), zap.String("plugin", plugin))
			if tc.errorExpected {
				assert.Assert(t, err != nil, "An error is expected")
			} else {
				assert.NilError(t, err, "No error expected")
			}
		})
	}
}

func TestPreFilter(t *testing.T) {
	node1 := "node1"
	node2 := "node2"
	node3 := "node3"
	nodes, nodes1, nodes2, nodes3 := make(sets.Set[string]), make(sets.Set[string]), make(sets.Set[string]), make(sets.Set[string])
	nodes.Insert(node1)
	nodes1.Insert(node1)
	nodes1.Insert(node2)
	nodes2.Insert(node1)
	nodes2.Insert(node3)
	nodes3.Insert(node2)

	pod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "foo",
		},
	}
	testCases := []struct {
		name           string
		pod            *v1.Pod
		plugins        []*MockPreFilterPlugin
		feasibleNodes  []string
		skippedPlugins []string
		errorExpected  error
	}{
		{"success", pod,
			[]*MockPreFilterPlugin{
				{name: mockPreFilterPlugin, inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{NodeNames: nodes}, PreFilterStatus: int(fwk.Success)}},
				{name: mockPreFilterPlugin + "-skip", inj: injectedResult{PreFilterStatus: int(fwk.Skip)}},
			},
			[]string{node1},
			[]string{"mock-prefilter-plugin-skip"}, nil,
		},
		{"intersection of nodes", pod,
			[]*MockPreFilterPlugin{
				{name: mockPreFilterPlugin + "-1", inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{NodeNames: nodes1}, PreFilterStatus: int(fwk.Success)}},
				{name: mockPreFilterPlugin + "-2", inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{NodeNames: nodes2}, PreFilterStatus: int(fwk.Success)}},
			},
			[]string{node1},
			nil, nil,
		},

		{"disjoint set of nodes", pod,
			[]*MockPreFilterPlugin{
				{name: mockPreFilterPlugin + "-1", inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{NodeNames: nodes}, PreFilterStatus: int(fwk.Success)}},
				{name: mockPreFilterPlugin + "-2", inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{NodeNames: nodes3}, PreFilterStatus: int(fwk.Success)}},
			},
			[]string{},
			nil, nil,
		},

		{"rejected - unschedulable and unresolvable", pod,
			[]*MockPreFilterPlugin{
				{name: mockPreFilterPlugin, inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{}, PreFilterStatus: int(fwk.UnschedulableAndUnresolvable)}},
			},
			[]string{},
			nil, errors.New("UnschedulableAndUnresolvable"),
		},
		{"rejected - unschedulable", pod,
			[]*MockPreFilterPlugin{
				{name: mockPreFilterPlugin, inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{}, PreFilterStatus: int(fwk.Unschedulable)}},
			},
			[]string{},
			nil, errors.New("unschedulable"),
		},
		{"error", pod,
			[]*MockPreFilterPlugin{
				{name: mockPreFilterPlugin, inj: injectedResult{PreFilterResult: &fwk.PreFilterResult{}, PreFilterStatus: int(fwk.Error)}},
			},
			[]string{},
			nil, errors.New("error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := make(runtime.Registry)
			enabled := make([]apiConfig.Plugin, len(tc.plugins))
			var eps []string
			for i, p := range tc.plugins {
				enabled[i].Name = p.name
				eps = append(eps, p.name)
				if err := r.Register(p.name, func(_ context.Context, _ runtime2.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return p, nil
				}); err != nil {
					t.Fatalf("fail to register PreFilter plugin (%s)", p.Name())
				}
			}
			for _, allocate := range []bool{true, false} {
				ep := enabledPlugins(eps...)
				handle, _ := getFrameworkHandle()
				config, err := prepareConfig(eps)
				assert.NilError(t, err)
				var resPreFilters map[string]bool
				var allocPreFilters map[string]bool
				if allocate {
					allocPreFilters = ep
				} else {
					resPreFilters = ep
				}
				p := newPredicateManagerInternal(handle, r, config, resPreFilters, allocPreFilters, nil, nil)
				feasibleNodes, cycleState, filterErr := p.PreFilter(tc.pod, allocate)
				if tc.skippedPlugins != nil {
					for _, sp := range tc.skippedPlugins {
						assert.Assert(t, cycleState.GetSkipFilterPlugins().Has(sp) == true, nil)
					}
				}
				if tc.feasibleNodes != nil {
					assert.Equal(t, len(tc.feasibleNodes), len(feasibleNodes))
					for _, fn := range tc.feasibleNodes {
						assert.Assert(t, feasibleNodes[fn] != nil, nil)
					}
				}
				if tc.errorExpected != nil {
					assert.Assert(t, filterErr != nil, "error should not be nil")
				}
			}
		})
	}
}

func TestFilter(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "foo",
		},
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		}}
	skip := make(sets.Set[string])
	testCases := []struct {
		name           string
		pod            *v1.Pod
		filterPlugins  []*MockFilterPlugin
		skippedPlugins sets.Set[string]
		errorExpected  error
	}{
		{"success", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.Success)}},
			},
			nil, nil,
		},
		{
			"skip", pod,
			[]*MockFilterPlugin{
				// Inject error code so that if not skipped error would be thrown
				{name: mockFilterPlugin + "-skip", inj: injectedResult{FilterStatus: int(fwk.Error)}},
			},
			skip.Insert("mock-filter-plugin-skip"), nil,
		},
		{"rejected - unschedulable and unresolvable", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.UnschedulableAndUnresolvable)}},
			},
			nil, errors.New("UnschedulableAndUnresolvable"),
		},
		{"rejected - unschedulable", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.Unschedulable)}},
			},
			nil, errors.New("unschedulable"),
		},
		{"error", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.Error)}},
			},
			nil, errors.New("error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := make(runtime.Registry)
			enabled := make([]apiConfig.Plugin, len(tc.filterPlugins))
			var eps []string
			for i, p := range tc.filterPlugins {
				enabled[i].Name = p.name
				eps = append(eps, p.name)
				if err := r.Register(p.name, func(_ context.Context, _ runtime2.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return p, nil
				}); err != nil {
					t.Fatalf("fail to register PreFilter plugin (%s)", p.Name())
				}
			}
			for _, allocate := range []bool{true, false} {
				ep := enabledPlugins(eps...)
				handle, _ := getFrameworkHandle()
				config, err := prepareConfig(eps)
				assert.NilError(t, err)
				var resFilters map[string]bool
				var allocFilters map[string]bool
				if allocate {
					allocFilters = ep
				} else {
					resFilters = ep
				}
				p := newPredicateManagerInternal(handle, r, config, nil, nil, resFilters, allocFilters)
				cycleState := framework.NewCycleState()
				if tc.skippedPlugins != nil {
					cycleState.SetSkipFilterPlugins(tc.skippedPlugins)
				}
				nodeInfo := framework.NewNodeInfo()
				nodeInfo.SetNode(node)
				filter, err := p.Filter(tc.pod, nodeInfo, cycleState, allocate)
				if tc.errorExpected != nil {
					assert.Assert(t, err != nil, "error should not be nil")
					assert.Equal(t, filter, tc.filterPlugins[0].Name())
				} else {
					assert.Equal(t, filter, "")
				}
			}
		})
	}
}

func TestPreemptionFilter(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "foo",
		},
	}
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		}}
	skip := make(sets.Set[string])
	testCases := []struct {
		name           string
		pod            *v1.Pod
		filterPlugins  []*MockFilterPlugin
		skippedPlugins sets.Set[string]
		errorExpected  error
	}{
		{"success", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.Success)}},
			},
			nil, nil,
		},
		{
			"skip", pod,
			[]*MockFilterPlugin{
				// Inject error code so that if not skipped error would be thrown
				{name: mockFilterPlugin + "-skip", inj: injectedResult{FilterStatus: int(fwk.Error)}},
			},
			skip.Insert("mock-filter-plugin-skip"), nil,
		},
		{"rejected - unschedulable and unresolvable", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.UnschedulableAndUnresolvable)}},
			},
			nil, errors.New("UnschedulableAndUnresolvable"),
		},
		{"rejected - unschedulable", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.Unschedulable)}},
			},
			nil, errors.New("unschedulable"),
		},
		{"error", pod,
			[]*MockFilterPlugin{
				{name: mockFilterPlugin, inj: injectedResult{FilterStatus: int(fwk.Error)}},
			},
			nil, errors.New("error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := make(runtime.Registry)
			enabled := make([]apiConfig.Plugin, len(tc.filterPlugins))
			var eps []string
			for i, p := range tc.filterPlugins {
				enabled[i].Name = p.name
				eps = append(eps, p.name)
				if err := r.Register(p.name, func(_ context.Context, _ runtime2.Object, fh fwk.Handle) (fwk.Plugin, error) {
					return p, nil
				}); err != nil {
					t.Fatalf("fail to register PreFilter plugin (%s)", p.Name())
				}
			}
			ep := enabledPlugins(eps...)
			handle, _ := getFrameworkHandle()
			config, err := prepareConfig(eps)
			assert.NilError(t, err)
			p := newPredicateManagerInternal(handle, r, config, nil, nil, nil, ep)
			cycleState := framework.NewCycleState()
			if tc.skippedPlugins != nil {
				cycleState.SetSkipFilterPlugins(tc.skippedPlugins)
			}
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(node)

			victims := []*v1.Pod{
				newResourcePod(framework.Resource{MilliCPU: 100, Memory: 1000000}),
			}
			victims[0].Name = "pod0"
			idx := p.PreemptionFilter(tc.pod, nodeInfo, cycleState, victims, 0)
			if tc.errorExpected != nil {
				assert.Equal(t, idx, -1)
			} else {
				assert.Equal(t, idx, 0)
			}
		})
	}
}

func prepareConfig(plugins []string) (*apiConfig.KubeSchedulerConfiguration, error) {
	var ep []apiConfig.Plugin
	cfg := apiConfig.KubeSchedulerConfiguration{}
	if len(plugins) > 0 {
		for _, pl := range plugins {
			ep = append(ep, apiConfig.Plugin{Name: pl})
		}
		cfg.Profiles = []apiConfig.KubeSchedulerProfile{
			{
				SchedulerName: "default-scheduler",
				Plugins: &apiConfig.Plugins{
					MultiPoint: apiConfig.PluginSet{
						Enabled: ep,
					},
				},
			},
		}
	}
	return &cfg, nil
}

func enabledPlugins(name ...string) map[string]bool {
	pm := make(map[string]bool)
	for _, k := range name {
		pm[k] = true
	}
	return pm
}

func getFrameworkHandle() (fwk.Handle, *test.SharedListerMock) {
	clientSet := support.ClientSet()
	si := support.InformerFactory(clientSet)
	lister := support.Lister()
	handle := support.NewFrameworkHandle(lister, si, clientSet, test.NewCSIManagerMock(client.NewMockedAPIProvider(false).GetAPIs().CSINodeInformer.Lister()), support.SharedDRAManager())
	return handle, lister
}

var _ fwk.SharedLister = test.NewSharedListerMock()
var _ fwk.NodeInfoLister = test.NewNodeInfoListerMock()
var _ fwk.StorageInfoLister = test.NewStorageInfoListerMock()
