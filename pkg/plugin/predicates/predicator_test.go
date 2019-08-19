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

package predicates

import (
	"fmt"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/factory"
	deschedulernode "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"strconv"
	"strings"
	"testing"
)

var (
	extendedResourceA     = v1.ResourceName("example.com/aaa")
	extendedResourceB     = v1.ResourceName("example.com/bbb")
	kubernetesIOResourceA = v1.ResourceName("kubernetes.io/something")
	kubernetesIOResourceB = v1.ResourceName("subdomain.kubernetes.io/something")
	hugePageResourceA     = v1helper.HugePageResourceName(resource.MustParse("2Mi"))
)

func TestPodFitsHost(t *testing.T) {
	conf.Set(&conf.SchedulerConf{TestMode: true})
	predictor := newPredictorInternal(&factory.PluginFactoryArgs{}, schedulerapi.Policy{
		Predicates: []schedulerapi.PredicatePolicy{
			{Name: predicates.HostNamePred},
		}})
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
			nodeInfo := deschedulernode.NewNodeInfo()
			nodeInfo.SetNode(test.node)
			err := predictor.Predicates(test.pod, nil, nodeInfo)
			assert.Equal(t, err == nil, test.fits)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
	}
}

func newPod(host string, hostPortInfos ...string) *v1.Pod {
	var networkPorts []v1.ContainerPort
	for _, portInfo := range hostPortInfos {
		split := strings.Split(portInfo, "/")
		hostPort, _ := strconv.Atoi(split[2])

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
	predictor := newPredictorInternal(&factory.PluginFactoryArgs{}, schedulerapi.Policy{
		Predicates: []schedulerapi.PredicatePolicy{
			{Name: predicates.PodFitsHostPortsPred },
		}})
	tests := []struct {
		pod      *v1.Pod
		nodeInfo *deschedulernode.NodeInfo
		fits     bool
		name     string
	}{
		{
			pod:      &v1.Pod{},
			nodeInfo: deschedulernode.NewNodeInfo(),
			fits:     true,
			name:     "nothing running",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/9090")),
			fits: true,
			name: "other port",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/8080")),
			fits: false,
			name: "same udp port",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8080")),
			fits: false,
			name: "same tcp port",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.2/8080")),
			fits: true,
			name: "different host ip",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8080")),
			fits: true,
			name: "different protocol",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8000", "UDP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "UDP/127.0.0.1/8080")),
			fits: false,
			name: "second udp port conflict",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8001", "UDP/127.0.0.1/8080"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001", "UDP/127.0.0.1/8081")),
			fits: false,
			name: "first tcp port conflict",
		},
		{
			pod: newPod("m1", "TCP/0.0.0.0/8001"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001")),
			fits: false,
			name: "first tcp port conflict due to 0.0.0.0 hostIP",
		},
		{
			pod: newPod("m1", "TCP/10.0.10.10/8001", "TCP/0.0.0.0/8001"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/127.0.0.1/8001")),
			fits: false,
			name: "TCP hostPort conflict due to 0.0.0.0 hostIP",
		},
		{
			pod: newPod("m1", "TCP/127.0.0.1/8001"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			fits: false,
			name: "second tcp port conflict to 0.0.0.0 hostIP",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8001"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001")),
			fits: true,
			name: "second different protocol",
		},
		{
			pod: newPod("m1", "UDP/127.0.0.1/8001"),
			nodeInfo: deschedulernode.NewNodeInfo(
				newPod("m1", "TCP/0.0.0.0/8001", "UDP/0.0.0.0/8001")),
			fits: false,
			name: "UDP hostPort conflict due to 0.0.0.0 hostIP",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := predictor.Predicates(test.pod, nil, test.nodeInfo)
			assert.Equal(t, err == nil, test.fits)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
	}
}

func TestPodFitsSelector(t *testing.T) {
	predictor := newPredictorInternal(&factory.PluginFactoryArgs{}, schedulerapi.Policy{
		Predicates: []schedulerapi.PredicatePolicy{
			{Name: predicates.MatchNodeSelectorPred },
		}})
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
												Key:      schedulerapi.NodeFieldSelectorKeyNodeName,
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
												Key:      schedulerapi.NodeFieldSelectorKeyNodeName,
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
												Key:      schedulerapi.NodeFieldSelectorKeyNodeName,
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
												Key:      schedulerapi.NodeFieldSelectorKeyNodeName,
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
												Key:      schedulerapi.NodeFieldSelectorKeyNodeName,
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
												Key:      schedulerapi.NodeFieldSelectorKeyNodeName,
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
			nodeInfo := deschedulernode.NewNodeInfo()
			nodeInfo.SetNode(&node)

			err := predictor.Predicates(test.pod, nil, nodeInfo)
			assert.Equal(t, err == nil, test.fits)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
	}
}

func newResourcePod(usage ...deschedulernode.Resource) *v1.Pod {
	var containers []v1.Container
	for _, req := range usage {
		containers = append(containers, v1.Container{
			Resources: v1.ResourceRequirements{Requests: req.ResourceList()},
		})
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

func makeResources(milliCPU, memory, pods, extendedA, storage, hugePageA int64) v1.NodeResources {
	return v1.NodeResources{
		Capacity: v1.ResourceList{
			v1.ResourceCPU:              *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
			v1.ResourceMemory:           *resource.NewQuantity(memory, resource.BinarySI),
			v1.ResourcePods:             *resource.NewQuantity(pods, resource.DecimalSI),
			extendedResourceA:           *resource.NewQuantity(extendedA, resource.DecimalSI),
			v1.ResourceEphemeralStorage: *resource.NewQuantity(storage, resource.BinarySI),
			hugePageResourceA:           *resource.NewQuantity(hugePageA, resource.BinarySI),
		},
	}
}

func makeAllocatableResources(milliCPU, memory, pods, extendedA, storage, hugePageA int64) v1.ResourceList {
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
	networkPorts := []v1.ContainerPort{}
	for _, port := range hostPorts {
		networkPorts = append(networkPorts, v1.ContainerPort{HostPort: int32(port)})
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

func TestRunGeneralPredicates(t *testing.T) {
	predictor := newPredictorInternal(&factory.PluginFactoryArgs{}, schedulerapi.Policy{
		Predicates: []schedulerapi.PredicatePolicy{
			{Name: predicates.GeneralPred},
		}})
	resourceTests := []struct {
		pod      *v1.Pod
		nodeInfo *deschedulernode.NodeInfo
		node     *v1.Node
		fits     bool
		name     string
		wErr     error
		reasons  []predicates.PredicateFailureReason
	}{
		{
			pod: &v1.Pod{},
			nodeInfo: deschedulernode.NewNodeInfo(
				newResourcePod(deschedulernode.Resource{MilliCPU: 9, Memory: 19})),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0).Capacity, Allocatable: makeAllocatableResources(10, 20, 32, 0, 0, 0)},
			},
			fits: true,
			wErr: nil,
			name: "no resources/port/host requested always fits",
		},
		{
			pod: newResourcePod(deschedulernode.Resource{MilliCPU: 8, Memory: 10}),
			nodeInfo: deschedulernode.NewNodeInfo(
				newResourcePod(deschedulernode.Resource{MilliCPU: 5, Memory: 19})),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0).Capacity, Allocatable: makeAllocatableResources(10, 20, 32, 0, 0, 0)},
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
			nodeInfo: deschedulernode.NewNodeInfo(),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0).Capacity, Allocatable: makeAllocatableResources(10, 20, 32, 0, 0, 0)},
			},
			fits:    false,
			wErr:    nil,
			name:    "host not match",
		},
		{
			pod:      newPodWithPort(123),
			nodeInfo: deschedulernode.NewNodeInfo(newPodWithPort(123)),
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "machine1"},
				Status:     v1.NodeStatus{Capacity: makeResources(10, 20, 32, 0, 0, 0).Capacity, Allocatable: makeAllocatableResources(10, 20, 32, 0, 0, 0)},
			},
			fits:    false,
			wErr:    nil,
			name:    "host port conflict",
		},
	}
	for _, test := range resourceTests {
		t.Run(test.name, func(t *testing.T) {
			test.nodeInfo.SetNode(test.node)
			err := predictor.Predicates(test.pod, nil, test.nodeInfo)
			assert.Equal(t, err == nil, test.fits)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
	}
}

func TestInterPodAffinity(t *testing.T) {
	predictor := newPredictorInternal(&factory.PluginFactoryArgs{}, schedulerapi.Policy{
		Predicates: []schedulerapi.PredicatePolicy{
			{Name: predicates.MatchInterPodAffinityPred},
		}})
	podLabel := map[string]string{"service": "securityscan"}
	labels1 := map[string]string{
		"region": "r1",
		"zone":   "z11",
	}
	podLabel2 := map[string]string{"security": "S1"}
	node1 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "machine1", Labels: labels1}}
	tests := []struct {
		pod                  *v1.Pod
		pods                 []*v1.Pod
		node                 *v1.Node
		fits                 bool
		name                 string
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
			pods:                 []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel, Namespace: "ns"}}},
			node:                 &node1,
			fits:                 false,
			name:                 "Does not satisfy the PodAffinity with labelSelector because of diff Namespace",
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
			pods:                 []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node:                 &node1,
			fits:                 false,
			name:                 "Doesn't satisfy the PodAffinity because of unmatching labelSelector with the existing pod",
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
			pods:                 []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node:                 &node1,
			fits:                 false,
			name:                 "The labelSelector requirements(items of matchExpressions) are ANDed, the pod cannot schedule onto the node because one of the matchExpression item don't match.",
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
			pods:                 []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine1"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node:                 &node1,
			fits:                 false,
			name:                 "satisfies the PodAffinity but doesn't satisfy the PodAntiAffinity with the existing pod",
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
			node:                 &node1,
			fits:                 false,
			name:                 "satisfies the PodAffinity and PodAntiAffinity but doesn't satisfy PodAntiAffinity symmetry with the existing pod",
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
			pods:                 []*v1.Pod{{Spec: v1.PodSpec{NodeName: "machine2"}, ObjectMeta: metav1.ObjectMeta{Labels: podLabel}}},
			node:                 &node1,
			fits:                 false,
			name:                 "pod matches its own Label in PodAffinity and that matches the existing pod Labels",
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
			node:                 &node1,
			fits:                 false,
			name:                 "verify that PodAntiAffinity from existing pod is respected when pod has no AntiAffinity constraints. doesn't satisfy PodAntiAffinity symmetry with the existing pod",
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
			node:                 &node1,
			fits:                 false,
			name:                 "satisfies the PodAntiAffinity with existing pod but doesn't satisfy PodAntiAffinity symmetry with incoming pod",
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
			node:                 &node1,
			fits:                 false,
			name:                 "PodAntiAffinity symmetry check a1: incoming pod and existing pod partially match each other on AffinityTerms",
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
			node:                 &node1,
			fits:                 false,
			name:                 "PodAntiAffinity symmetry check a2: incoming pod and existing pod partially match each other on AffinityTerms",
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
			node:                 &node1,
			fits:                 false,
			name:                 "PodAntiAffinity symmetry check b1: incoming pod and existing pod partially match each other on AffinityTerms",
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
			node:                 &node1,
			fits:                 false,
			name:                 "PodAntiAffinity symmetry check b2: incoming pod and existing pod partially match each other on AffinityTerms",
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

			nodeInfo := deschedulernode.NewNodeInfo(podsOnNode...)
			nodeInfo.SetNode(test.node)
			nodeInfoMap := map[string]*deschedulernode.NodeInfo{test.node.Name: nodeInfo}
			meta := predictor.GetPredicateMeta(test.pod, nodeInfoMap)
			err := predictor.Predicates(test.pod, meta, nodeInfo)
			assert.Equal(t, err == nil, test.fits)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
	}
}
