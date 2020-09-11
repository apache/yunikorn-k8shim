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

package common

import (
	"strconv"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SleepPodConfig struct {
	Name  string
	NS    string
	AppID string
	Time  int
	CPU   int64
	Mem   int64
}

// TestPodConfig template for sleepPods
func InitSleepPod(conf SleepPodConfig) *v1.Pod {
	// Initialize default values
	if conf.Name == "" {
		conf.Name = "sleep-" + RandSeq(5)
	}
	if conf.AppID == "" {
		conf.AppID = GetUUID()
	}
	if conf.Time == 0 {
		conf.Time = 600
	}
	if conf.CPU == 0 {
		conf.CPU = 100
	}
	if conf.Mem == 0 {
		conf.Mem = 50
	}

	testPodConfig := TestPodConfig{
		Name:          conf.Name,
		Namespace:     conf.NS,
		RestartPolicy: v1.RestartPolicyNever,
		Command:       []string{"sleep", strconv.Itoa(conf.Time)},
		Labels: map[string]string{
			"app":           "sleep",
			"applicationId": conf.AppID,
		},
		Resources: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse(strconv.FormatInt(conf.CPU, 10) + "m"),
				"memory": resource.MustParse(strconv.FormatInt(conf.Mem, 10) + "M"),
			},
		},
	}

	return InitTestPod(testPodConfig)
}

type TestPodConfig struct {
	Name                              string
	Namespace                         string
	Affinity                          *v1.Affinity
	Annotations, Labels, NodeSelector map[string]string
	Resources                         *v1.ResourceRequirements
	RuntimeClassHandler               *string
	Tolerations                       []v1.Toleration
	NodeName                          string
	Ports                             []v1.ContainerPort
	OwnerReferences                   []metav1.OwnerReference
	PriorityClassName                 string
	DeletionGracePeriodSeconds        *int64
	TopologySpreadConstraints         []v1.TopologySpreadConstraint
	Image                             string
	RestartPolicy                     v1.RestartPolicy
	Command                           []string
}

func InitTestPod(conf TestPodConfig) *v1.Pod {
	var gracePeriod = int64(1)
	if conf.Image == "" {
		conf.Image = "alpine:latest"
	}
	if conf.Command == nil {
		conf.Command = []string{"sleep", "30"}
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            conf.Name,
			Namespace:       conf.Namespace,
			Labels:          conf.Labels,
			Annotations:     conf.Annotations,
			OwnerReferences: conf.OwnerReferences,
		},
		Spec: v1.PodSpec{
			SchedulerName:             configmanager.SchedulerName,
			RestartPolicy:             conf.RestartPolicy,
			NodeName:                  conf.NodeName,
			NodeSelector:              conf.NodeSelector,
			Affinity:                  conf.Affinity,
			TopologySpreadConstraints: conf.TopologySpreadConstraints,
			RuntimeClassName:          conf.RuntimeClassHandler,
			Containers: []v1.Container{
				{
					Name:    conf.Name,
					Image:   conf.Image,
					Ports:   conf.Ports,
					Command: conf.Command,
				},
			},
			Tolerations:                   conf.Tolerations,
			PriorityClassName:             conf.PriorityClassName,
			TerminationGracePeriodSeconds: &gracePeriod,
		},
	}
	if conf.Resources != nil {
		pod.Spec.Containers[0].Resources = *conf.Resources
	}
	if conf.DeletionGracePeriodSeconds != nil {
		pod.ObjectMeta.DeletionGracePeriodSeconds = conf.DeletionGracePeriodSeconds
	}
	return pod
}
