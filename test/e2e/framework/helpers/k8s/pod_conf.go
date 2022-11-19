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

package k8s

import (
	"encoding/json"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
)

type SleepPodConfig struct {
	Name         string
	NS           string
	AppID        string
	Time         int
	CPU          int64
	Mem          int64
	RequiredNode string
	Optedout     bool
	UID          types.UID
}

// TestPodConfig template for  sleepPods
func InitSleepPod(conf SleepPodConfig) (*v1.Pod, error) {
	// Initialize default values
	if conf.Name == "" {
		conf.Name = "sleep-" + common.RandSeq(5)
	}
	if conf.AppID == "" {
		conf.AppID = common.GetUUID()
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

	var owners []metav1.OwnerReference
	affinity := &v1.Affinity{}
	if conf.RequiredNode != "" {
		owner := metav1.OwnerReference{APIVersion: "v1", Kind: constants.DaemonSetType, Name: "daemonset job", UID: "daemonset"}
		owners = []metav1.OwnerReference{owner}

		requirement := v1.NodeSelectorRequirement{
			Key:      "metadata.name",
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{conf.RequiredNode},
		}
		fields := []v1.NodeSelectorRequirement{requirement}
		terms := []v1.NodeSelectorTerm{
			{
				MatchFields: fields,
			},
		}
		affinity = &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: terms,
				},
			},
		}
	}

	if conf.UID != "" {
		owner := metav1.OwnerReference{APIVersion: "v1", Kind: "ReplicaSet", Name: "ReplicaSetJob", UID: conf.UID}
		owners = []metav1.OwnerReference{owner}
	}

	optedOut := "true"
	if !conf.Optedout {
		optedOut = "false"
	}

	var secs int64 = 0
	testPodConfig := TestPodConfig{
		Name:                       conf.Name,
		Namespace:                  conf.NS,
		RestartPolicy:              v1.RestartPolicyNever,
		DeletionGracePeriodSeconds: &secs,
		Command:                    []string{"sleep", strconv.Itoa(conf.Time)},
		Labels: map[string]string{
			"app":                                  "sleep",
			"applicationId":                        conf.AppID,
			"yunikorn.apache.org/allow-preemption": optedOut,
		},
		Resources: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse(strconv.FormatInt(conf.CPU, 10) + "m"),
				"memory": resource.MustParse(strconv.FormatInt(conf.Mem, 10) + "M"),
			},
		},
		Affinity:        affinity,
		OwnerReferences: owners,
	}

	return InitTestPod(testPodConfig)
}

type TestPodConfig struct {
	Name                       string
	Namespace                  string
	Affinity                   *v1.Affinity
	Annotations                *PodAnnotation
	Labels, NodeSelector       map[string]string
	Resources                  *v1.ResourceRequirements
	RuntimeClassHandler        *string
	Tolerations                []v1.Toleration
	NodeName                   string
	Ports                      []v1.ContainerPort
	OwnerReferences            []metav1.OwnerReference
	PriorityClassName          string
	DeletionGracePeriodSeconds *int64
	TopologySpreadConstraints  []v1.TopologySpreadConstraint
	Image                      string
	RestartPolicy              v1.RestartPolicy
	Command                    []string
	InitContainerSleepSecs     int
}

func InitTestPod(conf TestPodConfig) (*v1.Pod, error) { //nolint:funlen
	var gracePeriod = int64(1)
	if conf.Image == "" {
		conf.Image = "alpine:latest"
	}
	if conf.Command == nil {
		conf.Command = []string{"sleep", "300"}
	}

	podAnnotation, err := PodAnnotationToMap(conf.Annotations)
	if err != nil {
		return nil, err
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            conf.Name,
			Namespace:       conf.Namespace,
			Labels:          conf.Labels,
			Annotations:     podAnnotation,
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
					Name:            "sleepcontainer",
					Image:           conf.Image,
					ImagePullPolicy: v1.PullIfNotPresent,
					Ports:           conf.Ports,
					Command:         conf.Command,
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
	if conf.InitContainerSleepSecs > 0 {
		containerReqs := v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse(strconv.FormatInt(20, 10) + "m"),
				"memory": resource.MustParse(strconv.FormatInt(20, 10) + "M"),
			},
		}
		pod.Spec.InitContainers = []v1.Container{
			{
				Name:            "init-sleep",
				Image:           conf.Image,
				ImagePullPolicy: v1.PullIfNotPresent,
				Command:         []string{"sleep", strconv.Itoa(conf.InitContainerSleepSecs)},
				Resources:       containerReqs,
			},
		}
	}
	return pod, nil
}

// Pod.ObjectMeta.Annotations expect map[string]string. Converts struct to map[string]string
func PodAnnotationToMap(annotations *PodAnnotation) (map[string]string, error) {
	// Check for empty annotations
	if annotations == nil {
		return make(map[string]string), nil
	}

	// Convert string vals first
	var annotationsMap map[string]string
	annotationsJSON, err := json.Marshal(annotations)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(annotationsJSON, &annotationsMap)
	if err != nil {
		return nil, err
	}

	// Add TaskGroup definition with string
	taskGroupJSON, err := json.Marshal(annotations.TaskGroups)
	if err != nil {
		return nil, err
	}
	annotationsMap[TaskGroups] = string(taskGroupJSON)

	// Add non-YK annotations
	for annKey, annVal := range annotations.Other {
		annotationsMap[annKey] = annVal
	}

	return annotationsMap, nil
}
