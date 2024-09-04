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
	"encoding/json"
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

const (
	gpu       = "nvidia.com/gpu"
	ephemeral = "ephemeral-storage"
	hugepages = "hugepages-1Gi"
)

var taskGroups = []TaskGroup{
	{
		Name:      "test-group-1",
		MinMember: 10,
		MinResource: map[string]resource.Quantity{
			"cpu":     resource.MustParse("500m"),
			"memory":  resource.MustParse("1024M"),
			gpu:       resource.MustParse("2"),
			ephemeral: resource.MustParse("2G"),
			hugepages: resource.MustParse("2"),
		},
		Labels: map[string]string{
			"labelKey0": "labelKeyValue0",
			"labelKey1": "labelKeyValue1",
		},
		Annotations: map[string]string{
			"annotationKey0": "annotationValue0",
			"annotationKey1": "annotationValue1",
			"annotationKey2": "annotationValue2",
		},
		NodeSelector: map[string]string{
			"nodeType":  "test",
			"nodeState": "healthy",
		},
		Tolerations: []v1.Toleration{
			{
				Key:      "key1",
				Operator: v1.TolerationOpEqual,
				Value:    "value1",
				Effect:   v1.TaintEffectNoSchedule,
			},
		},
		Affinity: &v1.Affinity{
			PodAffinity: &v1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
					{
						TopologyKey: "topologyKey",
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "service",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"securityscan", "value2"},
								},
							},
						},
					},
				},
			},
		},
		TopologySpreadConstraints: []v1.TopologySpreadConstraint{
			{
				MaxSkew:           1,
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"labelKey0": "labelKeyValue0",
						"labelKey1": "labelKeyValue1",
					},
				},
			},
		},
	},
}

func TestNewPlaceholder(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue, "bob",
		testGroups, map[string]string{constants.AppTagNamespace: namespace, constants.AppTagImagePullSecrets: "secret1,secret2"},
		mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)
	app.setSchedulingParamsDefinition("gangSchedulingStyle=Soft")
	marshalledTaskGroups, err := json.Marshal(taskGroups)
	assert.NilError(t, err, "taskGroups marshalling failed")
	app.setTaskGroupsDefinition(string(marshalledTaskGroups))

	assert.Equal(t, app.placeholderAsk.Resources[siCommon.CPU].Value, int64(10*500))
	assert.Equal(t, app.placeholderAsk.Resources[siCommon.Memory].Value, int64(10*1024*1000*1000))
	assert.Equal(t, app.placeholderAsk.Resources["pods"].Value, int64(10))

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, holder.appID, appID)
	assert.Equal(t, holder.taskGroupName, app.taskGroups[0].Name)
	assert.Equal(t, holder.pod.Spec.SchedulerName, constants.SchedulerName)
	assert.Equal(t, holder.pod.Name, "ph-name")
	assert.Equal(t, holder.pod.Namespace, namespace)
	assert.DeepEqual(t, holder.pod.Labels, map[string]string{
		constants.CanonicalLabelApplicationID: appID,
		constants.CanonicalLabelQueueName:     queue,
		"labelKey0":                           "labelKeyValue0",
		"labelKey1":                           "labelKeyValue1",
	})
	assert.Equal(t, len(holder.pod.Annotations), 7, "unexpected number of annotations")
	assert.Equal(t, holder.pod.Annotations[constants.AnnotationTaskGroupName], app.taskGroups[0].Name)
	assert.Equal(t, holder.pod.Annotations[constants.AnnotationPlaceholderFlag], constants.True)
	assert.Equal(t, holder.pod.Annotations["annotationKey0"], "annotationValue0")
	assert.Equal(t, holder.pod.Annotations["annotationKey1"], "annotationValue1")
	assert.Equal(t, holder.pod.Annotations["annotationKey2"], "annotationValue2")
	assert.Equal(t, holder.pod.Annotations[constants.AnnotationSchedulingPolicyParam], "gangSchedulingStyle=Soft")
	var taskGroupsDef []TaskGroup
	err = json.Unmarshal([]byte(holder.pod.Annotations[siCommon.DomainYuniKorn+"task-groups"]), &taskGroupsDef)
	assert.NilError(t, err, "taskGroupsDef unmarshal failed")
	assert.Equal(t, common.GetPodResource(holder.pod).Resources[siCommon.CPU].Value, int64(500))
	assert.Equal(t, common.GetPodResource(holder.pod).Resources[siCommon.Memory].Value, int64(1024*1000*1000))
	assert.Equal(t, common.GetPodResource(holder.pod).Resources["pods"].Value, int64(1))
	assert.Equal(t, len(holder.pod.Spec.NodeSelector), 2, "unexpected number of node selectors")
	assert.Equal(t, len(holder.pod.Spec.Tolerations), 1, "unexpected number of tolerations")
	assert.Equal(t, holder.String(), "appID: app01, taskGroup: test-group-1, podName: test/ph-name")
	assert.Equal(t, holder.pod.Spec.SecurityContext.RunAsUser, &runAsUser)
	assert.Equal(t, holder.pod.Spec.SecurityContext.RunAsGroup, &runAsGroup)
	assert.Equal(t, len(holder.pod.Spec.ImagePullSecrets), 2, "unexpected number of pull secrets")
	assert.Equal(t, "secret1", holder.pod.Spec.ImagePullSecrets[0].Name)
	assert.Equal(t, "secret2", holder.pod.Spec.ImagePullSecrets[1].Name)
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, "", holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderWithNodeSelectors(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.NodeSelector), 2)
	assert.Equal(t, holder.pod.Spec.NodeSelector["nodeType"], "test")
	assert.Equal(t, holder.pod.Spec.NodeSelector["nodeState"], "healthy")
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, "", holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderWithTolerations(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.Tolerations), 1)
	tlr := holder.pod.Spec.Tolerations[0]
	assert.Equal(t, tlr.Key, "key1")
	assert.Equal(t, tlr.Value, "value1")
	assert.Equal(t, tlr.Operator, v1.TolerationOpEqual)
	assert.Equal(t, tlr.Effect, v1.TaintEffectNoSchedule)
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, "", holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderWithAffinity(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution), 1)
	term := holder.pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	assert.Equal(t, term[0].TopologyKey, "topologyKey")

	assert.Equal(t, len(term[0].LabelSelector.MatchExpressions), 1)
	assert.Equal(t, term[0].LabelSelector.MatchExpressions[0].Key, "service")
	assert.Equal(t, term[0].LabelSelector.MatchExpressions[0].Operator, metav1.LabelSelectorOpIn)
	assert.Equal(t, term[0].LabelSelector.MatchExpressions[0].Values[0], "securityscan")
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, "", holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderTaskGroupsDefinition(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)
	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, "", holder.pod.Annotations[constants.AnnotationTaskGroups])

	app = NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)
	app.setTaskGroupsDefinition("taskGroupsDef")
	holder = newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, "taskGroupsDef", holder.pod.Annotations[constants.AnnotationTaskGroups])
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, "", holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderExtendedResources(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)
	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.Containers[0].Resources.Requests), 5, "expected requests not found")
	assert.Equal(t, len(holder.pod.Spec.Containers[0].Resources.Limits), 5, "expected limits not found")
	assert.Equal(t, holder.pod.Spec.Containers[0].Resources.Limits[gpu], holder.pod.Spec.Containers[0].Resources.Requests[gpu], "gpu: expected same value for request and limit")
	assert.Equal(t, holder.pod.Spec.Containers[0].Resources.Limits[hugepages], holder.pod.Spec.Containers[0].Resources.Requests[hugepages], "hugepages: expected same value for request and limit")
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, "", holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderWithPriorityClassName(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)
	mockedContext := initContextForTest()
	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "tg-test-group-1-app01-0",
			UID:  "UID-01",
		},
		Spec: v1.PodSpec{
			PriorityClassName: priorityClassName,
		},
	}
	taskID1 := "task1-01"
	task1 := NewTask(taskID1, app, mockedContext, pod1)
	task1.placeholder = true
	task1.pod = pod1
	task1.originator = true
	app.taskMap[taskID1] = task1
	app.setOriginatingTask(task1)

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.Containers[0].Resources.Requests), 5, "expected requests not found")
	assert.Equal(t, len(holder.pod.Spec.Containers[0].Resources.Limits), 5, "expected limits not found")
	assert.Equal(t, holder.pod.Spec.Containers[0].Resources.Limits[gpu], holder.pod.Spec.Containers[0].Resources.Requests[gpu], "gpu: expected same value for request and limit")
	assert.Equal(t, holder.pod.Spec.Containers[0].Resources.Limits[hugepages], holder.pod.Spec.Containers[0].Resources.Requests[hugepages], "hugepages: expected same value for request and limit")
	var priority *int32
	assert.Equal(t, priority, holder.pod.Spec.Priority)
	assert.Equal(t, priorityClassName, holder.pod.Spec.PriorityClassName)
}

func TestNewPlaceholderWithTopologySpreadConstraints(t *testing.T) {
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", testGroups, map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups(taskGroups)

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.TopologySpreadConstraints), 1)
	assert.Equal(t, holder.pod.Spec.TopologySpreadConstraints[0].MaxSkew, int32(1))
	assert.Equal(t, holder.pod.Spec.TopologySpreadConstraints[0].TopologyKey, v1.LabelTopologyZone)
	assert.Equal(t, holder.pod.Spec.TopologySpreadConstraints[0].WhenUnsatisfiable, v1.DoNotSchedule)
	assert.DeepEqual(t, holder.pod.Spec.TopologySpreadConstraints[0].LabelSelector.MatchLabels, map[string]string{
		"labelKey0": "labelKeyValue0",
		"labelKey1": "labelKeyValue1",
	})
}
