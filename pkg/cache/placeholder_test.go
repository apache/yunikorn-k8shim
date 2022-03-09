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
	"os"
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
)

func TestNewPlaceholder(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]v1alpha1.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("1024M"),
			},
		},
	})

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, holder.appID, appID)
	assert.Equal(t, holder.taskGroupName, app.taskGroups[0].Name)
	assert.Equal(t, holder.pod.Spec.SchedulerName, constants.SchedulerName)
	assert.Equal(t, holder.pod.Name, "ph-name")
	assert.Equal(t, holder.pod.Namespace, namespace)
	assert.Equal(t, len(holder.pod.Labels), 3)
	assert.Equal(t, holder.pod.Labels[constants.LabelApplicationID], appID)
	assert.Equal(t, holder.pod.Labels[constants.LabelQueueName], queue)
	assert.Equal(t, len(holder.pod.Annotations), 2)
	assert.Equal(t, holder.pod.Annotations[constants.AnnotationTaskGroupName], app.taskGroups[0].Name)
	assert.Equal(t, common.GetPodResource(holder.pod).Resources[constants.CPU].Value, int64(500))
	assert.Equal(t, common.GetPodResource(holder.pod).Resources[constants.Memory].Value, int64(1024))
	assert.Equal(t, len(holder.pod.Spec.NodeSelector), 0)
	assert.Equal(t, len(holder.pod.Spec.Tolerations), 0)
	assert.Equal(t, holder.String(), "appID: app01, taskGroup: test-group-1, podName: test/ph-name")
	assert.Equal(t, holder.pod.Spec.SecurityContext.RunAsUser, &runAsUser)
	assert.Equal(t, holder.pod.Spec.SecurityContext.RunAsGroup, &runAsGroup)
}

func TestNewPlaceholderWithLabelsAndAnnotations(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]v1alpha1.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("1024M"),
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
		},
	})

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Labels), 5)
	assert.Equal(t, len(holder.pod.Annotations), 5)
	assert.Equal(t, holder.pod.Labels["labelKey0"], "labelKeyValue0")
	assert.Equal(t, holder.pod.Labels["labelKey1"], "labelKeyValue1")
	assert.Equal(t, holder.pod.Annotations["annotationKey0"], "annotationValue0")
	assert.Equal(t, holder.pod.Annotations["annotationKey1"], "annotationValue1")
	assert.Equal(t, holder.pod.Annotations["annotationKey2"], "annotationValue2")
}

func TestNewPlaceholderWithNodeSelectors(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]v1alpha1.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("1024M"),
			},
			NodeSelector: map[string]string{
				"nodeType":  "test",
				"nodeState": "healthy",
			},
		},
	})

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.NodeSelector), 2)
	assert.Equal(t, holder.pod.Spec.NodeSelector["nodeType"], "test")
	assert.Equal(t, holder.pod.Spec.NodeSelector["nodeState"], "healthy")
}

func TestNewPlaceholderWithTolerations(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]v1alpha1.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("1024M"),
			},
			Tolerations: []v1.Toleration{
				{
					Key:      "key1",
					Operator: v1.TolerationOpEqual,
					Value:    "value1",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
		},
	})

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.Tolerations), 1)
	tlr := holder.pod.Spec.Tolerations[0]
	assert.Equal(t, tlr.Key, "key1")
	assert.Equal(t, tlr.Value, "value1")
	assert.Equal(t, tlr.Operator, v1.TolerationOpEqual)
	assert.Equal(t, tlr.Effect, v1.TaintEffectNoSchedule)
}

func TestNewPlaceholderWithAffinity(t *testing.T) {
	const (
		appID     = "app01"
		queue     = "root.default"
		namespace = "test"
	)
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication(appID, queue,
		"bob", map[string]string{constants.AppTagNamespace: namespace}, mockedSchedulerAPI)
	app.setTaskGroups([]v1alpha1.TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 10,
			MinResource: map[string]resource.Quantity{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("1024M"),
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
		},
	})

	holder := newPlaceholder("ph-name", app, app.taskGroups[0])
	assert.Equal(t, len(holder.pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution), 1)
	term := holder.pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	assert.Equal(t, term[0].TopologyKey, "topologyKey")

	assert.Equal(t, len(term[0].LabelSelector.MatchExpressions), 1)
	assert.Equal(t, term[0].LabelSelector.MatchExpressions[0].Key, "service")
	assert.Equal(t, term[0].LabelSelector.MatchExpressions[0].Operator, metav1.LabelSelectorOpIn)
	assert.Equal(t, term[0].LabelSelector.MatchExpressions[0].Values[0], "securityscan")
}

func TestPlaceHolderImageSetFromEnvVar(t *testing.T) {
	os.Setenv("PLACEHOLDER_IMAGE", "abcd")
	placeHolderImage = ""
	defer func() {
		placeHolderImage = ""
		os.Unsetenv("PLACEHOLDER_IMAGE")
	}()

	// first call
	imgName := getPlaceHolderImage()
	assert.Equal(t, imgName, "abcd")
	assert.Equal(t, placeHolderImage, "abcd")

	// second call should have no effect
	os.Setenv("PLACEHOLDER_IMAGE", "xyz")
	imgName = getPlaceHolderImage()
	assert.Equal(t, imgName, "abcd")
	assert.Equal(t, placeHolderImage, "abcd")
}

func TestPlaceHolderImageSetFromDefault(t *testing.T) {
	defer func() {
		placeHolderImage = ""
	}()

	// first call
	imgName := getPlaceHolderImage()
	assert.Equal(t, imgName, "k8s.gcr.io/pause")
	assert.Equal(t, imgName, placeHolderImage)

	// second call, set it directly
	placeHolderImage = "test"
	imgName = getPlaceHolderImage()
	assert.Equal(t, imgName, "test")
}
