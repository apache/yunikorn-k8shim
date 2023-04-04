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

package admission

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

const testPC = "test-pc"

func TestIsPreemptSelfAllowed(t *testing.T) {
	cache := NewPriorityClassCache(nil)
	cache.priorityClasses["yes"] = true
	cache.priorityClasses["no"] = false

	assert.Check(t, cache.isPreemptSelfAllowed(""), "empty value failed")
	assert.Check(t, cache.isPreemptSelfAllowed("not-found"), "not-found value failed")
	assert.Check(t, cache.isPreemptSelfAllowed("yes"), "yes value failed")
	assert.Check(t, !cache.isPreemptSelfAllowed("no"), "no value failed")
}

func TestPriorityClassHandlers(t *testing.T) {
	kubeClient := client.NewKubeClientMock(false)

	informers := NewInformers(kubeClient, "default")
	cache := NewPriorityClassCache(informers.PriorityClass)
	informers.Start()
	defer informers.Stop()

	assert.Assert(t, cache.isPreemptSelfAllowed(testPC), "non existing, should return true")

	priorityClass := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testPC,
		},
	}

	priorityClasses := kubeClient.GetClientSet().SchedulingV1().PriorityClasses()

	// validate OnAdd
	_, err := priorityClasses.Create(context.Background(), priorityClass, metav1.CreateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.priorityClassExists(testPC)
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	assert.Assert(t, cache.isPreemptSelfAllowed(testPC), "exists, not set should return true")

	// validate OnUpdate
	priorityClass2 := priorityClass.DeepCopy()
	priorityClass2.Annotations = map[string]string{constants.AnnotationAllowPreemption: "false"}

	_, err = priorityClasses.Update(context.Background(), priorityClass2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return !cache.isPreemptSelfAllowed(testPC)
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	// validate OnDelete
	err = priorityClasses.Delete(context.Background(), testPC, metav1.DeleteOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return !cache.priorityClassExists(testPC)
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)
}

func TestGetBoolAnnotation(t *testing.T) {
	tests := map[string]struct {
		annotation map[string]string
		expect     bool
	}{
		"nil annotations": {
			annotation: nil,
			expect:     true,
		},
		"empty annotations": {
			annotation: map[string]string{},
			expect:     true,
		},
		"invalid value": {
			annotation: map[string]string{constants.AnnotationAllowPreemption: "value"},
			expect:     true,
		},
		"valid value": {
			annotation: map[string]string{constants.AnnotationAllowPreemption: "false"},
			expect:     false,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := getAnnotationBoolean(test.annotation, constants.AnnotationAllowPreemption)
			assert.Equal(t, got, test.expect, "value incorrect")
		})
	}
}
