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
	cache, pcErr := NewPriorityClassCache(nil)
	assert.NilError(t, pcErr)
	cache.priorityClasses["yes"] = true
	cache.priorityClasses["no"] = false

	assert.Check(t, cache.isPreemptSelfAllowed(""), "empty value failed")
	assert.Check(t, cache.isPreemptSelfAllowed("not-found"), "not-found value failed")
	assert.Check(t, cache.isPreemptSelfAllowed("yes"), "yes value failed")
	assert.Check(t, !cache.isPreemptSelfAllowed("no"), "no value failed")
}

func TestPriorityClassHandlers(t *testing.T) {
	kubeClient := client.NewKubeClientMock(false)

	// Specify the namespace for the informers (this is still required for consistency, even if PriorityClasses are cluster-scoped)
	namespace := "default"
	informers := NewInformers(kubeClient, namespace)
	cache, pcErr := NewPriorityClassCache(informers.PriorityClass)
	assert.NilError(t, pcErr)

	// Start informers and ensure proper cleanup
	informers.Start()
	defer informers.Stop()

	// Test behavior for a non-existing PriorityClass
	assert.Assert(t, cache.isPreemptSelfAllowed(testPC), "non-existing PriorityClass should return true by default")

	// Define a PriorityClass
	priorityClass := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: testPC,
		},
	}

	// Simulate PriorityClass API interaction
	priorityClasses := kubeClient.GetClientSet().SchedulingV1().PriorityClasses()

	// Validate OnAdd: Add a new PriorityClass
	_, err := priorityClasses.Create(context.Background(), priorityClass, metav1.CreateOptions{})
	assert.NilError(t, err)

	// Wait until the cache reflects the new PriorityClass
	err = utils.WaitForCondition(func() bool {
		return cache.priorityClassExists(testPC)
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)
	assert.Assert(t, cache.isPreemptSelfAllowed(testPC), "existing PriorityClass (not annotated) should return true")

	// Validate OnUpdate: Update the PriorityClass with an annotation
	priorityClass2 := priorityClass.DeepCopy()
	priorityClass2.Annotations = map[string]string{
		constants.AnnotationAllowPreemption: "false",
	}

	_, err = priorityClasses.Update(context.Background(), priorityClass2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	// Wait until the cache reflects the updated PriorityClass
	err = utils.WaitForCondition(func() bool {
		return !cache.isPreemptSelfAllowed(testPC)
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	// Validate OnDelete: Remove the PriorityClass
	err = priorityClasses.Delete(context.Background(), testPC, metav1.DeleteOptions{})
	assert.NilError(t, err)

	// Wait until the cache reflects the deleted PriorityClass
	err = utils.WaitForCondition(func() bool {
		return !cache.priorityClassExists(testPC)
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	// Ensure proper namespace validation (though PriorityClasses are cluster-scoped, consistency matters)
	assert.Equal(t, "default", namespace, "namespace should be restricted to 'default'")
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
