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

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

func TestIsPreemptSelfAllowed(t *testing.T) {
	cache := NewPriorityClassCache()
	cache.priorityClasses["yes"] = &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "yes",
			Annotations: map[string]string{constants.AnnotationAllowPreemption: constants.True}},
	}
	cache.priorityClasses["no"] = &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "no",
			Annotations: map[string]string{constants.AnnotationAllowPreemption: constants.False}},
	}
	cache.priorityClasses["invalid"] = &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "no",
			Annotations: map[string]string{constants.AnnotationAllowPreemption: "invalid"}},
	}
	cache.priorityClasses["default"] = &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
	}

	assert.Check(t, cache.IsPreemptSelfAllowed(""), "empty value failed")
	assert.Check(t, cache.IsPreemptSelfAllowed("not-found"), "not-found value failed")
	assert.Check(t, cache.IsPreemptSelfAllowed("invalid"), "invalid value failed")
	assert.Check(t, cache.IsPreemptSelfAllowed("default"), "default value failed")
	assert.Check(t, cache.IsPreemptSelfAllowed("yes"), "yes value failed")
	assert.Check(t, !cache.IsPreemptSelfAllowed("no"), "no value failed")
}

func TestRegisterHandlers(t *testing.T) {
	kubeClient := client.NewKubeClientMock(false)

	informers := NewInformers(kubeClient, "default")
	cache := NewPriorityClassCache()
	cache.RegisterHandlers(informers.PriorityClass)
	informers.Start()
	defer informers.Stop()

	policy := v1.PreemptNever
	priorityClass := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pc",
		},
		Value:            1000,
		PreemptionPolicy: &policy,
	}

	priorityClasses := kubeClient.GetClientSet().SchedulingV1().PriorityClasses()

	// validate OnAdd
	_, err := priorityClasses.Create(context.Background(), priorityClass, metav1.CreateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.getPriorityClass("test-pc") != nil
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	pc := cache.getPriorityClass("test-pc")
	assert.Assert(t, pc != nil)
	assert.Equal(t, pc.Value, int32(1000))

	// validate OnUpdate
	priorityClass2 := priorityClass.DeepCopy()
	priorityClass2.Value = 1001

	_, err = priorityClasses.Update(context.Background(), priorityClass2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		value := cache.getPriorityClass("test-pc")
		if value == nil {
			return false
		}
		return value.Value == 1001
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	pc = cache.getPriorityClass("test-pc")
	assert.Assert(t, pc != nil)
	assert.Equal(t, pc.Value, int32(1001))

	// validate OnDelete
	err = priorityClasses.Delete(context.Background(), priorityClass.Name, metav1.DeleteOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.getPriorityClass("test-pc") == nil
	}, 10*time.Millisecond, 10*time.Second)
	assert.NilError(t, err)

	pc = cache.getPriorityClass("test-pc")
	assert.Assert(t, pc == nil)
}
