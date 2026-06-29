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
	"errors"
	"testing"

	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

func TestClassifyBindFailure_Transient(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		apierrors.NewTimeoutError("timed out", 1),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopeUnknown)
	assert.Equal(t, decision.Durability, BindFailureDurabilityTransient)
	assert.Equal(t, decision.Action, BindFailureActionRetrySameNode)
	assert.Equal(t, decision.Confidence, BindFailureConfidenceHigh)
}

func TestClassifyBindFailure_NodeNotFound(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "node-a"),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopeNode)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionRetryDifferentNode)
	assert.Equal(t, decision.Confidence, BindFailureConfidenceHigh)
}

func TestClassifyBindFailure_ConflictReasons(t *testing.T) {
	err := NewBindFailureConflictError(
		BindFailureStageAssumePod,
		BindFailureOperationFindPodVolumes,
		"pod-a",
		volumebinding.ConflictReasons{
			volumebinding.ConflictReason("NodeVolumeLimitsExceeded"),
		},
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopeNode)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionRetryDifferentNode)
	assert.Equal(t, decision.Confidence, BindFailureConfidenceHigh)
}

func TestClassifyBindFailure_PodForbidden(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "pod-a", errors.New("forbidden")),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopePod)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionFailFast)
	assert.Equal(t, decision.Confidence, BindFailureConfidenceHigh)
}

func TestClassifyBindFailure_AlreadyBoundTreatAsSuccess(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, "pod-a"),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopePod)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionTreatAsSuccess)
	assert.Equal(t, decision.Confidence, BindFailureConfidenceHigh)
}
