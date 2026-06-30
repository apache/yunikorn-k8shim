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
	"k8s.io/apimachinery/pkg/util/validation/field"
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
}

func TestClassifyBindFailure_TooManyRequestsIsTransient(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		apierrors.NewTooManyRequests("throttled", 1),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopeUnknown)
	assert.Equal(t, decision.Durability, BindFailureDurabilityTransient)
	assert.Equal(t, decision.Action, BindFailureActionRetrySameNode)
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
}

func TestClassifyBindFailure_ConflictReasons(t *testing.T) {
	err := NewBindFailureConflictError(
		BindFailureStageAssumePod,
		BindFailureOperationFindPodVolumes,
		"pod-a",
		volumebinding.ConflictReasons{
			volumebinding.ConflictReason(volumebinding.ErrReasonNodeConflict),
		},
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopeNode)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionRetryDifferentNode)
}

func TestClassifyBindFailure_ConflictReasonsPodScoped(t *testing.T) {
	err := NewBindFailureConflictError(
		BindFailureStageAssumePod,
		BindFailureOperationFindPodVolumes,
		"pod-a",
		volumebinding.ConflictReasons{
			volumebinding.ConflictReason(volumebinding.ErrReasonPVNotExist),
		},
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopePod)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionFailFast)
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
}

func TestClassifyBindFailure_PodInvalid(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		apierrors.NewInvalid(
			schema.GroupKind{Group: "", Kind: "Pod"},
			"pod-a",
			field.ErrorList{field.Invalid(field.NewPath("spec", "nodeName"), "", "node is invalid")},
		),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopePod)
	assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
	assert.Equal(t, decision.Action, BindFailureActionFailFast)
}

func TestClassifyBindFailure_NotFoundPodOrPVC(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "pod not found",
			err: WrapBindFailureError(
				BindFailureStageBindPod,
				BindFailureOperationKubeBindPod,
				apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "pod-a"),
			),
		},
		{
			name: "pvc not found",
			err: WrapBindFailureError(
				BindFailureStageBindPodVolumes,
				BindFailureOperationGetPodVolumeClaims,
				apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, "pvc-a"),
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decision := ClassifyBindFailure(tc.err)
			assert.Equal(t, decision.Scope, BindFailureScopePod)
			assert.Equal(t, decision.Durability, BindFailureDurabilityPermanent)
			assert.Equal(t, decision.Action, BindFailureActionFailFast)
		})
	}
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
}

func TestClassifyBindFailure_UnknownRetriesByDefault(t *testing.T) {
	err := WrapBindFailureError(
		BindFailureStageBindPod,
		BindFailureOperationKubeBindPod,
		errors.New("opaque bind failure"),
	)

	decision := ClassifyBindFailure(err)
	assert.Equal(t, decision.Scope, BindFailureScopeUnknown)
	assert.Equal(t, decision.Durability, BindFailureDurabilityUnknown)
	assert.Equal(t, decision.Action, BindFailureActionRetrySameNode)
}
