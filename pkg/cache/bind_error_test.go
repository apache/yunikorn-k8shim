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
	"testing"

	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
)

func TestWrapBindFailureError_PreservesMessageAndStatus(t *testing.T) {
	baseErr := apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "test-pod")
	wrappedErr := WrapBindFailureError(BindFailureStageBindPod, BindFailureOperationKubeBindPod, baseErr)

	assert.Equal(t, wrappedErr.Error(), baseErr.Error())

	details, ok := GetBindFailureDetails(wrappedErr)
	assert.Assert(t, ok)
	assert.Equal(t, details.Stage, BindFailureStageBindPod)
	assert.Equal(t, details.Operation, BindFailureOperationKubeBindPod)
	assert.Equal(t, details.StatusReason, metav1.StatusReasonNotFound)
	assert.Equal(t, details.StatusCode, int32(404))
	assert.Equal(t, details.ResourceKind, "pods")
	assert.Equal(t, details.ResourceName, "test-pod")
}

func TestNewBindFailureConflictError_PreservesConflictReasons(t *testing.T) {
	reasons := volumebinding.ConflictReasons{
		volumebinding.ConflictReason("reason-a"),
		volumebinding.ConflictReason("reason-b"),
	}

	err := NewBindFailureConflictError(
		BindFailureStageAssumePod,
		BindFailureOperationFindPodVolumes,
		"pod-1",
		reasons,
	)

	assert.Equal(t, err.Error(), "pod pod-1 has conflicting volume claims: reason-a, reason-b")

	details, ok := GetBindFailureDetails(err)
	assert.Assert(t, ok)
	assert.Equal(t, details.Stage, BindFailureStageAssumePod)
	assert.Equal(t, details.Operation, BindFailureOperationFindPodVolumes)
	assert.Equal(t, len(details.ConflictReasons), 2)
	assert.Equal(t, string(details.ConflictReasons[0]), "reason-a")
	assert.Equal(t, string(details.ConflictReasons[1]), "reason-b")
}

func TestAssumePodConflictErrorContainsBindFailureDetails(t *testing.T) {
	binder := test.NewVolumeBinderMock()
	binder.SetConflictReasons("reason1", "reason2")
	context := initAssumePodTest(binder)
	defer dispatcher.UnregisterAllEventHandlers()
	defer dispatcher.Stop()

	err := context.AssumePod(pod1UID, fakeNodeName)
	assert.Error(t, err, "pod pod1 has conflicting volume claims: reason1, reason2")

	details, ok := GetBindFailureDetails(err)
	assert.Assert(t, ok)
	assert.Equal(t, details.Stage, BindFailureStageAssumePod)
	assert.Equal(t, details.Operation, BindFailureOperationFindPodVolumes)
	assert.Equal(t, len(details.ConflictReasons), 2)
	assert.Equal(t, string(details.ConflictReasons[0]), "reason1")
	assert.Equal(t, string(details.ConflictReasons[1]), "reason2")
}
