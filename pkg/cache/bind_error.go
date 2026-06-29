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
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

type BindFailureStage string

const (
	BindFailureStageAssumePod      BindFailureStage = "assume_pod"
	BindFailureStageBindPodVolumes BindFailureStage = "bind_pod_volumes"
	BindFailureStageBindPod        BindFailureStage = "bind_pod"
)

type BindFailureOperation string

const (
	BindFailureOperationGetPodVolumeClaims BindFailureOperation = "get_pod_volume_claims"
	BindFailureOperationGetNodeInfo        BindFailureOperation = "get_node_info"
	BindFailureOperationFindPodVolumes     BindFailureOperation = "find_pod_volumes"
	BindFailureOperationAssumePodVolumes   BindFailureOperation = "assume_pod_volumes"
	BindFailureOperationBindPodVolumes     BindFailureOperation = "bind_pod_volumes"
	BindFailureOperationKubeBindPod        BindFailureOperation = "kube_bind_pod"
)

type BindFailureDetails struct {
	Stage           BindFailureStage
	Operation       BindFailureOperation
	StatusReason    metav1.StatusReason
	StatusCode      int32
	ResourceKind    string
	ResourceName    string
	ConflictReasons volumebinding.ConflictReasons
}

type BindFailureError struct {
	details BindFailureDetails
	cause   error
}

func (e *BindFailureError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *BindFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *BindFailureError) Details() BindFailureDetails {
	if e == nil {
		return BindFailureDetails{}
	}

	details := e.details
	if len(e.details.ConflictReasons) > 0 {
		details.ConflictReasons = append(volumebinding.ConflictReasons(nil), e.details.ConflictReasons...)
	}
	return details
}

func WrapBindFailureError(stage BindFailureStage, operation BindFailureOperation, err error) error {
	if err == nil {
		return nil
	}

	details := BindFailureDetails{
		Stage:     stage,
		Operation: operation,
	}
	enrichBindFailureStatus(&details, err)

	return &BindFailureError{
		details: details,
		cause:   err,
	}
}

func NewBindFailureConflictError(stage BindFailureStage, operation BindFailureOperation, podName string,
	reasons volumebinding.ConflictReasons) error {
	sReasons := make([]string, len(reasons))
	for i, reason := range reasons {
		sReasons[i] = string(reason)
	}

	err := errors.New(fmt.Sprintf("pod %s has conflicting volume claims: %s", podName, strings.Join(sReasons, ", ")))
	return &BindFailureError{
		details: BindFailureDetails{
			Stage:           stage,
			Operation:       operation,
			ConflictReasons: append(volumebinding.ConflictReasons(nil), reasons...),
		},
		cause: err,
	}
}

func GetBindFailureDetails(err error) (BindFailureDetails, bool) {
	var bindErr *BindFailureError
	if !errors.As(err, &bindErr) {
		return BindFailureDetails{}, false
	}
	return bindErr.Details(), true
}

func enrichBindFailureStatus(details *BindFailureDetails, err error) {
	type apiStatus interface {
		Status() metav1.Status
	}

	var statusErr apiStatus
	if !errors.As(err, &statusErr) {
		return
	}

	status := statusErr.Status()
	details.StatusReason = status.Reason
	details.StatusCode = status.Code
	if status.Details != nil {
		details.ResourceKind = status.Details.Kind
		details.ResourceName = status.Details.Name
	}
}
