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
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

type BindFailureScope string

const (
	BindFailureScopeNode    BindFailureScope = "node"
	BindFailureScopePod     BindFailureScope = "pod"
	BindFailureScopeRequest BindFailureScope = "request"
	BindFailureScopeUnknown BindFailureScope = "unknown"
)

type BindFailureDurability string

const (
	BindFailureDurabilityTransient BindFailureDurability = "transient"
	BindFailureDurabilityPermanent BindFailureDurability = "permanent"
	BindFailureDurabilityUnknown   BindFailureDurability = "unknown"
)

type BindFailureAction string

const (
	BindFailureActionRetrySameNode      BindFailureAction = "retry_same_node"
	BindFailureActionRetryDifferentNode BindFailureAction = "retry_different_node"
	BindFailureActionTreatAsSuccess     BindFailureAction = "treat_as_success"
	BindFailureActionFailFast           BindFailureAction = "fail_fast"
)

type BindFailureConfidence string

const (
	BindFailureConfidenceHigh BindFailureConfidence = "high"
	BindFailureConfidenceLow  BindFailureConfidence = "low"
)

type BindFailureDecision struct {
	Scope      BindFailureScope
	Durability BindFailureDurability
	Action     BindFailureAction
	Confidence BindFailureConfidence
	Reason     string
}

func ClassifyBindFailure(err error) BindFailureDecision {
	if err == nil {
		return BindFailureDecision{
			Scope:      BindFailureScopeUnknown,
			Durability: BindFailureDurabilityUnknown,
			Action:     BindFailureActionFailFast,
			Confidence: BindFailureConfidenceLow,
			Reason:     "no error to classify",
		}
	}

	details, hasDetails := GetBindFailureDetails(err)

	if hasDetails && details.Stage == BindFailureStageBindPod && apierrors.IsAlreadyExists(err) {
		return BindFailureDecision{
			Scope:      BindFailureScopePod,
			Durability: BindFailureDurabilityPermanent,
			Action:     BindFailureActionTreatAsSuccess,
			Confidence: BindFailureConfidenceHigh,
			Reason:     "pod bind is already recorded",
		}
	}

	if isTransientBindFailure(err) {
		return BindFailureDecision{
			Scope:      BindFailureScopeUnknown,
			Durability: BindFailureDurabilityTransient,
			Action:     BindFailureActionRetrySameNode,
			Confidence: BindFailureConfidenceHigh,
			Reason:     "kubernetes API reported transient condition",
		}
	}

	if hasDetails {
		if len(details.ConflictReasons) > 0 {
			return classifyVolumeConflictReasons(details.ConflictReasons)
		}

		resourceKind := strings.ToLower(details.ResourceKind)
		if apierrors.IsNotFound(err) {
			switch resourceKind {
			case "node", "nodes":
				return BindFailureDecision{
					Scope:      BindFailureScopeNode,
					Durability: BindFailureDurabilityPermanent,
					Action:     BindFailureActionRetryDifferentNode,
					Confidence: BindFailureConfidenceHigh,
					Reason:     "target node was not found",
				}
			case "pod", "pods", "persistentvolumeclaim", "persistentvolumeclaims", "persistentvolume", "persistentvolumes":
				return BindFailureDecision{
					Scope:      BindFailureScopePod,
					Durability: BindFailureDurabilityPermanent,
					Action:     BindFailureActionFailFast,
					Confidence: BindFailureConfidenceHigh,
					Reason:     "pod-scoped object required for binding was not found",
				}
			default:
				return BindFailureDecision{
					Scope:      BindFailureScopeRequest,
					Durability: BindFailureDurabilityPermanent,
					Action:     BindFailureActionFailFast,
					Confidence: BindFailureConfidenceLow,
					Reason:     "resource required for request processing was not found",
				}
			}
		}
	}

	if apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsBadRequest(err) {
		return BindFailureDecision{
			Scope:      BindFailureScopePod,
			Durability: BindFailureDurabilityPermanent,
			Action:     BindFailureActionFailFast,
			Confidence: BindFailureConfidenceHigh,
			Reason:     "request payload or permissions are permanently invalid for this pod",
		}
	}

	if apierrors.IsConflict(err) {
		return BindFailureDecision{
			Scope:      BindFailureScopePod,
			Durability: BindFailureDurabilityTransient,
			Action:     BindFailureActionRetrySameNode,
			Confidence: BindFailureConfidenceLow,
			Reason:     "optimistic concurrency conflict while writing bind state",
		}
	}

	return BindFailureDecision{
		Scope:      BindFailureScopeUnknown,
		Durability: BindFailureDurabilityUnknown,
		Action:     BindFailureActionFailFast,
		Confidence: BindFailureConfidenceLow,
		Reason:     "no reliable classifier match",
	}
}

func classifyVolumeConflictReasons(reasons volumebinding.ConflictReasons) BindFailureDecision {
	hasNodeScopedReason := false
	hasPodScopedReason := false

	for _, reason := range reasons {
		if isPodScopedVolumeConflictReason(reason) {
			hasPodScopedReason = true
			continue
		}
		if isNodeScopedVolumeConflictReason(reason) {
			hasNodeScopedReason = true
		}
	}

	if hasPodScopedReason {
		return BindFailureDecision{
			Scope:      BindFailureScopePod,
			Durability: BindFailureDurabilityPermanent,
			Action:     BindFailureActionFailFast,
			Confidence: BindFailureConfidenceHigh,
			Reason:     "volume conflict reasons indicate pod-scoped incompatibility",
		}
	}

	if hasNodeScopedReason {
		return BindFailureDecision{
			Scope:      BindFailureScopeNode,
			Durability: BindFailureDurabilityPermanent,
			Action:     BindFailureActionRetryDifferentNode,
			Confidence: BindFailureConfidenceHigh,
			Reason:     "volume conflict reasons indicate node-scoped incompatibility",
		}
	}

	return BindFailureDecision{
		Scope:      BindFailureScopePod,
		Durability: BindFailureDurabilityPermanent,
		Action:     BindFailureActionFailFast,
		Confidence: BindFailureConfidenceLow,
		Reason:     "volume conflict reason is unknown, treating as pod-scoped",
	}
}

func isNodeScopedVolumeConflictReason(reason volumebinding.ConflictReason) bool {
	normalized := strings.ToLower(strings.TrimSpace(string(reason)))
	if normalized == "" {
		return false
	}

	switch normalized {
	case strings.ToLower(string(volumebinding.ErrReasonBindConflict)):
		return true
	case strings.ToLower(string(volumebinding.ErrReasonNodeConflict)):
		return true
	case strings.ToLower(volumebinding.ErrReasonNotEnoughSpace):
		return true
	case "nodevolumelimitsexceeded":
		return true
	}

	return strings.Contains(normalized, "node")
}

func isPodScopedVolumeConflictReason(reason volumebinding.ConflictReason) bool {
	normalized := strings.ToLower(strings.TrimSpace(string(reason)))
	if normalized == "" {
		return false
	}

	if normalized == strings.ToLower(volumebinding.ErrReasonPVNotExist) {
		return true
	}

	return strings.Contains(normalized, "pvc") ||
		strings.Contains(normalized, "persistentvolumeclaim") ||
		strings.Contains(normalized, "non-existent pv")
}

func isTransientBindFailure(err error) bool {
	return apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err)
}
