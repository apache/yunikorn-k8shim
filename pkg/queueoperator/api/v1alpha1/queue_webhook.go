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

package v1alpha1

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

// +kubebuilder:webhook:path=/validate-queue-yunikorn-k8s-io-v1alpha1-queue,mutating=false,failurePolicy=fail,sideEffects=None,groups=queue.yunikorn.k8s.io,resources=queues,verbs=create;update,versions=v1alpha1,name=vqueue.queue.yunikorn.k8s.io,admissionReviewVersions=v1

// QueueValidator implements webhook.CustomValidator with three layers of
// admission validation, each layer cheaper than the next:
//
//  1. Per-CR structural checks (cheap, no API calls): name regex, parent/leaf
//     consistency, child-name uniqueness within the CR.
//  2. Cluster-wide queue-name uniqueness (one LIST): no two Queue CRs may
//     claim the same YuniKorn queue name.
//  3. MERGED scheduler-config validation through YuniKorn's own validator
//     (one LIST + YAML render + configs.LoadSchedulerConfigFromByteArray):
//     guarantees that whatever this admission produces, when shipped to the
//     scheduler ConfigMap, will not crash YuniKorn at startup or hot-refresh.
//
// Layer 3 is the fix for the production incident where a malformed config
// reached the scheduler (the in-cluster admission webhook hop in YuniKorn's
// own admission controller can soft-fail when unreachable). We refuse the CR
// at the API server boundary so the bad config never lands in etcd at all.
//
// +kubebuilder:object:generate=false
type QueueValidator struct {
	Client client.Reader

	// BuildOptions is the partition/placement context used to assemble the
	// merged config. It is read once from env vars at SetupWebhookWithManager
	// time so admission-time validation sees the same scheduler topology the
	// reconciler will eventually write.
	BuildOptions queueconfig.BuildOptions
}

// SetupWebhookWithManager registers the webhook with the manager. Build
// options are loaded from env vars (PARTITION_NAME, PLACEMENT_RULES) so the
// webhook validates against the exact partition + placement topology the
// reconciler will ultimately produce.
func (r *Queue) SetupWebhookWithManager(mgr ctrl.Manager) error {
	opts, err := queueconfig.LoadOptionsFromEnv()
	if err != nil {
		// Optional placement rules failed to parse — log and proceed with the
		// defaulted partition. Same softfail behaviour as the reconciler so
		// startup is never blocked by a typo in PLACEMENT_RULES.
		ctrl.Log.WithName("webhook").Error(err,
			"failed to parse PLACEMENT_RULES env, falling back to no placement rules")
	}

	// Use the API reader (direct, uncached) rather than mgr.GetClient()
	// (cached). Admission decisions MUST see the latest committed state of
	// every Queue CR — a stale cache would let two CRs racing for the
	// same spec.queue.name both pass uniqueness validation, or let the
	// merged-config validator miss a sibling that was just admitted.
	return ctrl.NewWebhookManagedBy(mgr, &Queue{}).
		WithValidator(&QueueValidator{
			Client:       mgr.GetAPIReader(),
			BuildOptions: opts,
		}).
		Complete()
}

var _ admission.Validator[*Queue] = &QueueValidator{}

// ValidateCreate validates Queue creation.
func (v *QueueValidator) ValidateCreate(ctx context.Context, queue *Queue) (admission.Warnings, error) {
	return v.validate(ctx, queue)
}

// ValidateUpdate validates Queue updates.
func (v *QueueValidator) ValidateUpdate(ctx context.Context, _, queue *Queue) (admission.Warnings, error) {
	return v.validate(ctx, queue)
}

// ValidateDelete allows Queue deletions without additional checks.
func (v *QueueValidator) ValidateDelete(_ context.Context, _ *Queue) (admission.Warnings, error) {
	return nil, nil
}

// isSelf reports whether existing refers to the same Queue CR as candidate.
//
// Shared by the uniqueness and merged-config layers: both need to "skip" (or
// substitute) the candidate's own entry in the cluster-wide list returned by
// the API reader, otherwise an update would always conflict with itself.
//
// Match precedence:
//
//   - UID equality wins (the candidate has been admitted before and we have
//     its server-assigned UID — this is the precise identifier).
//   - Fallback to namespace+name (used on fresh creates where the candidate
//     has no UID yet; also handles the edge case where the fake client in
//     unit tests stores objects without UIDs).
func isSelf(existing, candidate *Queue) bool {
	if candidate.UID != "" && existing.UID == candidate.UID {
		return true
	}
	return existing.Namespace == candidate.Namespace && existing.Name == candidate.Name
}

// validate applies the three layers in cheap-to-expensive order.
//
// On rejection we ALWAYS log a structured line from the webhook pod, even
// though the API server already returns the error to the client.
//
// Logger keys: controller-runtime's DefaultLogConstructor already attaches
// `object`, `namespace`, `name`, `resource`, `user`, and `requestID` (the
// *admission request* UID, ephemeral per call) to the request-scoped logger
// — we don't repeat them. We only add what isn't already there:
//
//   - queueName: the queue's spec.queue.name, distinct from metadata.name in
//     the rare Layer-1 mismatch case the structural validator catches.
//   - objectUID: the CR's persisted metadata.uid, useful for cross-correlating
//     with reconciler logs (the reconciler tags by object UID, not the
//     admission requestID).
func (v *QueueValidator) validate(ctx context.Context, queue *Queue) (admission.Warnings, error) {
	log := logf.FromContext(ctx).WithValues(
		"queueName", queue.Spec.Queue.Name,
		"objectUID", queue.UID,
	)

	if warnings, err := queue.validateQueue(); err != nil {
		log.Info("queue admission rejected", "layer", "1-structural", "reason", err.Error())
		return warnings, err
	}
	if err := v.validateUniqueQueueName(ctx, queue); err != nil {
		log.Info("queue admission rejected", "layer", "2-uniqueness", "reason", err.Error())
		return nil, err
	}
	if err := v.validateMergedConfig(ctx, queue); err != nil {
		log.Info("queue admission rejected", "layer", "3-merged-config", "reason", err.Error())
		return nil, err
	}
	return nil, nil
}

// validateUniqueQueueName checks that no other Queue CR in the cluster uses the same spec.queue.name.
func (v *QueueValidator) validateUniqueQueueName(ctx context.Context, queue *Queue) error {
	queueList := &QueueList{}
	if err := v.Client.List(ctx, queueList); err != nil {
		return fmt.Errorf("failed to list Queue CRs for duplicate check: %w", err)
	}

	for i := range queueList.Items {
		existing := &queueList.Items[i]
		// Skip self (for updates — the CR being updated will appear in the list)
		if isSelf(existing, queue) {
			continue
		}
		if existing.Spec.Queue.Name == queue.Spec.Queue.Name {
			return apierrors.NewInvalid(
				schema.GroupKind{Group: GroupVersion.Group, Kind: "Queue"},
				queue.Name,
				field.ErrorList{
					field.Invalid(
						field.NewPath("spec", "queue", "name"),
						queue.Spec.Queue.Name,
						fmt.Sprintf("queue name %q is already used by Queue CR %s/%s",
							queue.Spec.Queue.Name, existing.Namespace, existing.Name),
					),
				},
			)
		}
	}

	return nil
}

// validateMergedConfig builds the full YuniKorn scheduler config that would
// result from admitting this CR alongside every other Queue CR currently in
// the cluster, then runs it through YuniKorn's own validator.
//
// On rejection the error is returned as a structured field.Error so the user
// sees a clean kubectl message rather than a wall of YAML. The full rendered
// YAML lives in the wrapped *queueconfig.ValidationError for operators
// digging through controller logs.
func (v *QueueValidator) validateMergedConfig(ctx context.Context, queue *Queue) error {
	queueList := &QueueList{}
	if err := v.Client.List(ctx, queueList); err != nil {
		return fmt.Errorf("failed to list Queue CRs for merged-config validation: %w", err)
	}

	// Replace the existing entry for this CR (matched by UID for updates, or
	// by namespace/name when UID isn't set yet on a fresh create) with the
	// candidate so validation reflects what the cluster will look like AFTER
	// this admission decision.
	merged := make([]Queue, 0, len(queueList.Items)+1)
	replaced := false
	for i := range queueList.Items {
		existing := queueList.Items[i]
		if isSelf(&existing, queue) {
			merged = append(merged, *queue)
			replaced = true
			continue
		}
		merged = append(merged, existing)
	}
	if !replaced {
		merged = append(merged, *queue)
	}

	merged = SortByNamespaceName(merged)

	cfg := queueconfig.BuildMerged(L1QueuesFromCRs(merged), v.BuildOptions)
	_, _, err := queueconfig.Validate(cfg)
	if err == nil {
		return nil
	}

	var ve *queueconfig.ValidationError
	if errors.As(err, &ve) {
		return apierrors.NewInvalid(
			schema.GroupKind{Group: GroupVersion.Group, Kind: "Queue"},
			queue.Name,
			field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "queue"),
					queue.Spec.Queue.Name,
					fmt.Sprintf("merged YuniKorn scheduler config is invalid (%s stage): %v",
						ve.Stage, ve.Err),
				),
			},
		)
	}
	return err
}

func (r *Queue) validateQueue() (admission.Warnings, error) {
	var allErrs field.ErrorList

	if r.Spec.Queue.Name != r.Name {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "queue", "name"),
			r.Spec.Queue.Name,
			"must match metadata.name",
		))
	}

	allErrs = append(allErrs, validateQueueConfig(r.Spec.Queue, field.NewPath("spec", "queue"))...)

	if len(allErrs) == 0 {
		return nil, nil
	}

	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: GroupVersion.Group, Kind: "Queue"},
		r.Name,
		allErrs,
	)
}

func validateQueueConfig(queue QueueConfig, path *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if queue.Parent != nil && !*queue.Parent && len(queue.Queues) > 0 {
		allErrs = append(allErrs, field.Invalid(
			path.Child("queues"),
			queue.Queues,
			"queues must be empty when parent is false",
		))
	}

	seen := make(map[string]int, len(queue.Queues))
	for i, child := range queue.Queues {
		childPath := path.Child("queues").Index(i)
		if child.Name == queue.Name {
			allErrs = append(allErrs, field.Invalid(
				childPath.Child("name"),
				child.Name,
				"child queue name must differ from parent",
			))
		}

		if firstIdx, exists := seen[child.Name]; exists {
			allErrs = append(allErrs, field.Duplicate(
				childPath.Child("name"),
				queue.Queues[firstIdx].Name,
			))
		} else {
			seen[child.Name] = i
		}

		allErrs = append(allErrs, validateQueueConfig(child, childPath)...)
	}

	return allErrs
}
