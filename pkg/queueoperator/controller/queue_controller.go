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

package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

const (
	ConfigMapName          = "yunikorn-configs"
	ConfigMapQueueKey      = "queues.yaml"
	DefaultTargetNamespace = "flowsnake-yunikorn"
	TargetNamespaceEnvVar  = "TARGET_NAMESPACE"

	// The partition/placement env vars and their default are defined in
	// internal/queueconfig (single source of truth — the webhook reads them
	// the same way). We re-export them here so existing callers and tests
	// don't need to learn the new import path.
	PartitionNameEnvVar  = queueconfig.PartitionNameEnvVar
	PlacementRulesEnvVar = queueconfig.PlacementRulesEnvVar
	DefaultPartitionName = queueconfig.DefaultPartitionName
	RootQueueName        = queueconfig.RootQueueName

	// Event reasons emitted by the controller.
	EventReasonConfigMapCreated      = "ConfigMapCreated"
	EventReasonConfigMapUpdated      = "ConfigMapUpdated"
	EventReasonConfigMapUnchanged    = "ConfigMapUnchanged"
	EventReasonConfigMapUpdateFailed = "ConfigMapUpdateFailed"
	EventReasonQueueConfigured       = "QueueConfigured"
	EventReasonDuplicateQueueSkipped = "DuplicateQueueSkipped"
	// EventReasonInvalidMergedConfig fires when the assembled YuniKorn config
	// fails YuniKorn's own validator. The reconciler refuses to write the
	// ConfigMap in this case (defense in depth — the webhook should have
	// already caught it, but we never want a bad config to land in the
	// scheduler regardless of how the CR got into etcd).
	EventReasonInvalidMergedConfig = "InvalidMergedConfig"
)

// QueueReconciler reconciles a Queue object.
//
// PartitionName and PlacementRules are stored as separate fields (rather than
// as a single queueconfig.BuildOptions) to keep the existing test surface
// (`PartitionName: "default"`) intact. They're assembled into a BuildOptions
// at use time so the reconciler and the webhook always feed the SAME builder.
type QueueReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	TargetNamespace string
	PartitionName   string
	PlacementRules  []queueconfig.PlacementRule
}

// buildOptions assembles the QueueReconciler's partition + placement settings
// into the queueconfig.BuildOptions shape consumed by BuildMerged.
func (r *QueueReconciler) buildOptions() queueconfig.BuildOptions {
	return queueconfig.BuildOptions{
		PartitionName:  r.PartitionName,
		PlacementRules: r.PlacementRules,
	}
}

// +kubebuilder:rbac:groups=queue.yunikorn.k8s.io,resources=queues,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=queue.yunikorn.k8s.io,resources=queues/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=queue.yunikorn.k8s.io,resources=queues/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// When any Queue CR is created, updated, or deleted, this function:
// 1. Lists all Queue CRs across all namespaces
// 2. Builds the merged YuniKorn scheduler config via queueconfig.BuildMerged
// 3. Validates the result through YuniKorn's own validator
// 4. Creates or updates the ConfigMap in the target namespace
// The ConfigMap is yunikorn-configs in namespace flowsnake-yunikorn (or TARGET_NAMESPACE).
// YuniKorn expects the queue configurations as a configmap.
func (r *QueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	startTime := time.Now()
	log := logf.FromContext(ctx).WithValues("queue", req.NamespacedName)
	log.Info("Reconciling Queue CR")

	result, reconcileErr := r.reconcileInner(ctx, req)

	resultLabel := "success"
	if reconcileErr != nil {
		resultLabel = "error"
	}
	reconcileTotal.WithLabelValues(resultLabel).Inc()
	reconcileDuration.WithLabelValues(resultLabel).Observe(time.Since(startTime).Seconds())

	return result, reconcileErr
}

// reconcileInner is the (thin) orchestrator. Every interesting piece of
// logic lives in a focused phase helper below so this function reads as
// a flat sequence: dedupe → build → validate → apply. Each phase owns its
// own error handling + status updates; this function only short-circuits
// on the failures that cannot be expressed via Degraded conditions.
func (r *QueueReconciler) reconcileInner(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("queue", req.NamespacedName)

	validItems, duplicateItems, err := r.listAndDedupeQueues(ctx, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	cfg := r.buildAndInstrumentMerged(log, validItems)

	yamlBytes := r.validateMergedOrDegrade(ctx, log, cfg, validItems, duplicateItems)
	if yamlBytes == nil {
		// validateMergedOrDegrade has already updated status, emitted
		// events, and ticked metrics. Returning nil keeps the workqueue
		// from retrying — a fresh CR change will trigger reconcile again.
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.applyConfigMap(ctx, log, yamlBytes, validItems, duplicateItems)
}

// listAndDedupeQueues fetches every Queue CR in the cluster, sorts them by
// creation timestamp (so the oldest CR wins any name collision), and
// partitions the result into:
//
//   - validItems: CRs that will be grafted into the merged config
//   - duplicateItems: CRs skipped because another CR claimed their
//     spec.queue.name first
//
// Duplicate skip events + the queues_duplicate_skipped counter are emitted
// here so callers don't have to think about them.
func (r *QueueReconciler) listAndDedupeQueues(ctx context.Context, log logr.Logger) ([]queuev1alpha1.Queue, []duplicateInfo, error) {
	queueList := &queuev1alpha1.QueueList{}
	if err := r.List(ctx, queueList); err != nil {
		log.Error(err, "Failed to list Queue CRs")
		return nil, nil, err
	}

	log.Info("Found Queue CRs", "count", len(queueList.Items))
	if len(queueList.Items) == 0 {
		log.Info("No Queue CRs found, generating empty root queue configuration")
	}

	// Oldest first → deterministic duplicate resolution.
	sort.Slice(queueList.Items, func(i, j int) bool {
		return queueList.Items[i].CreationTimestamp.Before(&queueList.Items[j].CreationTimestamp)
	})

	seenQueues := make(map[string]string, len(queueList.Items))
	validItems := make([]queuev1alpha1.Queue, 0, len(queueList.Items))
	var duplicateItems []duplicateInfo

	for i := range queueList.Items {
		cr := &queueList.Items[i]
		name := cr.Spec.Queue.Name
		keptCR, exists := seenQueues[name]
		if !exists {
			seenQueues[name] = fmt.Sprintf("%s/%s", cr.Namespace, cr.Name)
			validItems = append(validItems, *cr)
			continue
		}
		log.Error(fmt.Errorf("duplicate queue name %q", name),
			"Skipping Queue CR with duplicate queue name to prevent invalid YuniKorn config",
			"keptCR", keptCR,
			"skippedCR", fmt.Sprintf("%s/%s", cr.Namespace, cr.Name),
		)
		queuesDuplicateSkippedTotal.Inc()
		r.emitEvent(cr, corev1.EventTypeWarning, EventReasonDuplicateQueueSkipped,
			fmt.Sprintf("Queue name %q already claimed by %s; this CR is excluded from the ConfigMap", name, keptCR))
		duplicateItems = append(duplicateItems, duplicateInfo{queue: *cr, keptBy: keptCR})
	}

	queuesTotal.Set(float64(len(validItems)))
	return validItems, duplicateItems, nil
}

// buildAndInstrumentMerged builds the merged scheduler config via the
// shared queueconfig package (so the webhook and the reconciler produce
// byte-identical YAML) and updates the per-L1 hierarchy-depth gauge as a
// side effect. Returns the assembled SchedulerConfig for downstream
// validation + ConfigMap write.
func (r *QueueReconciler) buildAndInstrumentMerged(log logr.Logger, validItems []queuev1alpha1.Queue) *queueconfig.SchedulerConfig {
	cfg := queueconfig.BuildMerged(
		queuev1alpha1.L1QueuesFromCRs(validItems),
		r.buildOptions(),
	)
	for _, l1 := range cfg.Partitions[0].Queues[0].Queues {
		queueHierarchyDepth.WithLabelValues(l1.Name).Set(float64(computeHierarchyDepth(l1)))
		log.Info("Converted Queue CR", "queueName", l1.Name)
	}
	return cfg
}

// validateMergedOrDegrade runs cfg through YuniKorn's own validator
// (defense in depth — the webhook should have caught this already, but
// we never want a bad config to land in the scheduler ConfigMap
// regardless of how the CR got into etcd).
//
// On success returns the rendered YAML so callers don't need a separate
// yaml.Marshal step.
//
// On failure the function takes full ownership of the failure path:
// metrics are ticked (configmap-invalid + the webhook-missed observability
// counter), the failure is attributed to the specific offending CR(s),
// events are emitted, and statuses are reconciled. A nil return signals
// "validation failed, caller should short-circuit with a no-requeue
// Result{}".
func (r *QueueReconciler) validateMergedOrDegrade(
	ctx context.Context,
	log logr.Logger,
	cfg *queueconfig.SchedulerConfig,
	validItems []queuev1alpha1.Queue,
	duplicateItems []duplicateInfo,
) []byte {
	yamlBytes, _, err := queueconfig.Validate(cfg)
	if err == nil {
		return yamlBytes
	}

	log.Error(err, "Generated YuniKorn configuration is invalid; refusing to write ConfigMap")
	configMapUpdatesTotal.WithLabelValues("invalid").Inc()
	recordWebhookMissed(err)

	// Attribute the failure to the specific CR(s) that are individually
	// invalid so we only mark THOSE as Degraded — not every unrelated CR
	// in the cluster. The previous (last-known-good) ConfigMap is still
	// in place serving the good CRs.
	_, badItems, perCRReasons := findInvalidCRs(validItems, r.buildOptions())
	if len(badItems) == 0 {
		// In this operator's L1-CR design this branch is unreachable:
		// every CR is independent at the root grafting point, so the
		// merge fails iff some individual CR fails. We keep the fallback
		// so a future change that introduces cross-CR coupling never
		// silently swallows the failure.
		r.degradeAllAsFallback(ctx, err, validItems, duplicateItems)
		return nil
	}

	r.degradeAttributedBadCRs(ctx, log, badItems, perCRReasons)
	r.degradeDuplicates(ctx, log, duplicateItems)

	log.Info("Refused to write ConfigMap; only invalid CR(s) marked Degraded",
		"invalidCount", len(badItems),
		"validButBlockedCount", len(validItems)-len(badItems),
	)
	return nil
}

// recordWebhookMissed increments the trust-but-verify counter for the
// admission webhook, tagged with the YuniKorn pipeline stage the
// reconciler caught at.
//
// A non-zero rate alert on this counter is how on-call learns that the
// webhook stopped doing its job (disabled / failurePolicy: Ignore /
// pre-existing bad CRs / validation drift). See metrics.go for the
// alert recommendation.
func recordWebhookMissed(err error) {
	stage := "unknown"
	var ve *queueconfig.ValidationError
	if errors.As(err, &ve) {
		stage = ve.Stage
	}
	webhookMissedInvalidTotal.WithLabelValues(stage).Inc()
}

// degradeAllAsFallback is the legacy "everyone is degraded" path used only
// when findInvalidCRs could not attribute the merge failure to a specific
// CR. See validateMergedOrDegrade for the reachability note.
func (r *QueueReconciler) degradeAllAsFallback(
	ctx context.Context,
	err error,
	validItems []queuev1alpha1.Queue,
	duplicateItems []duplicateInfo,
) {
	stage := "validate"
	underlying := err
	var ve *queueconfig.ValidationError
	if errors.As(err, &ve) {
		stage = ve.Stage
		if ve.Err != nil {
			underlying = ve.Err
		}
	}
	msg := fmt.Sprintf("YuniKorn config %s failed (could not attribute to a specific CR): %v", stage, underlying)
	for i := range validItems {
		r.emitEvent(&validItems[i], corev1.EventTypeWarning, EventReasonInvalidMergedConfig, msg)
	}
	r.updateAllQueueStatus(ctx, validItems, duplicateItems, false, msg)
}

// degradeAttributedBadCRs marks only the offending CRs as Degraded with
// their per-CR YuniKorn rejection reason. Good CRs are intentionally left
// untouched — the last-known-good ConfigMap is still in place serving
// them, and a poisoned CR must not clobber an unrelated team's status.
func (r *QueueReconciler) degradeAttributedBadCRs(
	ctx context.Context,
	log logr.Logger,
	badItems []queuev1alpha1.Queue,
	perCRReasons map[string]invalidCRReason,
) {
	for i := range badItems {
		bad := &badItems[i]
		reason := perCRReasons[crKey(bad.Namespace, bad.Name)]
		msg := fmt.Sprintf("YuniKorn config %s failed: %s", reason.stage, reason.message)
		r.emitEvent(bad, corev1.EventTypeWarning, EventReasonInvalidMergedConfig, msg)
		if updateErr := r.updateQueueStatusDegraded(ctx, bad, StatusReasonInvalidConfig, msg); updateErr != nil {
			log.Error(updateErr, "Failed to update status for invalid queue", "queue", bad.Name, "namespace", bad.Namespace)
		}
	}
}

// degradeDuplicates refreshes the Degraded condition on every duplicate
// CR. Called from the validate-failure path so duplicate statuses don't
// silently grow stale when the merge would also have failed.
func (r *QueueReconciler) degradeDuplicates(
	ctx context.Context,
	log logr.Logger,
	duplicateItems []duplicateInfo,
) {
	for _, dup := range duplicateItems {
		q := &dup.queue
		dupMsg := fmt.Sprintf("Duplicate queue name %q; already claimed by %s", q.Spec.Queue.Name, dup.keptBy)
		if updateErr := r.updateQueueStatusDegraded(ctx, q, StatusReasonDuplicateQueueName, dupMsg); updateErr != nil {
			log.Error(updateErr, "Failed to update status for duplicate queue", "queue", q.Name, "namespace", q.Namespace)
		}
	}
}

// applyConfigMap reconciles the YuniKorn ConfigMap to contain yamlBytes.
// Delegates the three terminal outcomes (Create, Update, no-op) to focused
// helpers and only owns the Get + dispatch logic itself.
func (r *QueueReconciler) applyConfigMap(
	ctx context.Context,
	log logr.Logger,
	yamlBytes []byte,
	validItems []queuev1alpha1.Queue,
	duplicateItems []duplicateInfo,
) error {
	yamlString := string(yamlBytes)
	log.Info("Generated YuniKorn configuration", "yamlLength", len(yamlString))

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigMapName,
			Namespace: r.TargetNamespace,
		},
	}

	err := r.Get(ctx, types.NamespacedName{
		Name:      ConfigMapName,
		Namespace: r.TargetNamespace,
	}, configMap)
	if apierrors.IsNotFound(err) {
		return r.createConfigMap(ctx, log, configMap, yamlString, validItems, duplicateItems)
	}
	if err != nil {
		log.Error(err, "Failed to get ConfigMap", "namespace", r.TargetNamespace)
		configMapUpdatesTotal.WithLabelValues("error").Inc()
		r.emitConfigMapEvent(configMap, corev1.EventTypeWarning, EventReasonConfigMapUpdateFailed,
			fmt.Sprintf("Failed to get ConfigMap: %v", err))
		r.updateAllQueueStatus(ctx, validItems, duplicateItems, false, fmt.Sprintf("ConfigMap get failed: %v", err))
		return fmt.Errorf("failed to get ConfigMap: %w", err)
	}
	return r.updateConfigMapIfChanged(ctx, log, configMap, yamlString, validItems, duplicateItems)
}

// createConfigMap handles the "ConfigMap does not exist yet" path.
func (r *QueueReconciler) createConfigMap(
	ctx context.Context,
	log logr.Logger,
	configMap *corev1.ConfigMap,
	yamlString string,
	validItems []queuev1alpha1.Queue,
	duplicateItems []duplicateInfo,
) error {
	configMap.Data = map[string]string{ConfigMapQueueKey: yamlString}
	if err := r.Create(ctx, configMap); err != nil {
		log.Error(err, "Failed to create ConfigMap", "namespace", r.TargetNamespace)
		configMapUpdatesTotal.WithLabelValues("error").Inc()
		r.emitConfigMapEvent(configMap, corev1.EventTypeWarning, EventReasonConfigMapUpdateFailed,
			fmt.Sprintf("Failed to create ConfigMap: %v", err))
		r.updateAllQueueStatus(ctx, validItems, duplicateItems, false, fmt.Sprintf("ConfigMap create failed: %v", err))
		return fmt.Errorf("failed to create ConfigMap: %w", err)
	}
	log.Info("Created ConfigMap", "namespace", r.TargetNamespace, "name", ConfigMapName)
	configMapUpdatesTotal.WithLabelValues("success").Inc()
	r.emitConfigMapEvent(configMap, corev1.EventTypeNormal, EventReasonConfigMapCreated,
		fmt.Sprintf("Created ConfigMap with %d queue(s)", len(validItems)))
	r.emitPerQueueEvents(validItems)
	r.updateAllQueueStatus(ctx, validItems, duplicateItems, true, "")
	return nil
}

// updateConfigMapIfChanged handles the "ConfigMap already exists" path,
// turning an unchanged-payload reconcile into a no-op (skips the apiserver
// write) and an actual diff into an Update.
func (r *QueueReconciler) updateConfigMapIfChanged(
	ctx context.Context,
	log logr.Logger,
	configMap *corev1.ConfigMap,
	yamlString string,
	validItems []queuev1alpha1.Queue,
	duplicateItems []duplicateInfo,
) error {
	if configMap.Data == nil {
		configMap.Data = make(map[string]string)
	}
	if configMap.Data[ConfigMapQueueKey] == yamlString {
		log.Info("ConfigMap is already up to date", "namespace", r.TargetNamespace)
		configMapUpdatesTotal.WithLabelValues("noop").Inc()
		r.updateAllQueueStatus(ctx, validItems, duplicateItems, true, "")
		return nil
	}
	configMap.Data[ConfigMapQueueKey] = yamlString
	if err := r.Update(ctx, configMap); err != nil {
		log.Error(err, "Failed to update ConfigMap", "namespace", r.TargetNamespace)
		configMapUpdatesTotal.WithLabelValues("error").Inc()
		r.emitConfigMapEvent(configMap, corev1.EventTypeWarning, EventReasonConfigMapUpdateFailed,
			fmt.Sprintf("Failed to update ConfigMap: %v", err))
		r.updateAllQueueStatus(ctx, validItems, duplicateItems, false, fmt.Sprintf("ConfigMap update failed: %v", err))
		return fmt.Errorf("failed to update ConfigMap: %w", err)
	}
	log.Info("Updated ConfigMap", "namespace", r.TargetNamespace, "name", ConfigMapName)
	configMapUpdatesTotal.WithLabelValues("success").Inc()
	r.emitConfigMapEvent(configMap, corev1.EventTypeNormal, EventReasonConfigMapUpdated,
		fmt.Sprintf("Updated ConfigMap with %d queue(s)", len(validItems)))
	r.emitPerQueueEvents(validItems)
	r.updateAllQueueStatus(ctx, validItems, duplicateItems, true, "")
	return nil
}

type duplicateInfo struct {
	queue  queuev1alpha1.Queue
	keptBy string
}

func (r *QueueReconciler) updateAllQueueStatus(ctx context.Context, validQueues []queuev1alpha1.Queue, duplicates []duplicateInfo, configMapSuccess bool, failureMessage string) {
	log := logf.FromContext(ctx)

	for i := range validQueues {
		q := &validQueues[i]
		if err := r.updateQueueStatus(ctx, q, configMapSuccess, failureMessage); err != nil {
			log.Error(err, "Failed to update status for queue", "queue", q.Name, "namespace", q.Namespace)
		}
	}

	for _, dup := range duplicates {
		q := &dup.queue
		msg := fmt.Sprintf("Duplicate queue name %q; already claimed by %s", q.Spec.Queue.Name, dup.keptBy)
		if err := r.updateQueueStatusDegraded(ctx, q, StatusReasonDuplicateQueueName, msg); err != nil {
			log.Error(err, "Failed to update status for duplicate queue", "queue", q.Name, "namespace", q.Namespace)
		}
	}
}

func (r *QueueReconciler) updateQueueStatus(ctx context.Context, queue *queuev1alpha1.Queue, available bool, failureMessage string) error {
	fresh := &queuev1alpha1.Queue{}
	if err := r.Get(ctx, types.NamespacedName{Name: queue.Name, Namespace: queue.Namespace}, fresh); err != nil {
		return err
	}

	now := metav1.Now()
	if available {
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               queuev1alpha1.ConditionTypeAvailable,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: fresh.Generation,
			LastTransitionTime: now,
			Reason:             "Reconciled",
			Message:            fmt.Sprintf("Queue %q is included in the YuniKorn ConfigMap", fresh.Spec.Queue.Name),
		})
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               queuev1alpha1.ConditionTypeDegraded,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: fresh.Generation,
			LastTransitionTime: now,
			Reason:             "Reconciled",
			Message:            "No issues detected",
		})
	} else {
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               queuev1alpha1.ConditionTypeAvailable,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: fresh.Generation,
			LastTransitionTime: now,
			Reason:             "ConfigMapFailed",
			Message:            failureMessage,
		})
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               queuev1alpha1.ConditionTypeDegraded,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: fresh.Generation,
			LastTransitionTime: now,
			Reason:             "ConfigMapFailed",
			Message:            failureMessage,
		})
	}

	fresh.Status.ObservedGeneration = fresh.Generation
	return r.Status().Update(ctx, fresh)
}

// Status condition reasons emitted by updateQueueStatusDegraded.
const (
	// StatusReasonDuplicateQueueName is set when a CR is excluded from the
	// ConfigMap because another CR already claims its spec.queue.name.
	StatusReasonDuplicateQueueName = "DuplicateQueueName"
	// StatusReasonInvalidConfig is set when a CR is the attributed cause of
	// a merged YuniKorn config validation failure (i.e. its single-CR
	// scheduler config also fails YuniKorn validation when rendered alone).
	StatusReasonInvalidConfig = "InvalidConfig"
)

// updateQueueStatusDegraded marks the queue Available=False and Degraded=True
// with the supplied (reason, message). Used both for duplicate-name skips and
// merged-config validation failures attributed to a specific CR.
func (r *QueueReconciler) updateQueueStatusDegraded(ctx context.Context, queue *queuev1alpha1.Queue, reason, message string) error {
	fresh := &queuev1alpha1.Queue{}
	if err := r.Get(ctx, types.NamespacedName{Name: queue.Name, Namespace: queue.Namespace}, fresh); err != nil {
		return err
	}

	now := metav1.Now()
	apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
		Type:               queuev1alpha1.ConditionTypeAvailable,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: fresh.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})
	apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
		Type:               queuev1alpha1.ConditionTypeDegraded,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: fresh.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	})

	fresh.Status.ObservedGeneration = fresh.Generation
	return r.Status().Update(ctx, fresh)
}

func (r *QueueReconciler) emitEvent(obj runtime.Object, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(obj, eventType, reason, message)
	}
}

func (r *QueueReconciler) emitConfigMapEvent(cm *corev1.ConfigMap, eventType, reason, message string) {
	if r.Recorder != nil && cm.ResourceVersion != "" {
		r.Recorder.Event(cm, eventType, reason, message)
	}
}

func (r *QueueReconciler) emitPerQueueEvents(queues []queuev1alpha1.Queue) {
	for i := range queues {
		r.emitEvent(&queues[i], corev1.EventTypeNormal, EventReasonQueueConfigured,
			fmt.Sprintf("Queue %q successfully included in ConfigMap", queues[i].Spec.Queue.Name))
	}
}

// invalidCRReason captures why a single CR's isolated single-CR scheduler
// config failed YuniKorn validation. Used by findInvalidCRs to attribute
// merged-config failures to specific CR(s) so we only mark the offender(s)
// as Degraded — not every unrelated CR in the cluster.
type invalidCRReason struct {
	stage   string // "render" | "parse" | "validate"
	message string // underlying YuniKorn error message
}

// findInvalidCRs partitions items into those whose isolated single-CR
// scheduler config (rendered under the same partition + placement options
// used for the merge) passes YuniKorn validation and those that don't.
//
// Why per-CR re-validation works in this operator's design:
//
// Queue CRs are L1-independent at the merge boundary — each CR's
// `spec.queue` is grafted as a direct child of root with NO cross-CR
// references. There is no root-level resource quota, placement rules come
// from env vars rather than CRs, and no CR's spec mentions another CR's
// queue path. So any merged-config invalidity is traceable to one or more
// CRs that are individually invalid when rendered alone.
//
// In the rare emergent case (every CR is individually valid but the union
// is not) findInvalidCRs returns an empty bad slice; the caller falls
// back to the legacy "degrade the whole valid set" behaviour rather than
// silently swallowing the failure.
func findInvalidCRs(items []queuev1alpha1.Queue, opts queueconfig.BuildOptions) (good, bad []queuev1alpha1.Queue, reasons map[string]invalidCRReason) {
	reasons = make(map[string]invalidCRReason, 0)
	for i := range items {
		cr := items[i]
		single := queueconfig.BuildMerged(
			queuev1alpha1.L1QueuesFromCRs([]queuev1alpha1.Queue{cr}),
			opts,
		)
		_, _, err := queueconfig.Validate(single)
		if err == nil {
			good = append(good, cr)
			continue
		}
		reason := invalidCRReason{stage: "validate", message: err.Error()}
		var ve *queueconfig.ValidationError
		if errors.As(err, &ve) {
			reason.stage = ve.Stage
			if ve.Err != nil {
				reason.message = ve.Err.Error()
			}
		}
		reasons[crKey(cr.Namespace, cr.Name)] = reason
		bad = append(bad, cr)
	}
	return good, bad, reasons
}

// crKey returns the canonical "namespace/name" key used by findInvalidCRs's
// reason map. K8s metadata.name and metadata.namespace cannot contain "/"
// so this is unambiguous.
func crKey(namespace, name string) string {
	return namespace + "/" + name
}

// SetupWithManager sets up the controller with the Manager.
//
// Partition + placement configuration is loaded via queueconfig.LoadOptionsFromEnv
// so the reconciler and the admission webhook share a single source of truth.
// A malformed PLACEMENT_RULES is soft-failed (logged + ignored) — mirroring
// the webhook's behaviour — so startup is never blocked by a typo.
func (r *QueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	targetNamespace := os.Getenv(TargetNamespaceEnvVar)
	if targetNamespace == "" {
		targetNamespace = DefaultTargetNamespace
	}
	r.TargetNamespace = targetNamespace

	opts, err := queueconfig.LoadOptionsFromEnv()
	if err != nil {
		logf.Log.Error(err, "Failed to parse placement rules from environment variable, ignoring", "envVar", PlacementRulesEnvVar)
	}
	r.PartitionName = opts.PartitionName
	r.PlacementRules = opts.PlacementRules

	logf.Log.Info("Queue controller configured", "targetNamespace", r.TargetNamespace, "partitionName", r.PartitionName, "placementRulesCount", len(r.PlacementRules))

	return ctrl.NewControllerManagedBy(mgr).
		For(&queuev1alpha1.Queue{}).
		Named("queue").
		Complete(r)
}
