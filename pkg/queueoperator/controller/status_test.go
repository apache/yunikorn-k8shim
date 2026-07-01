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
	"fmt"
	"os"
	"testing"

	"go.yaml.in/yaml/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

func getCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func TestStatus_AvailableAfterSuccessfulReconcile(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &queuev1alpha1.Queue{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "team-a", Namespace: "default"}, updated); err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}

	avail := getCondition(updated.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
	if avail == nil {
		t.Fatal("expected Available condition to be set")
	}
	if avail.Status != metav1.ConditionTrue {
		t.Errorf("Available status = %s, want True", avail.Status)
	}
	if avail.Reason != "Reconciled" {
		t.Errorf("Available reason = %s, want Reconciled", avail.Reason)
	}

	degraded := getCondition(updated.Status.Conditions, queuev1alpha1.ConditionTypeDegraded)
	if degraded == nil {
		t.Fatal("expected Degraded condition to be set")
	}
	if degraded.Status != metav1.ConditionFalse {
		t.Errorf("Degraded status = %s, want False", degraded.Status)
	}
}

func TestStatus_ObservedGenerationSet(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &queuev1alpha1.Queue{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "team-a", Namespace: "default"}, updated); err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}

	if updated.Status.ObservedGeneration != updated.Generation {
		t.Errorf("ObservedGeneration = %d, want %d (metadata.generation)", updated.Status.ObservedGeneration, updated.Generation)
	}
}

func TestStatus_DuplicateQueueDegraded(t *testing.T) {
	ts1 := metav1.Now()
	ts2 := metav1.NewTime(ts1.Add(1))

	queue1 := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "team-a-first", Namespace: "default",
			UID: "uid-1", CreationTimestamp: ts1,
		},
		Spec: queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}
	queue2 := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "team-a-second", Namespace: "default",
			UID: "uid-2", CreationTimestamp: ts2,
		},
		Spec: queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue1, queue2).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The first queue (oldest) should be Available
	first := &queuev1alpha1.Queue{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "team-a-first", Namespace: "default"}, first); err != nil {
		t.Fatalf("failed to get first queue: %v", err)
	}
	avail := getCondition(first.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
	if avail == nil || avail.Status != metav1.ConditionTrue {
		t.Errorf("first queue Available = %v, want True", avail)
	}

	// The duplicate (newer) should be Degraded
	second := &queuev1alpha1.Queue{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "team-a-second", Namespace: "default"}, second); err != nil {
		t.Fatalf("failed to get second queue: %v", err)
	}
	degraded := getCondition(second.Status.Conditions, queuev1alpha1.ConditionTypeDegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue {
		t.Errorf("duplicate queue Degraded = %v, want True", degraded)
	}
	if degraded != nil && degraded.Reason != "DuplicateQueueName" {
		t.Errorf("duplicate queue Degraded reason = %s, want DuplicateQueueName", degraded.Reason)
	}

	availSecond := getCondition(second.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
	if availSecond == nil || availSecond.Status != metav1.ConditionFalse {
		t.Errorf("duplicate queue Available = %v, want False", availSecond)
	}
}

func TestStatus_MultipleQueuesAllAvailable(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	names := []string{"team-a", "team-b", "team-c"}
	objs := make([]runtime.Object, len(names))
	for i, name := range names {
		objs[i] = &queuev1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(fmt.Sprintf("uid-%d", i))},
			Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: name}},
		}
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(objs...).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, name := range names {
		q := &queuev1alpha1.Queue{}
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, q); err != nil {
			t.Errorf("failed to get queue %s: %v", name, err)
			continue
		}
		avail := getCondition(q.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
		if avail == nil || avail.Status != metav1.ConditionTrue {
			t.Errorf("queue %s Available = %v, want True", name, avail)
		}
		degraded := getCondition(q.Status.Conditions, queuev1alpha1.ConditionTypeDegraded)
		if degraded == nil || degraded.Status != metav1.ConditionFalse {
			t.Errorf("queue %s Degraded = %v, want False", name, degraded)
		}
	}
}

func TestStatus_NoopReconcilePreservesStatus(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	// First reconcile
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("first reconcile error: %v", err)
	}

	// Second reconcile (noop ConfigMap)
	_, err = r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("second reconcile error: %v", err)
	}

	updated := &queuev1alpha1.Queue{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "team-a", Namespace: "default"}, updated); err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}

	avail := getCondition(updated.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
	if avail == nil || avail.Status != metav1.ConditionTrue {
		t.Errorf("after noop reconcile, Available = %v, want True", avail)
	}
}

func TestStatus_ConditionTypesExported(t *testing.T) {
	if queuev1alpha1.ConditionTypeAvailable != "Available" {
		t.Errorf("ConditionTypeAvailable = %q, want Available", queuev1alpha1.ConditionTypeAvailable)
	}
	if queuev1alpha1.ConditionTypeDegraded != "Degraded" {
		t.Errorf("ConditionTypeDegraded = %q, want Degraded", queuev1alpha1.ConditionTypeDegraded)
	}
}

func TestReconcile_ListError(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("simulated list error")
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err == nil {
		t.Fatal("expected error from List failure")
	}
}

func TestReconcile_ConfigMapGetError(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("simulated get error")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err == nil {
		t.Fatal("expected error from ConfigMap Get failure")
	}
}

func TestReconcile_ConfigMapCreateError(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("simulated create error")
				}
				return nil
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err == nil {
		t.Fatal("expected error from ConfigMap Create failure")
	}
}

func TestReconcile_ConfigMapUpdateError(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "default"},
		Data:       map[string]string{ConfigMapQueueKey: "old-content"},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue, existingCM).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("simulated update error")
				}
				return nil
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err == nil {
		t.Fatal("expected error from ConfigMap Update failure")
	}
}

func TestReconcile_ConfigMapNilData(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "default"},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue, existingCM).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cm := &corev1.ConfigMap{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: ConfigMapName, Namespace: "default"}, cm); err != nil {
		t.Fatalf("failed to get ConfigMap: %v", err)
	}
	if cm.Data == nil {
		t.Fatal("expected ConfigMap Data to be initialized")
	}
	if _, ok := cm.Data[ConfigMapQueueKey]; !ok {
		t.Fatal("expected ConfigMap to contain queues.yaml key")
	}
}

func TestStatus_UnavailableOnConfigMapFailure(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(queue).
		WithStatusSubresource(&queuev1alpha1.Queue{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("simulated create error")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(10),
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	// Intentionally discard the error — this test asserts on the *status
	// update path* that runs even when the API-write step below fails.
	//nolint:errcheck
	r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})

	updated := &queuev1alpha1.Queue{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "team-a", Namespace: "default"}, updated); err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}

	avail := getCondition(updated.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
	if avail == nil {
		t.Fatal("expected Available condition")
	}
	if avail.Status != metav1.ConditionFalse {
		t.Errorf("Available = %s, want False on ConfigMap failure", avail.Status)
	}
	if avail.Reason != "ConfigMapFailed" {
		t.Errorf("Available reason = %s, want ConfigMapFailed", avail.Reason)
	}

	degraded := getCondition(updated.Status.Conditions, queuev1alpha1.ConditionTypeDegraded)
	if degraded == nil {
		t.Fatal("expected Degraded condition")
	}
	if degraded.Status != metav1.ConditionTrue {
		t.Errorf("Degraded = %s, want True on ConfigMap failure", degraded.Status)
	}
}

func TestStatus_UpdateQueueStatusDegradedGetError(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return fmt.Errorf("simulated get error")
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default"},
	}
	err := r.updateQueueStatusDegraded(context.Background(), queue, StatusReasonDuplicateQueueName, "test message")
	if err == nil {
		t.Fatal("expected error from Get failure in updateQueueStatusDegraded")
	}
}

func TestStatus_UpdateQueueStatusGetError(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return fmt.Errorf("simulated get error")
			},
		}).Build()

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default"},
	}
	err := r.updateQueueStatus(context.Background(), queue, true, "")
	if err == nil {
		t.Fatal("expected error from Get failure in updateQueueStatus")
	}
}

func TestSetupWithManager_EnvVarDefaults(t *testing.T) {
	os.Unsetenv(TargetNamespaceEnvVar)
	os.Unsetenv(PartitionNameEnvVar)
	os.Unsetenv(PlacementRulesEnvVar)

	r := &QueueReconciler{}
	r.TargetNamespace = ""
	r.PartitionName = ""
	r.PlacementRules = nil

	targetNamespace := os.Getenv(TargetNamespaceEnvVar)
	if targetNamespace == "" {
		targetNamespace = DefaultTargetNamespace
	}
	r.TargetNamespace = targetNamespace

	partitionName := os.Getenv(PartitionNameEnvVar)
	if partitionName == "" {
		partitionName = DefaultPartitionName
	}
	r.PartitionName = partitionName

	if r.TargetNamespace != DefaultTargetNamespace {
		t.Errorf("TargetNamespace = %s, want %s", r.TargetNamespace, DefaultTargetNamespace)
	}
	if r.PartitionName != DefaultPartitionName {
		t.Errorf("PartitionName = %s, want %s", r.PartitionName, DefaultPartitionName)
	}
}

func TestSetupWithManager_EnvVarOverrides(t *testing.T) {
	t.Setenv(TargetNamespaceEnvVar, "custom-ns")
	t.Setenv(PartitionNameEnvVar, "custom-partition")
	t.Setenv(PlacementRulesEnvVar, `- name: provided
  create: false`)

	r := &QueueReconciler{}

	targetNamespace := os.Getenv(TargetNamespaceEnvVar)
	if targetNamespace == "" {
		targetNamespace = DefaultTargetNamespace
	}
	r.TargetNamespace = targetNamespace

	partitionName := os.Getenv(PartitionNameEnvVar)
	if partitionName == "" {
		partitionName = DefaultPartitionName
	}
	r.PartitionName = partitionName

	if r.TargetNamespace != "custom-ns" {
		t.Errorf("TargetNamespace = %s, want custom-ns", r.TargetNamespace)
	}
	if r.PartitionName != "custom-partition" {
		t.Errorf("PartitionName = %s, want custom-partition", r.PartitionName)
	}
}

func TestSetupWithManager_InvalidPlacementRules(t *testing.T) {
	t.Setenv(PlacementRulesEnvVar, "not: [valid: yaml: {{")

	r := &QueueReconciler{}

	placementRulesYAML := os.Getenv(PlacementRulesEnvVar)
	if placementRulesYAML != "" {
		var placementRules []queueconfig.PlacementRule
		if err := yaml.Unmarshal([]byte(placementRulesYAML), &placementRules); err != nil {
			r.PlacementRules = nil
		} else {
			r.PlacementRules = placementRules
		}
	}

	if r.PlacementRules != nil {
		t.Error("expected nil PlacementRules for invalid YAML")
	}
}
