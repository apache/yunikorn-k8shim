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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
)

func setupReconciler(objs ...runtime.Object) (*QueueReconciler, *record.FakeRecorder) {
	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	builder := fake.NewClientBuilder().WithScheme(scheme)
	if len(objs) > 0 {
		builder = builder.WithRuntimeObjects(objs...)
	}
	fakeClient := builder.Build()
	recorder := record.NewFakeRecorder(100)

	r := &QueueReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        recorder,
		TargetNamespace: "default",
		PartitionName:   "default",
	}
	return r, recorder
}

func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func assertHasEvent(t *testing.T, events []string, eventType, reason string) {
	t.Helper()
	prefix := eventType + " " + reason
	for _, e := range events {
		if strings.HasPrefix(e, prefix) {
			return
		}
	}
	t.Errorf("expected event with prefix %q, got events: %v", prefix, events)
}

func assertNoEvent(t *testing.T, events []string, reason string) {
	t.Helper()
	for _, e := range events {
		if strings.Contains(e, reason) {
			t.Errorf("unexpected event with reason %q found: %s", reason, e)
		}
	}
}

func TestEvents_ConfigMapCreated(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	r, recorder := setupReconciler(queue)
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := drainEvents(recorder)
	assertHasEvent(t, events, corev1.EventTypeNormal, EventReasonQueueConfigured)

	hasConfigured := false
	for _, e := range events {
		if strings.Contains(e, EventReasonQueueConfigured) && strings.Contains(e, "team-a") {
			hasConfigured = true
		}
	}
	if !hasConfigured {
		t.Errorf("expected QueueConfigured event mentioning 'team-a', got: %v", events)
	}
}

func TestEvents_ConfigMapUpdated(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName, Namespace: "default"},
		Data:       map[string]string{ConfigMapQueueKey: "old-content"},
	}

	r, recorder := setupReconciler(queue, existingCM)
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := drainEvents(recorder)
	assertHasEvent(t, events, corev1.EventTypeNormal, EventReasonConfigMapUpdated)
	assertHasEvent(t, events, corev1.EventTypeNormal, EventReasonQueueConfigured)
}

func TestEvents_ConfigMapNoop(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	r, recorder := setupReconciler(queue)

	// First reconcile creates the ConfigMap
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error on first reconcile: %v", err)
	}
	_ = drainEvents(recorder)

	// Second reconcile should be a noop
	_, err = r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error on second reconcile: %v", err)
	}

	events := drainEvents(recorder)
	assertNoEvent(t, events, EventReasonConfigMapCreated)
	assertNoEvent(t, events, EventReasonConfigMapUpdated)
	assertNoEvent(t, events, EventReasonQueueConfigured)
}

func TestEvents_DuplicateQueueSkipped(t *testing.T) {
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

	r, recorder := setupReconciler(queue1, queue2)
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := drainEvents(recorder)
	assertHasEvent(t, events, corev1.EventTypeWarning, EventReasonDuplicateQueueSkipped)

	found := false
	for _, e := range events {
		if strings.Contains(e, EventReasonDuplicateQueueSkipped) && strings.Contains(e, "team-a") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected DuplicateQueueSkipped event mentioning 'team-a', got: %v", events)
	}
}

func TestEvents_MultipleQueuesConfigured(t *testing.T) {
	queues := make([]runtime.Object, 3)
	for i, name := range []string{"team-a", "team-b", "team-c"} {
		queues[i] = &queuev1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(fmt.Sprintf("uid-%d", i))},
			Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: name}},
		}
	}

	r, recorder := setupReconciler(queues...)
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := drainEvents(recorder)
	configuredCount := 0
	for _, e := range events {
		if strings.Contains(e, EventReasonQueueConfigured) {
			configuredCount++
		}
	}
	if configuredCount != 3 {
		t.Errorf("expected 3 QueueConfigured events, got %d; events: %v", configuredCount, events)
	}
}

func TestEvents_NoRecorder(t *testing.T) {
	queue := &queuev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default", UID: "uid-1"},
		Spec:       queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{Name: "team-a"}},
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(queuev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	r := &QueueReconciler{
		Client:          fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(queue).Build(),
		Scheme:          scheme,
		Recorder:        nil,
		TargetNamespace: "default",
		PartitionName:   "default",
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "team-a", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("reconciler should not panic with nil recorder: %v", err)
	}
}

func TestEvents_EmptyCluster(t *testing.T) {
	r, recorder := setupReconciler()
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := drainEvents(recorder)
	assertNoEvent(t, events, EventReasonQueueConfigured)
	assertNoEvent(t, events, EventReasonDuplicateQueueSkipped)
}
