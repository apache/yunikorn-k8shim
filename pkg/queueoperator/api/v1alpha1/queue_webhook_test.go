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
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

func uint64Ptr(u uint64) *uint64 { return &u }

func boolPtr(b bool) *bool { return &b }

func makeQueue(metaName, queueName string, config *QueueConfig) Queue {
	q := Queue{
		ObjectMeta: metav1.ObjectMeta{Name: metaName},
		Spec:       QueueSpec{Queue: QueueConfig{Name: queueName}},
	}
	if config != nil {
		config.Name = queueName
		q.Spec.Queue = *config
	}
	return q
}

func makeQueueWithMeta(name, ns string, uid types.UID, queueName string) Queue {
	return Queue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: uid},
		Spec:       QueueSpec{Queue: QueueConfig{Name: queueName}},
	}
}

func checkErr(t *testing.T, expectErr bool, err error) {
	t.Helper()
	if expectErr && err == nil {
		t.Error("expected error but got nil")
	}
	if !expectErr && err != nil {
		t.Errorf("expected no error but got: %v", err)
	}
}

func TestValidateQueue(t *testing.T) {
	tests := []struct {
		name      string
		queue     Queue
		expectErr bool
	}{
		{"valid queue - name matches metadata", makeQueue("team-a", "team-a", nil), false},
		{"name mismatch", makeQueue("team-a", "team-b", nil), true},
		{"valid queue with children", makeQueue("team-a", "team-a", &QueueConfig{
			Parent: boolPtr(true),
			Queues: []QueueConfig{{Name: "dev"}, {Name: "prod"}},
		}), false},
		{"non-parent with children", makeQueue("team-a", "team-a", &QueueConfig{
			Parent: boolPtr(false),
			Queues: []QueueConfig{{Name: "dev"}},
		}), true},
		{"child name same as parent", makeQueue("team-a", "team-a", &QueueConfig{
			Parent: boolPtr(true),
			Queues: []QueueConfig{{Name: "team-a"}},
		}), true},
		{"duplicate child names", makeQueue("team-a", "team-a", &QueueConfig{
			Parent: boolPtr(true),
			Queues: []QueueConfig{{Name: "dev"}, {Name: "dev"}},
		}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.queue.validateQueue()
			checkErr(t, tt.expectErr, err)
		})
	}
}

func TestValidateQueueConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      QueueConfig
		expectCount int
	}{
		{
			name:        "valid leaf queue",
			config:      QueueConfig{Name: "leaf"},
			expectCount: 0,
		},
		{
			name: "parent=nil with children is allowed",
			config: QueueConfig{
				Name:   "parent-q",
				Queues: []QueueConfig{{Name: "child1"}},
			},
			expectCount: 0,
		},
		{
			name: "parent=true with children is valid",
			config: QueueConfig{
				Name:   "parent-q",
				Parent: boolPtr(true),
				Queues: []QueueConfig{{Name: "child1"}, {Name: "child2"}},
			},
			expectCount: 0,
		},
		{
			name: "parent=false with children is invalid",
			config: QueueConfig{
				Name:   "parent-q",
				Parent: boolPtr(false),
				Queues: []QueueConfig{{Name: "child1"}},
			},
			expectCount: 1,
		},
		{
			name: "child name equals parent name",
			config: QueueConfig{
				Name:   "team-a",
				Queues: []QueueConfig{{Name: "team-a"}},
			},
			expectCount: 1,
		},
		{
			name: "duplicate child names at same level",
			config: QueueConfig{
				Name:   "team-a",
				Queues: []QueueConfig{{Name: "dev"}, {Name: "dev"}},
			},
			expectCount: 1,
		},
		{
			name: "deeply nested valid hierarchy",
			config: QueueConfig{
				Name: "root-q",
				Queues: []QueueConfig{
					{
						Name: "l1",
						Queues: []QueueConfig{
							{
								Name:   "l2",
								Queues: []QueueConfig{{Name: "l3"}},
							},
						},
					},
				},
			},
			expectCount: 0,
		},
		{
			name: "deeply nested duplicate at level 2",
			config: QueueConfig{
				Name: "root-q",
				Queues: []QueueConfig{
					{
						Name:   "l1",
						Queues: []QueueConfig{{Name: "dup"}, {Name: "dup"}},
					},
				},
			},
			expectCount: 1,
		},
		{
			name: "multiple errors: parent=false with children and duplicate names",
			config: QueueConfig{
				Name:   "team-a",
				Parent: boolPtr(false),
				Queues: []QueueConfig{{Name: "dev"}, {Name: "dev"}},
			},
			expectCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validateQueueConfig(tt.config, field.NewPath("spec", "queue"))
			if len(errs) != tt.expectCount {
				t.Errorf("expected %d errors, got %d: %v", tt.expectCount, len(errs), errs)
			}
		})
	}
}

func TestValidateUniqueQueueName(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	tests := []struct {
		name      string
		existing  []Queue
		newQueue  Queue
		expectErr bool
	}{
		{"no conflict - empty cluster", nil,
			makeQueueWithMeta("team-a", "ns1", "uid-new", "team-a"), false},
		{"no conflict - different queue names",
			[]Queue{makeQueueWithMeta("team-a", "ns1", "uid-1", "team-a")},
			makeQueueWithMeta("team-b", "ns2", "uid-2", "team-b"), false},
		{"conflict - same queue name in different CRs",
			[]Queue{makeQueueWithMeta("team-a", "ns1", "uid-1", "team-a")},
			makeQueueWithMeta("team-a-dup", "ns2", "uid-2", "team-a"), true},
		{"skip self on update - same UID",
			[]Queue{makeQueueWithMeta("team-a", "ns1", "uid-1", "team-a")},
			makeQueueWithMeta("team-a", "ns1", "uid-1", "team-a"), false},
		{"skip self on update - same namespace and name",
			[]Queue{makeQueueWithMeta("team-a", "ns1", "", "team-a")},
			makeQueueWithMeta("team-a", "ns1", "", "team-a"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			objs := make([]runtime.Object, len(tt.existing))
			for i := range tt.existing {
				objs[i] = &tt.existing[i]
			}
			if len(objs) > 0 {
				builder = builder.WithRuntimeObjects(objs...)
			}
			fakeClient := builder.Build()

			v := &QueueValidator{Client: fakeClient}
			err := v.validateUniqueQueueName(context.Background(), &tt.newQueue)
			checkErr(t, tt.expectErr, err)
		})
	}
}

// TestValidateMergedConfig covers the third (and most expensive) admission
// layer: building the post-admission merged scheduler config and rejecting
// the CR if YuniKorn's own validator would reject it.
//
// The cases are chosen so that for each one, the candidate CR alone passes
// the cheap structural checks but the merged config breaks a specific
// YuniKorn rule. This is exactly the class of bug that previously slipped
// through the YK admission controller (when its endpoint was unreachable
// and failurePolicy was Ignore) and crashlooped the scheduler.
type mergedConfigCase struct {
	name      string
	existing  []Queue
	candidate Queue
	opts      queueconfig.BuildOptions
	expectErr bool
	errSubstr string // when expectErr, error message should contain this
}

// mergedConfigTestCases returns the fixture set for TestValidateMergedConfig.
// Extracted so the test itself stays under the funlen threshold.
func mergedConfigTestCases() []mergedConfigCase {
	return []mergedConfigCase{
		{
			name: "valid leaf — merged config passes",
			candidate: Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns1"},
				Spec:       QueueSpec{Queue: QueueConfig{Name: "team-a"}},
			},
			opts: queueconfig.BuildOptions{PartitionName: "default"},
		},
		{
			name: "valid alongside existing — merged config passes",
			existing: []Queue{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns1", UID: "uid-a"},
					Spec:       QueueSpec{Queue: QueueConfig{Name: "team-a"}},
				},
			},
			candidate: Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-b", Namespace: "ns2"},
				Spec:       QueueSpec{Queue: QueueConfig{Name: "team-b"}},
			},
			opts: queueconfig.BuildOptions{PartitionName: "default"},
		},
		{
			// YuniKorn rule: when a parent has max-applications set, every
			// child MUST also have max-applications
			// (checkQueueMaxApplications in yunikorn-core configs).
			name: "parent has max-apps but child does not",
			candidate: Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "parent-q", Namespace: "ns1"},
				Spec: QueueSpec{Queue: QueueConfig{
					Name:            "parent-q",
					Parent:          boolPtr(true),
					MaxApplications: uint64Ptr(100),
					Queues:          []QueueConfig{{Name: "child"}},
				}},
			},
			opts:      queueconfig.BuildOptions{PartitionName: "default"},
			expectErr: true,
			errSubstr: "merged YuniKorn scheduler config is invalid",
		},
		{
			// YuniKorn rule: queue name must match QueueNameRegEx.
			name: "queue name with illegal characters",
			candidate: Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-name", Namespace: "ns1"},
				Spec:       QueueSpec{Queue: QueueConfig{Name: "bad name with spaces"}},
			},
			opts:      queueconfig.BuildOptions{PartitionName: "default"},
			expectErr: true,
			errSubstr: "merged YuniKorn scheduler config is invalid",
		},
		{
			// Update path: candidate replaces the existing CR by UID.
			name: "update path replaces existing CR by UID",
			existing: []Queue{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns1", UID: "uid-a"},
					Spec:       QueueSpec{Queue: QueueConfig{Name: "team-a"}},
				},
			},
			candidate: Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns1", UID: "uid-a"},
				Spec: QueueSpec{Queue: QueueConfig{
					Name:            "team-a",
					Parent:          boolPtr(true),
					MaxApplications: uint64Ptr(100),
					Queues:          []QueueConfig{{Name: "child"}},
				}},
			},
			opts:      queueconfig.BuildOptions{PartitionName: "default"},
			expectErr: true,
			errSubstr: "merged YuniKorn scheduler config is invalid",
		},
		{
			// Same as above but matched by namespace/name (UID empty).
			name: "candidate replaces existing CR by namespace/name when UID empty",
			existing: []Queue{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns1"},
					Spec:       QueueSpec{Queue: QueueConfig{Name: "team-a"}},
				},
			},
			candidate: Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "ns1"},
				Spec: QueueSpec{Queue: QueueConfig{
					Name:            "team-a",
					Parent:          boolPtr(true),
					MaxApplications: uint64Ptr(100),
					Queues:          []QueueConfig{{Name: "child"}},
				}},
			},
			opts:      queueconfig.BuildOptions{PartitionName: "default"},
			expectErr: true,
			errSubstr: "merged YuniKorn scheduler config is invalid",
		},
	}
}

func TestValidateMergedConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	for _, tt := range mergedConfigTestCases() {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			objs := make([]runtime.Object, 0, len(tt.existing))
			for i := range tt.existing {
				objs = append(objs, &tt.existing[i])
			}
			if len(objs) > 0 {
				builder = builder.WithRuntimeObjects(objs...)
			}
			fakeClient := builder.Build()

			v := &QueueValidator{Client: fakeClient, BuildOptions: tt.opts}
			err := v.validateMergedConfig(context.Background(), &tt.candidate)
			checkErr(t, tt.expectErr, err)

			if tt.expectErr && err != nil {
				if !apierrors.IsInvalid(err) {
					t.Errorf("expected apierrors.IsInvalid but got %T: %v", err, err)
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("expected error to contain %q, got: %v", tt.errSubstr, err)
				}
			}
		})
	}
}

// TestValidate_LayerOrdering pins down the cheap-to-expensive ordering. A
// CR that fails layer 1 must NOT trigger a List call (we'd rather reject
// fast and not hammer the API server). We assert this indirectly by
// passing a nil client.Reader: if any later layer ran, we'd panic.
func TestValidate_LayerOrderingShortCircuits(t *testing.T) {
	v := &QueueValidator{Client: nil}
	// Layer 1 failure: queue.Name doesn't match metadata.name.
	bad := &Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a"},
		Spec:       QueueSpec{Queue: QueueConfig{Name: "team-b"}},
	}
	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected layer-1 error, got nil")
	}
	// If we got here without panicking on the nil client, layer 2/3 were
	// correctly skipped.
}

// Note: TestValidateCreate_RejectsWrongType has been removed because
// controller-runtime v0.24+ uses generics (Validator[*Queue]), so the
// runtime.Object type-assertion path no longer exists at the source.

// TestValidateDelete_AlwaysAllows pins the documented behaviour that
// deletes are unconditionally permitted.
func TestValidateDelete_AlwaysAllows(t *testing.T) {
	v := &QueueValidator{}
	w, err := v.ValidateDelete(context.Background(), &Queue{})
	if err != nil || w != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", w, err)
	}
}
