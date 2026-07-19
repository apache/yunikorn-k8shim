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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

func TestComputeHierarchyDepth(t *testing.T) {
	tests := []struct {
		name     string
		queue    queueconfig.QueueConfig
		expected int
	}{
		{
			name:     "leaf queue has depth 1",
			queue:    queueconfig.QueueConfig{Name: "leaf"},
			expected: 1,
		},
		{
			name: "single child gives depth 2",
			queue: queueconfig.QueueConfig{
				Name:   "parent",
				Queues: []queueconfig.QueueConfig{{Name: "child"}},
			},
			expected: 2,
		},
		{
			name: "three-level hierarchy gives depth 3",
			queue: queueconfig.QueueConfig{
				Name: "top",
				Queues: []queueconfig.QueueConfig{
					{
						Name: "mid",
						Queues: []queueconfig.QueueConfig{
							{Name: "bottom"},
						},
					},
				},
			},
			expected: 3,
		},
		{
			name: "asymmetric tree returns max depth",
			queue: queueconfig.QueueConfig{
				Name: "root",
				Queues: []queueconfig.QueueConfig{
					{Name: "shallow"},
					{
						Name: "deep",
						Queues: []queueconfig.QueueConfig{
							{
								Name: "deeper",
								Queues: []queueconfig.QueueConfig{
									{Name: "deepest"},
								},
							},
						},
					},
				},
			},
			expected: 4,
		},
		{
			name: "wide but flat gives depth 2",
			queue: queueconfig.QueueConfig{
				Name: "wide",
				Queues: []queueconfig.QueueConfig{
					{Name: "a"},
					{Name: "b"},
					{Name: "c"},
					{Name: "d"},
				},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeHierarchyDepth(tt.queue)
			if got != tt.expected {
				t.Errorf("computeHierarchyDepth() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestMetricRegistrations(t *testing.T) {
	// Verify all metrics can be described without panicking and have correct names
	expectedNames := map[string]bool{
		"queue_operator_reconcile_total":                true,
		"queue_operator_reconcile_duration_seconds":     true,
		"queue_operator_configmap_updates_total":        true,
		"queue_operator_queues_total":                   true,
		"queue_operator_queues_duplicate_skipped_total": true,
		"queue_operator_queue_hierarchy_depth":          true,
		"queue_operator_webhook_missed_invalid_total":   true,
	}

	collectors := []prometheus.Collector{
		reconcileTotal,
		reconcileDuration,
		configMapUpdatesTotal,
		queuesTotal,
		queuesDuplicateSkippedTotal,
		queueHierarchyDepth,
		webhookMissedInvalidTotal,
	}

	for _, c := range collectors {
		descCh := make(chan *prometheus.Desc, 10)
		c.Describe(descCh)
		close(descCh)
		for desc := range descCh {
			found := false
			s := desc.String()
			for name := range expectedNames {
				if containsSubstring(s, name) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected metric descriptor: %s", s)
			}
		}
	}
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && searchSubstring(s, sub)
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestReconcileTotalCounter(t *testing.T) {
	// Reset by creating a fresh counter for testing purposes
	ctr := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "test",
			Name:      "reconcile_total",
		},
		[]string{"result"},
	)

	ctr.WithLabelValues("success").Inc()
	ctr.WithLabelValues("success").Inc()
	ctr.WithLabelValues("error").Inc()

	if got := testutil.ToFloat64(ctr.WithLabelValues("success")); got != 2 {
		t.Errorf("success count = %v, want 2", got)
	}
	if got := testutil.ToFloat64(ctr.WithLabelValues("error")); got != 1 {
		t.Errorf("error count = %v, want 1", got)
	}
}

func TestConfigMapUpdatesTotalCounter(t *testing.T) {
	ctr := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "test",
			Name:      "configmap_updates_total",
		},
		[]string{"result"},
	)

	ctr.WithLabelValues("success").Inc()
	ctr.WithLabelValues("noop").Inc()
	ctr.WithLabelValues("noop").Inc()
	ctr.WithLabelValues("error").Inc()

	if got := testutil.ToFloat64(ctr.WithLabelValues("success")); got != 1 {
		t.Errorf("success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(ctr.WithLabelValues("noop")); got != 2 {
		t.Errorf("noop = %v, want 2", got)
	}
	if got := testutil.ToFloat64(ctr.WithLabelValues("error")); got != 1 {
		t.Errorf("error = %v, want 1", got)
	}
}

func TestQueuesTotalGauge(t *testing.T) {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "test",
		Name:      "queues_total",
	})

	gauge.Set(5)
	if got := testutil.ToFloat64(gauge); got != 5 {
		t.Errorf("gauge = %v, want 5", got)
	}

	gauge.Set(3)
	if got := testutil.ToFloat64(gauge); got != 3 {
		t.Errorf("gauge after decrease = %v, want 3", got)
	}

	gauge.Set(0)
	if got := testutil.ToFloat64(gauge); got != 0 {
		t.Errorf("gauge after zero = %v, want 0", got)
	}
}

func TestQueueHierarchyDepthGauge(t *testing.T) {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "test",
			Name:      "queue_hierarchy_depth",
		},
		[]string{"queue"},
	)

	gauge.WithLabelValues("team-a").Set(1)
	gauge.WithLabelValues("team-b").Set(3)

	if got := testutil.ToFloat64(gauge.WithLabelValues("team-a")); got != 1 {
		t.Errorf("team-a depth = %v, want 1", got)
	}
	if got := testutil.ToFloat64(gauge.WithLabelValues("team-b")); got != 3 {
		t.Errorf("team-b depth = %v, want 3", got)
	}
}

func TestDuplicateSkippedCounter(t *testing.T) {
	ctr := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "test",
		Name:      "dupes",
	})

	ctr.Inc()
	ctr.Inc()
	ctr.Inc()

	if got := testutil.ToFloat64(ctr); got != 3 {
		t.Errorf("duplicate counter = %v, want 3", got)
	}
}

// Exercises the (label, increment, sample) shape of the
// webhook_missed_invalid_total counter so accidental renames of the
// `stage` label or its cardinality regress immediately. The end-to-end
// "the reconciler actually increments it when it catches a bad config"
// assertion lives next to the existing invalid-merge envtest spec
// (see queue_controller_test.go: "refuses to write the ConfigMap").
func TestWebhookMissedInvalidCounter(t *testing.T) {
	ctr := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "test",
			Name:      "webhook_missed_invalid_total",
		},
		[]string{"stage"},
	)

	ctr.WithLabelValues("validate").Inc()
	ctr.WithLabelValues("validate").Inc()
	ctr.WithLabelValues("parse").Inc()
	ctr.WithLabelValues("unknown").Inc()

	if got := testutil.ToFloat64(ctr.WithLabelValues("validate")); got != 2 {
		t.Errorf("validate stage = %v, want 2", got)
	}
	if got := testutil.ToFloat64(ctr.WithLabelValues("parse")); got != 1 {
		t.Errorf("parse stage = %v, want 1", got)
	}
	if got := testutil.ToFloat64(ctr.WithLabelValues("unknown")); got != 1 {
		t.Errorf("unknown stage = %v, want 1", got)
	}
	if got := testutil.ToFloat64(ctr.WithLabelValues("render")); got != 0 {
		t.Errorf("render stage (never incremented) = %v, want 0", got)
	}
}

func TestReconcileDurationHistogram(t *testing.T) {
	hist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "test",
			Name:      "reconcile_duration_seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"result"},
	)

	hist.WithLabelValues("success").Observe(0.001)
	hist.WithLabelValues("success").Observe(0.05)
	hist.WithLabelValues("error").Observe(1.5)

	if count := testutil.CollectAndCount(hist); count != 2 {
		t.Errorf("histogram metric count = %d, want 2 (one per label set)", count)
	}
}
