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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

const metricsNamespace = "queue_operator"

var (
	reconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_total",
			Help:      "Total number of reconciliations performed by the queue controller.",
		},
		[]string{"result"},
	)

	reconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_duration_seconds",
			Help:      "Duration of reconciliation in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"result"},
	)

	configMapUpdatesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "configmap_updates_total",
			Help:      "Total number of ConfigMap write operations by outcome.",
		},
		[]string{"result"},
	)

	queuesTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "queues_total",
			Help:      "Current number of valid Queue CRs included in the ConfigMap.",
		},
	)

	queuesDuplicateSkippedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "queues_duplicate_skipped_total",
			Help:      "Total number of Queue CRs skipped due to duplicate queue names.",
		},
	)

	queueHierarchyDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "queue_hierarchy_depth",
			Help:      "Maximum depth of each L1 queue hierarchy.",
		},
		[]string{"queue"},
	)

	// webhookMissedInvalidTotal is the "trust-but-verify" counter for the
	// admission webhook. The reconciler runs queueconfig.Validate as a
	// last-line-of-defence before writing the YuniKorn ConfigMap — every
	// failure that fires here is a config the webhook *should* have
	// rejected at admission time.
	//
	// A non-zero (or rising) value means one of:
	//   - the webhook was disabled / failurePolicy=Ignore / unreachable,
	//   - a CR pre-dates the current webhook validation rules,
	//   - admission concurrency races leaked an invalid merged shape past
	//     the webhook's view of committed state,
	//   - validation logic has drifted between the webhook and the
	//     reconciler (they share queueconfig.Validate today; this guards
	//     against future divergence).
	//
	// Alert on rate > 0 over any 5m window. The `stage` label
	// (render/parse/validate) tells you which YuniKorn pipeline step the
	// reconciler caught, so an alert can include that in the page.
	webhookMissedInvalidTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "webhook_missed_invalid_total",
			Help: "Total number of times the reconciler caught an invalid merged " +
				"scheduler config that the admission webhook should have rejected. " +
				"A non-zero value indicates webhook bypass, drift, or pre-existing " +
				"bad state — alert on increase. Labels: stage = render|parse|validate.",
		},
		[]string{"stage"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		reconcileTotal,
		reconcileDuration,
		configMapUpdatesTotal,
		queuesTotal,
		queuesDuplicateSkippedTotal,
		queueHierarchyDepth,
		webhookMissedInvalidTotal,
	)
}

// computeHierarchyDepth returns the maximum depth of a queue hierarchy (1 for a leaf).
func computeHierarchyDepth(q queueconfig.QueueConfig) int {
	if len(q.Queues) == 0 {
		return 1
	}
	maxChild := 0
	for _, child := range q.Queues {
		d := computeHierarchyDepth(child)
		if d > maxChild {
			maxChild = d
		}
	}
	return 1 + maxChild
}
