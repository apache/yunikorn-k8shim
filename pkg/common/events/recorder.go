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

package events

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

var eventRecorder atomic.Pointer[events.EventRecorder]

func init() {
	r := events.EventRecorder(NewMockedRecorder())
	eventRecorder.Store(&r)
}

func GetRecorder() events.EventRecorder {
	return *eventRecorder.Load()
}

func SetRecorder(recorder events.EventRecorder) {
	eventRecorder.Store(&recorder)
}

const shedLogInterval = 30 * time.Second

// Shared with the call sites, so a renamed reason cannot silently leave the event budget.
const (
	ReasonScheduling        = "Scheduling"
	ReasonPodBindSuccessful = "PodBindSuccessful"
	ReasonTaskCompleted     = "TaskCompleted"
	ReasonGangScheduling    = "GangScheduling"
	ActionTaskGroupMatch    = "TaskGroupMatch"
)

// budgetedEvent returns true for the low-impact per-pod progress events which are shed when
// the event budget is spent. Every other event is always sent.
func budgetedEvent(eventtype, reason, action string) bool {
	if eventtype == v1.EventTypeWarning {
		return false
	}
	switch reason {
	case ReasonScheduling, ReasonPodBindSuccessful, ReasonTaskCompleted:
		return true
	case ReasonGangScheduling:
		return action == ActionTaskGroupMatch
	}
	return false
}

// filteredRecorder drops the events below the configured event level, and sheds the budgeted
// events once the event budget is spent.
type filteredRecorder struct {
	inner  events.EventRecorder
	budget flowcontrol.PassiveRateLimiter // nil: nothing is shed
	clock  clock.PassiveClock

	shed       atomic.Int64
	lastLogged atomic.Int64 // unix nanoseconds
}

// NewFilteredRecorder drops the events below kubernetes.eventLevel and sheds the budgeted
// events above kubernetes.eventQPS (burst twice that, <= 0 turns the budget off). A dropped
// or shed event is never cached or written.
func NewFilteredRecorder(inner events.EventRecorder) events.EventRecorder {
	return newFilteredRecorder(inner, conf.GetSchedulerConf().KubeEventQPS, clock.RealClock{})
}

func newFilteredRecorder(inner events.EventRecorder, qps int, clk clock.PassiveClock) *filteredRecorder {
	r := &filteredRecorder{inner: inner, clock: clk}
	r.lastLogged.Store(clk.Now().UnixNano())
	if qps > 0 {
		r.budget = flowcontrol.NewTokenBucketPassiveRateLimiterWithClock(float32(qps), 2*qps, clk)
	}
	return r
}

// Eventf reads the level on every call: the setting is hot-reloadable
func (r *filteredRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	if conf.GetSchedulerConf().KubeEventLevel == conf.EventLevelWarning && eventtype != v1.EventTypeWarning {
		return
	}
	if r.budget != nil && budgetedEvent(eventtype, reason, action) {
		// checked on every budgeted event so that the end of a storm is still reported
		r.logShed()
		if !r.budget.TryAccept() {
			r.shed.Add(1)
			return
		}
	}
	r.inner.Eventf(regarding, related, eventtype, reason, action, note, args...)
}

// logShed logs the number of events shed since the last log, at most once per interval
func (r *filteredRecorder) logShed() {
	if r.shed.Load() == 0 {
		return
	}
	now := r.clock.Now().UnixNano()
	previous := r.lastLogged.Load()
	if now-previous < int64(shedLogInterval) || !r.lastLogged.CompareAndSwap(previous, now) {
		return
	}
	if shed := r.shed.Swap(0); shed > 0 {
		log.Log(log.Shim).Warn("low-impact progress events shed by the event budget",
			zap.Int64("shedEvents", shed),
			zap.Int(conf.CMKubeEventQPS, conf.GetSchedulerConf().KubeEventQPS))
	}
}
