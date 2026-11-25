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
	"reflect"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

func TestInit(t *testing.T) {
	// simply test the get won't fail
	recorder := GetRecorder()
	assert.Equal(t, reflect.TypeOf(recorder).String(), "*events.MockedRecorder")
}

// recordingRecorder collects the type and the reason of every event which reaches it
type recordingRecorder struct {
	recorded []string
	reasons  []string
}

func (r *recordingRecorder) Eventf(_ runtime.Object, _ runtime.Object, eventtype, reason, _, _ string, _ ...interface{}) {
	r.recorded = append(r.recorded, eventtype)
	r.reasons = append(r.reasons, reason)
}

func withEventConf(t *testing.T, mutate func(*conf.SchedulerConf)) {
	t.Helper()
	original := conf.GetSchedulerConf()
	t.Cleanup(func() { conf.SetSchedulerConf(original) })
	c := conf.CreateDefaultConfig()
	mutate(c)
	conf.SetSchedulerConf(c)
}

// the level filter drops the events below the configured level before they are recorded
func TestFilteredRecorderLevel(t *testing.T) {
	testCases := []struct {
		level    string
		expected []string
	}{
		{conf.EventLevelNormal, []string{v1.EventTypeNormal, v1.EventTypeWarning}},
		{conf.EventLevelWarning, []string{v1.EventTypeWarning}},
	}

	for _, tc := range testCases {
		t.Run(tc.level, func(t *testing.T) {
			withEventConf(t, func(c *conf.SchedulerConf) { c.KubeEventLevel = tc.level })

			inner := &recordingRecorder{}
			recorder := NewFilteredRecorder(inner)
			recorder.Eventf(nil, nil, v1.EventTypeNormal, "reason", "action", "note")
			recorder.Eventf(nil, nil, v1.EventTypeWarning, "reason", "action", "note")

			assert.DeepEqual(t, tc.expected, inner.recorded)
		})
	}
}

type testEvent struct{ eventtype, reason, action string }

var (
	// the events which are never shed and never take a token
	alwaysSent = []testEvent{
		{v1.EventTypeWarning, "PodUnschedulable", "PodUnschedulable"},
		{v1.EventTypeWarning, ReasonScheduling, ReasonScheduling},
		{v1.EventTypeNormal, "Scheduled", "Scheduled"},
		{v1.EventTypeNormal, "Informational", "Informational"},
		{v1.EventTypeNormal, "NodeAccepted", "NodeAccepted"},
		{v1.EventTypeNormal, ReasonGangScheduling, "TaskGroupsCreated"},
	}
	// the low-impact progress events which the budget applies to
	budgeted = []testEvent{
		{v1.EventTypeNormal, ReasonScheduling, ReasonScheduling},
		{v1.EventTypeNormal, ReasonPodBindSuccessful, ReasonPodBindSuccessful},
		{v1.EventTypeNormal, ReasonTaskCompleted, ReasonTaskCompleted},
		{v1.EventTypeNormal, ReasonGangScheduling, ActionTaskGroupMatch},
	}
)

func send(r events.EventRecorder, e testEvent) {
	r.Eventf(nil, nil, e.eventtype, e.reason, e.action, "note")
}

// sendBudgeted sends n budgeted events, cycling through every budgeted kind
func sendBudgeted(r events.EventRecorder, n int) {
	for i := 0; i < n; i++ {
		send(r, budgeted[i%len(budgeted)])
	}
}

func TestFilteredRecorderBudget(t *testing.T) {
	const qps, burst = 10, 20
	withEventConf(t, func(c *conf.SchedulerConf) { c.KubeEventQPS = qps })
	clk := clocktesting.NewFakeClock(time.Now())
	inner := &recordingRecorder{}
	recorder := newFilteredRecorder(inner, qps, clk)

	// more than the burst: none of these may spend a token
	for i := 0; i < 10; i++ {
		for _, e := range alwaysSent {
			send(recorder, e)
		}
	}
	assert.Equal(t, 10*len(alwaysSent), len(inner.recorded), "always sent events were shed")

	before := len(inner.recorded)
	sendBudgeted(recorder, burst)
	assert.Equal(t, burst, len(inner.recorded)-before, "the burst must pass")
	assert.Equal(t, int64(0), recorder.shed.Load())

	sendBudgeted(recorder, 1)
	assert.Equal(t, burst, len(inner.recorded)-before, "the event above the burst must be shed")
	assert.Equal(t, int64(1), recorder.shed.Load())

	// the always sent events still pass with the budget spent
	send(recorder, alwaysSent[0])
	send(recorder, alwaysSent[2])
	assert.Equal(t, burst+2, len(inner.recorded)-before)
}

// the budget refills with the injected clock, not with the wall clock
func TestFilteredRecorderBudgetClock(t *testing.T) {
	const qps, burst = 10, 20
	withEventConf(t, func(c *conf.SchedulerConf) { c.KubeEventQPS = qps })
	clk := clocktesting.NewFakeClock(time.Now())
	inner := &recordingRecorder{}
	recorder := newFilteredRecorder(inner, qps, clk)

	sendBudgeted(recorder, burst)
	assert.Equal(t, burst, len(inner.recorded))

	// a stall in real time refills nothing
	time.Sleep(250 * time.Millisecond)
	sendBudgeted(recorder, 1)
	assert.Equal(t, burst, len(inner.recorded), "wall clock time must not refill the budget")

	// 300ms of fake time at 10 events per second refills 3 tokens
	clk.Step(300 * time.Millisecond)
	sendBudgeted(recorder, 5)
	assert.Equal(t, burst+3, len(inner.recorded), "the fake clock must refill the budget")
}

func TestFilteredRecorderBudgetOff(t *testing.T) {
	for _, qps := range []int{0, -1} {
		withEventConf(t, func(c *conf.SchedulerConf) { c.KubeEventQPS = qps })
		inner := &recordingRecorder{}
		recorder := newFilteredRecorder(inner, qps, clocktesting.NewFakeClock(time.Now()))
		sendBudgeted(recorder, 1000)
		assert.Equal(t, 1000, len(inner.recorded), "nothing may be shed with qps %d", qps)
		assert.Equal(t, int64(0), recorder.shed.Load())
	}
}

// the shed count is reported by the first budgeted event after the interval, shed or not
func TestFilteredRecorderShedReport(t *testing.T) {
	const qps, burst = 10, 20
	withEventConf(t, func(c *conf.SchedulerConf) { c.KubeEventQPS = qps })
	clk := clocktesting.NewFakeClock(time.Now())
	recorder := newFilteredRecorder(&recordingRecorder{}, qps, clk)

	sendBudgeted(recorder, burst+5)
	assert.Equal(t, int64(5), recorder.shed.Load())

	// within the interval the count is kept, the refilled budget passes the event
	clk.Step(shedLogInterval / 2)
	sendBudgeted(recorder, 1)
	assert.Equal(t, int64(5), recorder.shed.Load())

	// an always sent event does not report
	clk.Step(shedLogInterval)
	send(recorder, alwaysSent[0])
	assert.Equal(t, int64(5), recorder.shed.Load())

	// the budget has refilled by now: the next budgeted event passes and reports the count
	sendBudgeted(recorder, 1)
	assert.Equal(t, int64(0), recorder.shed.Load(), "the shed count was not reported")
}

// the level filter applies before the budget: a suppressed event takes no token
func TestFilteredRecorderLevelBeforeBudget(t *testing.T) {
	const qps, burst = 10, 20
	withEventConf(t, func(c *conf.SchedulerConf) {
		c.KubeEventQPS = qps
		c.KubeEventLevel = conf.EventLevelWarning
	})
	inner := &recordingRecorder{}
	recorder := newFilteredRecorder(inner, qps, clocktesting.NewFakeClock(time.Now()))
	sendBudgeted(recorder, 5*burst)
	assert.Equal(t, 0, len(inner.recorded))
	assert.Equal(t, int64(0), recorder.shed.Load())
}
