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

package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
)

// fakeEventSink counts the calls which reach it, it is the sink which would write to the
// API server
type fakeEventSink struct {
	creates int
	updates int
	patches int
	err     error
}

func (f *fakeEventSink) Create(_ context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	f.creates++
	return event, f.err
}

func (f *fakeEventSink) Update(_ context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	f.updates++
	return event, f.err
}

func (f *fakeEventSink) Patch(_ context.Context, event *eventsv1.Event, _ []byte) (*eventsv1.Event, error) {
	f.patches++
	return event, f.err
}

func newEvent(name string) *eventsv1.Event {
	return &eventsv1.Event{
		ObjectMeta: apis.ObjectMeta{Namespace: "default", Name: name},
	}
}

// step advances the fake clock, a passive clock can only have its time set
func step(clk *clocktesting.FakePassiveClock, d time.Duration) {
	clk.SetTime(clk.Now().Add(d))
}

// an exhausted bucket must shed the event: the wrapped sink is not called and the event is
// reported as written so that the broadcaster does not retry it
func TestEventSinkShedding(t *testing.T) {
	inner := &fakeEventSink{}
	// a burst of 1 leaves the bucket empty after the first event
	sink := NewRateLimitedEventSink(inner, 1, 1)

	event := newEvent("first")
	result, err := sink.Create(context.Background(), event)
	assert.NilError(t, err, "create failed")
	assert.Equal(t, event, result, "event not returned")
	assert.Equal(t, 1, inner.creates, "event not forwarded")

	for i := 0; i < 10; i++ {
		shedEvent := newEvent("shed")
		result, err = sink.Create(context.Background(), shedEvent)
		assert.NilError(t, err, "shed event must not fail the write")
		assert.Equal(t, shedEvent, result, "shed event not returned")
	}
	assert.Equal(t, 1, inner.creates, "shed events were forwarded")

	limited, ok := sink.(*rateLimitedEventSink)
	assert.Assert(t, ok, "unexpected sink type")
	// the first drop is logged which resets the counter, the remaining 9 are still counted
	assert.Equal(t, int64(9), limited.shed.Load(), "shed events not counted")
}

// all three sink operations must be shed, not just the creates
func TestEventSinkShedsAllOperations(t *testing.T) {
	inner := &fakeEventSink{}
	// a qps of 1 with a burst of 1: the single token is taken by the create
	sink := NewRateLimitedEventSink(inner, 1, 1)

	_, err := sink.Create(context.Background(), newEvent("first"))
	assert.NilError(t, err, "create failed")

	_, err = sink.Update(context.Background(), newEvent("update"))
	assert.NilError(t, err, "update failed")
	_, err = sink.Patch(context.Background(), newEvent("patch"), []byte("{}"))
	assert.NilError(t, err, "patch failed")

	assert.Equal(t, 1, inner.creates, "unexpected creates")
	assert.Equal(t, 0, inner.updates, "update not shed")
	assert.Equal(t, 0, inner.patches, "patch not shed")
}

// a qps <= 0 disables shedding, the sink must pass everything through
func TestEventSinkPassThrough(t *testing.T) {
	testCases := []struct {
		name  string
		qps   int
		burst int
	}{
		{"zero qps", 0, 0},
		{"negative qps", -1, 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeEventSink{}
			sink := NewRateLimitedEventSink(inner, tc.qps, tc.burst)
			limited, ok := sink.(*rateLimitedEventSink)
			assert.Assert(t, ok, "unexpected sink type")
			assert.Assert(t, limited.limiter == nil, "limiter must not be set")

			for i := 0; i < 100; i++ {
				_, err := sink.Create(context.Background(), newEvent("event"))
				assert.NilError(t, err, "create failed")
			}
			assert.Equal(t, 100, inner.creates, "events were shed")
			assert.Equal(t, int64(0), limited.shed.Load(), "events were counted as shed")
		})
	}
}

// an error from the wrapped sink must be passed back so that the broadcaster can retry
func TestEventSinkForwardsError(t *testing.T) {
	expected := errors.New("sink failed")
	inner := &fakeEventSink{err: expected}
	sink := NewRateLimitedEventSink(inner, 0, 0)

	_, err := sink.Create(context.Background(), newEvent("event"))
	assert.Equal(t, expected, err, "error not returned")
}

// a 429 mutes the events for the time the server asked for: nothing is sent until it passes
func TestEventSinkMuteOnThrottle(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	inner := &fakeEventSink{err: apierrors.NewTooManyRequests("slow down", 5)}
	// no client side limit, only the mute can shed here
	sink := newRateLimitedEventSink(inner, 0, 0, clk, nil)

	_, err := sink.Create(context.Background(), newEvent("throttled"))
	assert.Assert(t, err != nil, "the 429 must be returned to the broadcaster")
	assert.Equal(t, 1, inner.creates, "event not forwarded")

	// the sink is muted for the next 5 seconds, the wrapped sink must not be called
	inner.err = nil
	for i := 0; i < 4; i++ {
		step(clk, time.Second)
		event := newEvent("muted")
		result, muteErr := sink.Create(context.Background(), event)
		assert.NilError(t, muteErr, "muted event must not fail the write")
		assert.Equal(t, event, result, "muted event not returned")
	}
	assert.Equal(t, 1, inner.creates, "events sent while muted")
	// the first drop is logged which resets the counter, the remaining 3 are still counted
	assert.Equal(t, int64(3), sink.shed.Load(), "shed events not counted")

	// once the deadline is reached the events are sent again
	step(clk, time.Second)
	_, err = sink.Create(context.Background(), newEvent("after mute"))
	assert.NilError(t, err, "create failed")
	assert.Equal(t, 2, inner.creates, "event not forwarded after the mute expired")
}

// only a 429 mutes the events, any other failure is left to the broadcaster
func TestEventSinkMuteIgnoresOtherErrors(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())

	testCases := []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("sink failed")},
		{"not found", apierrors.NewNotFound(eventsv1.Resource("events"), "event")},
		{"server timeout", apierrors.NewInternalError(errors.New("boom"))},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeEventSink{err: tc.err}
			sink := newRateLimitedEventSink(inner, 0, 0, clk, nil)

			_, err := sink.Create(context.Background(), newEvent("failed"))
			assert.Assert(t, err != nil, "error not returned")
			assert.Equal(t, int64(0), sink.muteUntil.Load(), "events muted by a non 429 error")

			_, err = sink.Create(context.Background(), newEvent("next"))
			assert.Assert(t, err != nil, "error not returned")
			assert.Equal(t, 2, inner.creates, "event was shed")
		})
	}
}

// a mute is extended by a longer delay but never shortened by a shorter one
func TestEventSinkMuteOnlyExtends(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	inner := &fakeEventSink{err: apierrors.NewTooManyRequests("slow down", 30)}
	sink := newRateLimitedEventSink(inner, 0, 0, clk, nil)

	_, err := sink.Create(context.Background(), newEvent("throttled"))
	assert.Assert(t, err != nil, "error not returned")
	muted := sink.muteUntil.Load()
	assert.Equal(t, clk.Now().Add(30*time.Second).UnixNano(), muted, "unexpected mute deadline")

	// a shorter delay must not bring the deadline forward: step past the mute so that the
	// event is forwarded again, the new deadline is still earlier than the current one
	step(clk, time.Second)
	inner.err = apierrors.NewTooManyRequests("slow down", 1)
	sink.checkThrottled(inner.err)
	assert.Equal(t, muted, sink.muteUntil.Load(), "mute deadline was shortened")

	// a longer delay extends it
	sink.checkThrottled(apierrors.NewTooManyRequests("slow down", 60))
	assert.Equal(t, clk.Now().Add(60*time.Second).UnixNano(), sink.muteUntil.Load(), "mute not extended")
}

// a 429 without a delay must still mute, and an excessive delay must be capped
func TestEventSinkMuteBounds(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected time.Duration
	}{
		{"no delay advertised", apierrors.NewTooManyRequestsError("slow down"), defaultEventMute},
		{"delay capped", apierrors.NewTooManyRequests("slow down", 600), maxEventMute},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clk := clocktesting.NewFakePassiveClock(time.Now())
			sink := newRateLimitedEventSink(&fakeEventSink{}, 0, 0, clk, nil)

			sink.checkThrottled(tc.err)
			assert.Equal(t, clk.Now().Add(tc.expected).UnixNano(), sink.muteUntil.Load(), "unexpected mute deadline")
		})
	}
}
