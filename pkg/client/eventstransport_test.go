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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"
)

// the REST client only retries a rejection which tells it how long to wait for, so it must
// not see the header: the retries sleep in the calling goroutine and never reach the sink
const noRetryBudget = time.Second

// newThrottlingServer returns a server which rejects every request with the given status and
// headers, and counts the requests it served
func newThrottlingServer(t *testing.T, status int, headers map[string]string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		for name, value := range headers {
			w.Header().Set(name, value)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

// newTestEventsClient builds the events client used against the test server, with the
// throttle transport attached when hints are passed in
func newTestEventsClient(t *testing.T, server *httptest.Server, hints *muteHintHolder, clk *clocktesting.FakePassiveClock) kubernetes.Interface {
	t.Helper()
	config := &rest.Config{Host: server.URL}
	config.QPS = -1
	if hints != nil {
		config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			return &eventsThrottleTransport{base: rt, hints: hints, clock: clk}
		})
	}
	clientSet, err := kubernetes.NewForConfig(config)
	assert.NilError(t, err, "could not create clientset")
	return clientSet
}

func createEvent(clientSet kubernetes.Interface) error {
	event := &eventsv1.Event{ObjectMeta: apis.ObjectMeta{Namespace: "default", Name: "event"}}
	_, err := clientSet.EventsV1().Events("default").Create(context.Background(), event, apis.CreateOptions{})
	return err
}

// a 429 which advertises a delay must fail fast, the delay must be recorded and the header
// removed so that the REST client does not retry the request itself
func TestThrottleTransportRecordsAndStrips(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	hints := &muteHintHolder{}
	server, requests := newThrottlingServer(t, http.StatusTooManyRequests, map[string]string{retryAfterHeader: "7"})
	clientSet := newTestEventsClient(t, server, hints, clk)

	start := time.Now()
	err := createEvent(clientSet)
	elapsed := time.Since(start)

	assert.Assert(t, err != nil, "the rejection must be returned")
	assert.Assert(t, apierrors.IsTooManyRequests(err), "unexpected error: %v", err)
	assert.Assert(t, elapsed < noRetryBudget, "the request was retried internally, it took %s", elapsed)
	assert.Equal(t, int64(1), requests.Load(), "the request was sent more than once")

	seconds, ok := hints.get(clk.Now())
	assert.Assert(t, ok, "delay not recorded")
	assert.Equal(t, 7, seconds, "unexpected delay recorded")

	// the header is gone from the error, the sink has to fall back to the recorded delay
	_, hasDelay := apierrors.SuggestsClientDelay(err)
	assert.Assert(t, !hasDelay, "the Retry-After header was not removed")
}

// a 429 without a delay must fail fast as well, there is nothing to record
func TestThrottleTransportWithoutHeader(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	hints := &muteHintHolder{}
	server, requests := newThrottlingServer(t, http.StatusTooManyRequests, nil)
	clientSet := newTestEventsClient(t, server, hints, clk)

	start := time.Now()
	err := createEvent(clientSet)
	elapsed := time.Since(start)

	assert.Assert(t, apierrors.IsTooManyRequests(err), "unexpected error: %v", err)
	assert.Assert(t, elapsed < noRetryBudget, "the request was retried internally, it took %s", elapsed)
	assert.Equal(t, int64(1), requests.Load(), "the request was sent more than once")

	_, ok := hints.get(clk.Now())
	assert.Assert(t, !ok, "a delay was recorded")
}

// every response which is not a 429 must be passed through untouched
func TestThrottleTransportPassesThrough(t *testing.T) {
	testCases := []struct {
		name   string
		status int
	}{
		{"created", http.StatusOK},
		{"not found", http.StatusNotFound},
		{"server error", http.StatusInternalServerError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clk := clocktesting.NewFakePassiveClock(time.Now())
			hints := &muteHintHolder{}
			server, _ := newThrottlingServer(t, tc.status, map[string]string{retryAfterHeader: "7"})

			// call the transport directly, the typed client would retry a 500 for a while
			base := &eventsThrottleTransport{base: http.DefaultTransport, hints: hints, clock: clk}
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			assert.NilError(t, err, "could not create request")
			resp, err := base.RoundTrip(req)
			assert.NilError(t, err, "round trip failed")
			defer func() {
				_ = resp.Body.Close()
			}()

			assert.Equal(t, tc.status, resp.StatusCode, "unexpected status")
			assert.Equal(t, "7", resp.Header.Get(retryAfterHeader), "the header was removed")
			_, ok := hints.get(clk.Now())
			assert.Assert(t, !ok, "a delay was recorded")
		})
	}
}

// the recorded delay describes the server right now, it must not be used indefinitely
func TestMuteHintExpires(t *testing.T) {
	now := time.Now()
	hints := &muteHintHolder{}

	_, ok := hints.get(now)
	assert.Assert(t, !ok, "an empty holder returned a delay")

	hints.record(7, now)
	seconds, ok := hints.get(now.Add(muteHintValidity))
	assert.Assert(t, ok, "delay expired too early")
	assert.Equal(t, 7, seconds, "unexpected delay")

	_, ok = hints.get(now.Add(muteHintValidity + time.Second))
	assert.Assert(t, !ok, "stale delay returned")
}

// the sink mutes for the delay recorded by the transport when the error no longer carries it
func TestEventSinkMuteFromTransport(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	hints := &muteHintHolder{}
	hints.record(7, clk.Now())
	// the error carries no delay, exactly what the transport leaves behind
	inner := &fakeEventSink{err: apierrors.NewTooManyRequestsError("slow down")}
	sink := newRateLimitedEventSink(inner, 0, 0, clk, hints)

	_, err := sink.Create(context.Background(), newEvent("throttled"))
	assert.Assert(t, err != nil, "error not returned")

	delay, source := sink.muteDelay(err)
	assert.Equal(t, 7*time.Second, delay, "unexpected delay")
	assert.Equal(t, "transport", source, "unexpected delay source")
	assert.Equal(t, clk.Now().Add(7*time.Second).UnixNano(), sink.muteUntil.Load(), "unexpected mute deadline")

	// a stale delay is ignored, the sink falls back to the default
	step(clk, muteHintValidity+time.Second)
	delay, source = sink.muteDelay(inner.err)
	assert.Equal(t, defaultEventMute, delay, "stale delay used")
	assert.Equal(t, "default", source, "unexpected delay source")
}

// the delay on the error wins, it is the one the server sent for this request
func TestEventSinkMuteSourcePriority(t *testing.T) {
	clk := clocktesting.NewFakePassiveClock(time.Now())
	hints := &muteHintHolder{}
	hints.record(7, clk.Now())
	sink := newRateLimitedEventSink(&fakeEventSink{}, 0, 0, clk, hints)

	delay, source := sink.muteDelay(apierrors.NewTooManyRequests("slow down", 30))
	assert.Equal(t, 30*time.Second, delay, "unexpected delay")
	assert.Equal(t, "header", source, "unexpected delay source")
}
