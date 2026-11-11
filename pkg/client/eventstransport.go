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
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
)

const (
	// the header priority and fairness uses to advertise how long to wait
	retryAfterHeader = "Retry-After"
	// the longest a delay recorded by the transport is used by the sink
	muteHintValidity = 10 * time.Second
)

// muteHint is the delay the server advertised on a rejected request, handed from the
// transport to the sink which mutes the events
type muteHint struct {
	seconds    int
	recordedAt time.Time
}

// muteHintHolder holds the most recent delay advertised by the server
type muteHintHolder struct {
	hint atomic.Pointer[muteHint]
}

func (h *muteHintHolder) record(seconds int, now time.Time) {
	h.hint.Store(&muteHint{seconds: seconds, recordedAt: now})
}

// get returns the recorded delay if it was recorded recently enough to still describe the
// state of the server
func (h *muteHintHolder) get(now time.Time) (int, bool) {
	hint := h.hint.Load()
	if hint == nil || now.Sub(hint.recordedAt) > muteHintValidity {
		return 0, false
	}
	return hint.seconds, true
}

// eventsThrottleTransport records the delay advertised on a 429 and removes the Retry-After
// header from the response.
// Removing the header stops the REST client from retrying the request itself: it only retries
// a 429 which tells it how long to wait for (see checkWait in client-go rest/with_retry.go).
// Those retries sleep in the calling goroutine, which for events means the broadcaster
// goroutines pile up in the client for the whole time the server is throttling us, and the
// rejection never reaches the sink. Failing the request instead lets the sink mute the events
// for the window the server asked for, which is what the delay is for.
type eventsThrottleTransport struct {
	base  http.RoundTripper
	hints *muteHintHolder
	clock clock.PassiveClock
}

func (t *eventsThrottleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		return resp, err
	}

	if seconds, ok := retryAfterSeconds(resp); ok {
		t.hints.record(seconds, t.clock.Now())
	}
	resp.Header.Del(retryAfterHeader)
	return resp, nil
}

// retryAfterSeconds returns the advertised delay in seconds, or false if the header is
// missing or does not hold a number
func retryAfterSeconds(resp *http.Response) (int, bool) {
	header := resp.Header.Get(retryAfterHeader)
	if header == "" {
		return 0, false
	}
	seconds, err := strconv.Atoi(header)
	if err != nil {
		return 0, false
	}
	return seconds, true
}

// newEventsClientSet creates the events client. When hints are passed in the client fails
// fast on a 429 instead of retrying it internally, see eventsThrottleTransport.
func newEventsClientSet(kc string, hints *muteHintHolder, clk clock.PassiveClock) kubernetes.Interface {
	config := newRestConfig(kc, -1, 0, userAgentEvents)
	if hints != nil {
		config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			return &eventsThrottleTransport{base: rt, hints: hints, clock: clk}
		})
	}
	return newClientSetOrDie(config)
}
