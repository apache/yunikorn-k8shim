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
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

const (
	// the shortest time between two logs of the number of shed events
	eventShedLogInterval = 30 * time.Second
	// the longest the events are muted after the server asked us to back off
	maxEventMute = 60 * time.Second
	// the mute used when the server throttles without saying for how long
	defaultEventMute = time.Second
)

// rateLimitedEventSink sheds the Normal events which exceed the configured rate instead of
// sending them. Warning events are always sent: they explain why a pod is not running and
// there are few of them. The Normal events are the volume, three per scheduled pod
// (Scheduling, Scheduled and PodBindSuccessful) plus the informational records from the core.
// The broadcaster writes every event from its own goroutine, so a rate limiter on the events
// client would only pace those writes: the goroutines pile up for as long as a storm lasts.
// Events are discardable, dropping them here bounds a storm instead of delaying it.
// All events are muted for as long as the server asks us to back off: priority and fairness
// rejects with a 429 which carries the time to wait for.
type rateLimitedEventSink struct {
	inner     events.EventSink
	limiter   flowcontrol.RateLimiter
	rateLimit string
	clock     clock.PassiveClock
	// the delay advertised by the server, recorded by the transport which removes the header
	hints *muteHintHolder

	// number of events shed since they were last logged
	shed atomic.Int64
	// time the shed events were last logged, as unix nanoseconds
	lastLogged atomic.Int64
	// time the mute was last logged, as unix nanoseconds
	lastMuteLogged atomic.Int64
	// time until which no event is sent, as unix nanoseconds
	muteUntil atomic.Int64
}

// NewRateLimitedEventSink wraps an event sink and sheds the Normal events which exceed the
// given rate. A qps <= 0 disables shedding, every event is passed on to the wrapped sink.
func NewRateLimitedEventSink(inner events.EventSink, qps, burst int) events.EventSink {
	return newRateLimitedEventSink(inner, qps, burst, clock.RealClock{}, nil)
}

// newRateLimitedEventSink allows the clock and the delays recorded by the transport to be
// passed in for testing
func newRateLimitedEventSink(inner events.EventSink, qps, burst int, clk clock.PassiveClock, hints *muteHintHolder) *rateLimitedEventSink {
	sink := &rateLimitedEventSink{
		inner:     inner,
		rateLimit: "unlimited",
		clock:     clk,
		hints:     hints,
	}
	shedPolicy := sink.rateLimit
	// a qps <= 0 sheds nothing, a burst which is not set defaults to the qps
	if qps > 0 {
		if burst <= 0 {
			burst = qps
		}
		sink.rateLimit = fmt.Sprintf("%d qps / %d burst", qps, burst)
		sink.limiter = flowcontrol.NewTokenBucketRateLimiter(float32(qps), burst)
		shedPolicy = "shed above " + sink.rateLimit
	}

	log.Log(log.ShimClient).Info("creating event sink",
		zap.String("concern", userAgentEvents),
		zap.String("shedPolicy", shedPolicy))
	return sink
}

// NewEventSink creates the sink for the event broadcaster: events are written by the events
// client and shed above the configured event rate.
// The client and the sink share the delays advertised by the server: the client fails fast on
// a rejection instead of retrying it, the sink mutes the events until the advertised deadline.
func NewEventSink(kc string) events.EventSink {
	schedulerConf := conf.GetSchedulerConf()

	clk := clock.RealClock{}
	hints := &muteHintHolder{}
	sink := &events.EventSinkImpl{Interface: newEventsClientSet(kc, hints, clk).EventsV1()}
	return newRateLimitedEventSink(sink, schedulerConf.KubeEventQPS, schedulerConf.KubeEventBurst, clk, hints)
}

func (s *rateLimitedEventSink) Create(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	if s.shedEvent(event.Type) {
		return event, nil
	}
	result, err := s.inner.Create(ctx, event)
	s.checkThrottled(err)
	return result, err
}

func (s *rateLimitedEventSink) Update(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	if s.shedEvent(event.Type) {
		return event, nil
	}
	result, err := s.inner.Update(ctx, event)
	s.checkThrottled(err)
	return result, err
}

func (s *rateLimitedEventSink) Patch(ctx context.Context, oldEvent *eventsv1.Event, data []byte) (*eventsv1.Event, error) {
	if s.shedEvent(oldEvent.Type) {
		return oldEvent, nil
	}
	result, err := s.inner.Patch(ctx, oldEvent, data)
	s.checkThrottled(err)
	return result, err
}

// checkThrottled mutes the events for as long as the server asked us to back off. Priority
// and fairness rejects with a 429 and advertises a delay which grows while it keeps dropping
// requests, following it is cheaper than having every event rejected.
func (s *rateLimitedEventSink) checkThrottled(err error) {
	if err == nil || !errors.IsTooManyRequests(err) {
		return
	}
	delay, source := s.muteDelay(err)
	if delay > maxEventMute {
		delay = maxEventMute
	}

	muteUntil := s.clock.Now().Add(delay).UnixNano()
	for {
		current := s.muteUntil.Load()
		// a mute is only ever extended, a later response must not shorten it
		if muteUntil <= current {
			return
		}
		if s.muteUntil.CompareAndSwap(current, muteUntil) {
			break
		}
	}

	if s.claimLogInterval(&s.lastMuteLogged) {
		log.Log(log.ShimClient).Warn("muting events, the server asked to back off",
			zap.Duration("delay", delay),
			zap.String("delaySource", source),
			zap.String("rateLimit", s.rateLimit))
	}
}

// muteDelay returns how long the events must be muted and where that delay came from. The
// delay is normally recorded by the transport, which removes the header from the response to
// stop the REST client from retrying: the error the sink sees no longer carries it. It is
// still read from the error first, for the rejections which do not pass our transport.
func (s *rateLimitedEventSink) muteDelay(err error) (time.Duration, string) {
	if seconds, ok := errors.SuggestsClientDelay(err); ok && seconds > 0 {
		return time.Duration(seconds) * time.Second, "header"
	}
	if s.hints != nil {
		if seconds, ok := s.hints.get(s.clock.Now()); ok && seconds > 0 {
			return time.Duration(seconds) * time.Second, "transport"
		}
	}
	return defaultEventMute, "default"
}

// shedEvent returns true if the event must be dropped instead of being passed on. Dropping is
// reported to the broadcaster as a successful write: it does not retry and does not hold on
// to the goroutine which is recording the event.
// A Warning event is never shed by the bucket and spends no token on it. The mute is checked
// first and applies to every type: while the server is throttling us the event is rejected
// anyway, sending it changes nothing.
func (s *rateLimitedEventSink) shedEvent(eventType string) bool {
	muted := s.muted()
	if !muted && (eventType == v1.EventTypeWarning || s.limiter == nil || s.limiter.TryAccept()) {
		return false
	}
	s.shed.Add(1)
	s.logShedEvents(muted)
	return true
}

// muted returns true while the server asked us to stop sending events
func (s *rateLimitedEventSink) muted() bool {
	return s.clock.Now().UnixNano() < s.muteUntil.Load()
}

// logShedEvents logs the number of events shed since the last time they were logged, at most
// once every eventShedLogInterval. The first drop is always logged.
func (s *rateLimitedEventSink) logShedEvents(muted bool) {
	if !s.claimLogInterval(&s.lastLogged) {
		return
	}
	log.Log(log.ShimClient).Warn("events shed",
		zap.Int64("shedEvents", s.shed.Swap(0)),
		zap.Bool("muted", muted),
		zap.String("rateLimit", s.rateLimit))
}

// claimLogInterval returns true for the caller which claims the current logging interval, the
// callers racing with it just skip their log. The first call always claims the interval.
func (s *rateLimitedEventSink) claimLogInterval(last *atomic.Int64) bool {
	now := s.clock.Now().UnixNano()
	previous := last.Load()
	if now-previous < int64(eventShedLogInterval) {
		return false
	}
	return last.CompareAndSwap(previous, now)
}
