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

package dispatcher

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
)

// app event for testing
type TestAppEvent struct {
	appID     string
	eventType string
	flag      chan bool
}

func (t TestAppEvent) GetApplicationID() string {
	return t.appID
}

func (t TestAppEvent) GetEvent() string {
	return t.eventType
}

func (t TestAppEvent) GetArgs() []interface{} {
	return nil
}

const (
	RunApplication              = "RunApplication"
	maxTestAsyncDispatchDrain   = 15 * time.Second
	testAsyncDispatchPollPeriod = 100 * time.Millisecond
)

func TestRegisterEventHandler(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)

	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {})
	RegisterEventHandler("TestTaskHandler", EventTypeTask, func(obj interface{}) {})
	RegisterEventHandler("TestTaskHandler2", EventTypeTask, func(obj interface{}) {})
	assert.Equal(t, len(dispatcher.handlers), 2)
	assert.Equal(t, len(dispatcher.handlers[EventTypeTask]), 2)

	UnregisterEventHandler("TestTaskHandler2", EventTypeTask)
	assert.Equal(t, len(dispatcher.handlers), 2)
	assert.Equal(t, len(dispatcher.handlers[EventTypeTask]), 1)

	UnregisterEventHandler("TestTaskHandler", EventTypeTask)
	assert.Equal(t, len(dispatcher.handlers), 1)

	UnregisterEventHandler("TestAppHandler", EventTypeApp)
	assert.Equal(t, len(dispatcher.handlers), 0)
}

type appEventsRecorder struct {
	apps []string
	lock *locking.RWMutex
}

func (a *appEventsRecorder) addApp(appID string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.apps = append(a.apps, appID)
}

func (a *appEventsRecorder) contains(appID string) bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	for _, existingAppID := range a.apps {
		if existingAppID == appID {
			return true
		}
	}
	return false
}

func (a *appEventsRecorder) size() int {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return len(a.apps)
}

func TestDispatcherStartStop(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	// thread safe
	recorder := &appEventsRecorder{
		apps: make([]string, 0),
		lock: &locking.RWMutex{},
	}

	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			recorder.addApp(event.GetApplicationID())
		}
	})

	// start the dispatcher
	Start()

	// dispatch an event
	Dispatch(TestAppEvent{
		appID:     "test-app-001",
		eventType: RunApplication,
	})
	Dispatch(TestAppEvent{
		appID:     "test-app-002",
		eventType: RunApplication,
	})

	// wait until all events are handled
	dispatcher.drain()

	// stop the dispatcher,
	Stop()

	assert.Equal(t, recorder.size(), 2)
	assert.Equal(t, recorder.contains("test-app-001"), true)
	assert.Equal(t, recorder.contains("test-app-002"), true)

	// ensure state is stopped
	assert.Equal(t, dispatcher.isRunning(), false)

	// dispatch new events should fail
	if err := dispatcher.dispatch(TestAppEvent{
		appID:     "test-app-002",
		eventType: RunApplication,
	}); err == nil {
		t.Fatalf("dispatch is not running, this should fail")
	} else {
		t.Logf("seen expected error: %v", err)
	}
}

func TestDispatcherConcurrentStartStop(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)

	const callers = 100
	start := make(chan struct{})
	var started sync.WaitGroup
	var startTransitions atomic.Int32
	started.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer started.Done()
			<-start
			if dispatcher.start() {
				startTransitions.Add(1)
			}
		}()
	}
	close(start)
	started.Wait()
	assert.Equal(t, startTransitions.Load(), int32(1))
	assert.Assert(t, dispatcher.isRunning())

	stop := make(chan struct{})
	var stopped sync.WaitGroup
	var panics atomic.Int32
	stopped.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer stopped.Done()
			defer func() {
				if recover() != nil {
					panics.Add(1)
				}
			}()
			<-stop
			Stop()
		}()
	}
	close(stop)
	stopped.Wait()
	assert.Equal(t, panics.Load(), int32(0))
	assert.Assert(t, !dispatcher.isRunning())
}

func TestDispatcherRepeatedStartStop(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)

	processed := make(chan string, 1)
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		if event, ok := obj.(TestAppEvent); ok {
			processed <- event.appID
		}
	})

	for i := 0; i < 10; i++ {
		appID := fmt.Sprintf("test-app-%d", i)
		Start()
		assert.Assert(t, dispatcher.isRunning())
		Dispatch(TestAppEvent{appID: appID, eventType: RunApplication})
		select {
		case processedAppID := <-processed:
			assert.Equal(t, processedAppID, appID)
		case <-time.After(time.Second):
			t.Fatalf("dispatcher did not process event during lifecycle %d", i)
		}
		Stop()
		assert.Assert(t, !dispatcher.isRunning())
	}
}

func TestDispatcherStopBeforeStart(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)

	Stop()
	assert.Assert(t, !dispatcher.isRunning())
	Start()
	assert.Assert(t, dispatcher.isRunning())
	Stop()
	assert.Assert(t, !dispatcher.isRunning())
}

func TestDispatcherStopWaitsForWorker(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	dispatcher.stopTimeout = time.Second

	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseHandler()
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		close(blocked)
		<-release
	})

	Start()
	Dispatch(TestAppEvent{appID: "blocked", eventType: RunApplication})
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not start processing the blocking event")
	}

	stopComplete := make(chan struct{})
	go func() {
		Stop()
		close(stopComplete)
	}()

	select {
	case <-stopComplete:
		t.Fatal("Stop returned while the dispatcher worker was still active")
	case <-time.After(50 * time.Millisecond):
	}
	releaseHandler()

	select {
	case <-stopComplete:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after the dispatcher worker exited")
	}
	assert.Assert(t, !dispatcher.isRunning())
}

func TestDispatcherRestartAfterStopTimeout(t *testing.T) {
	testDispatcherRestartWaitsForWorker(t, false)
}

func TestDispatcherStartWaitsForConcurrentStop(t *testing.T) {
	testDispatcherRestartWaitsForWorker(t, true)
}

func testDispatcherRestartWaitsForWorker(t *testing.T, startDuringStop bool) {
	t.Helper()
	createDispatcher(t)
	defer createDispatcher(t)
	d := dispatcher
	d.stopTimeout = 50 * time.Millisecond
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()

	var active, maximum atomic.Int32
	processed := make(chan string, 3)
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		count := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); count > previous; previous = maximum.Load() {
			if maximum.CompareAndSwap(previous, count) {
				break
			}
		}
		event, ok := obj.(TestAppEvent)
		if !ok {
			return
		}
		if event.appID == "blocked" {
			close(blocked)
			<-release
		}
		processed <- event.appID
	})

	Start()
	run := d.currentRun
	Dispatch(TestAppEvent{appID: "blocked", eventType: RunApplication})
	waitDispatcherSignal(t, blocked, "handler did not start")
	assert.NilError(t, d.dispatch(TestAppEvent{appID: "queued", eventType: RunApplication}))
	stopReturned := make(chan struct{})
	go func() {
		d.stop()
		close(stopReturned)
	}()
	waitDispatcherSignal(t, run.stopChan, "Stop did not signal shutdown")
	if !startDuringStop {
		waitDispatcherSignal(t, stopReturned, "Stop did not return after its timeout")
	}

	const callers = 8
	var transitions atomic.Int32
	var starters sync.WaitGroup
	starters.Add(callers)
	startReturned := make(chan struct{})
	for i := 0; i < callers; i++ {
		go func() {
			defer starters.Done()
			if d.start() {
				transitions.Add(1)
			}
		}()
	}
	go func() {
		starters.Wait()
		close(startReturned)
	}()
	defer func() {
		releaseHandler()
		waitDispatcherSignal(t, run.doneChan, "old worker did not exit")
		waitDispatcherSignal(t, startReturned, "Start did not finish after old worker exited")
		waitDispatcherSignal(t, stopReturned, "Stop did not finish")
	}()

	waitDispatcherSignal(t, stopReturned, "Stop exceeded its shutdown timeout")
	select {
	case <-startReturned:
		t.Fatal("Start returned while the previous worker was still handling an event")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Assert(t, !d.isRunning(), "stopping run must reject new events")
	releaseHandler()
	waitDispatcherSignal(t, startReturned, "Start did not restart after worker completion")
	assert.Equal(t, transitions.Load(), int32(1), "only one replacement run should start")
	assert.NilError(t, d.dispatch(TestAppEvent{appID: "replacement", eventType: RunApplication}))
	for _, expected := range []string{"blocked", "queued", "replacement"} {
		select {
		case appID := <-processed:
			assert.Equal(t, appID, expected)
		case <-time.After(time.Second):
			t.Fatalf("dispatcher did not process %s", expected)
		}
	}
	assert.Equal(t, maximum.Load(), int32(1), "event handlers must not overlap across runs")
}

func waitDispatcherSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func TestAsyncDispatchStopsWithDispatcherRun(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	dispatcher.eventChan = make(chan events.SchedulingEvent, 1)
	dispatcher.asyncDispatchCheckInterval = time.Second
	dispatcher.dispatchTimeout = 5 * time.Second
	dispatcher.stopTimeout = 50 * time.Millisecond

	blocked := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		if event, ok := obj.(TestAppEvent); ok && event.appID == "blocked" {
			close(blocked)
			<-release
		}
	})

	Start()
	Dispatch(TestAppEvent{appID: "blocked", eventType: RunApplication})
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not start processing the blocking event")
	}
	Dispatch(TestAppEvent{appID: "queued", eventType: RunApplication})
	Dispatch(TestAppEvent{appID: "async", eventType: RunApplication})
	assert.Equal(t, dispatcher.asyncDispatchCount.Load(), int32(1))

	Stop()
	err := utils.WaitForCondition(func() bool {
		return dispatcher.asyncDispatchCount.Load() == 0
	}, time.Millisecond, time.Second)
	assert.NilError(t, err)
}

// Test sending events from multiple senders in parallel,
// verify that events won't be lost
func TestEventWillNotBeLostWhenEventChannelIsFull(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	dispatcher.eventChan = make(chan events.SchedulingEvent, 1)

	// thread safe
	recorder := &appEventsRecorder{
		apps: make([]string, 0),
		lock: &locking.RWMutex{},
	}
	// pretend to be an time-consuming event-handler
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			recorder.addApp(event.GetApplicationID())
			time.Sleep(1 * time.Millisecond)
		}
	})

	// start the dispatcher
	Start()

	// send events
	numEvents := 10
	for i := 0; i < numEvents; i++ {
		Dispatch(TestAppEvent{
			appID:     "test",
			eventType: RunApplication,
		})
	}

	// check event channel is full and some events are dispatched asynchronously
	assert.Assert(t, dispatcher.asyncDispatchCount.Load() > 0)

	// wait until all events are handled
	dispatcher.drain()

	// stop the dispatcher
	Stop()

	// assert all event are handled
	assert.Equal(t, recorder.size(), numEvents)
	assert.Assert(t, dispatcher.asyncDispatchCount.Load() == 0)

	// ensure state is stopped
	assert.Equal(t, dispatcher.isRunning(), false)
}

// Test dispatch timeout, verify that Dispatcher#asyncDispatch is called when event channel is full
// and will disappear after timeout.
func TestDispatchTimeout(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	// reset event channel with small capacity for testing
	dispatcher.eventChan = make(chan events.SchedulingEvent, 1)
	dispatcher.asyncDispatchCheckInterval = 100 * time.Millisecond
	dispatcher.dispatchTimeout = 500 * time.Millisecond

	// start the handler, but waiting on a flag
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		if appEvent, ok := obj.(TestAppEvent); ok {
			fmt.Printf("handling %s\n", appEvent.appID)
			<-appEvent.flag
			fmt.Printf("handling %s DONE\n", appEvent.appID)
		}
	})

	// start the dispatcher
	Start()

	// dispatch 3 events, the third event will be dispatched asynchronously
	stop := make(chan bool)
	for i := 0; i < 3; i++ {
		Dispatch(TestAppEvent{
			appID:     fmt.Sprintf("test-%d", i),
			eventType: RunApplication,
			flag:      stop,
		})
	}

	// give it a small amount of time,
	// 1st event should be picked up and stuck at handling
	// 2nd one should be added to the channel
	// 3rd one should be posted as an async request
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, dispatcher.asyncDispatchCount.Load(), int32(1))

	// verify Dispatcher#asyncDispatch is called
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	assert.Assert(t, strings.Contains(string(buf), "asyncDispatch"))

	// wait until async dispatch routine times out
	err := utils.WaitForCondition(func() bool {
		return dispatcher.asyncDispatchCount.Load() == 0
	}, testAsyncDispatchPollPeriod, dispatcher.dispatchTimeout+dispatcher.asyncDispatchCheckInterval)
	assert.NilError(t, err)

	// verify no left-over thread
	buf = make([]byte, 1<<16)
	runtime.Stack(buf, true)
	assert.Assert(t, !strings.Contains(string(buf), "asyncDispatch"))

	// stop the dispatcher
	close(stop)
	Stop()
}

// Test exceeding the async-dispatch limit, should panic immediately.
func TestExceedAsyncDispatchLimit(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)

	// reset event channel with small capacity for testing
	dispatcher.eventChan = make(chan events.SchedulingEvent, 1)
	dispatcher.asyncDispatchLimit = 1
	// pretend to be an time-consuming event-handler
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		if _, ok := obj.(events.ApplicationEvent); ok {
			time.Sleep(2 * time.Second)
		}
	})
	// Handle errors in defer func with recover.
	defer func() {
		// stop the dispatcher
		Stop()
		// check error
		if err := recover(); err != nil {
			errStr, ok := err.(error)
			assert.Assert(t, ok, "Expected error type from panic, got %T", err)
			assert.Assert(t, strings.Contains(errStr.Error(), "dispatcher exceeds async-dispatch limit"))
		} else {
			t.Error("Panic should be caught here")
		}
	}()
	// start the dispatcher
	Start()
	// dispatch 4 events, the third and forth events will be dispatched asynchronously
	for i := 0; i < 4; i++ {
		Dispatch(TestAppEvent{
			appID:     "test",
			eventType: RunApplication,
		})
	}
}

// createDispatcher resets the dispatcher for a clean test state. It first drains
// any async dispatch goroutines left by a previous test before reinitialising,
// failing the test if they do not exit in time.
func createDispatcher(t *testing.T) {
	if dispatcher != nil {
		d := dispatcher
		d.lifecycleLock.RLock()
		run := d.currentRun
		d.lifecycleLock.RUnlock()
		d.stop()
		if run != nil {
			waitDispatcherSignal(t, run.doneChan, "previous dispatcher worker did not exit")
		}
		if d.asyncDispatchCount.Load() > 0 {
			waitTimeout := d.dispatchTimeout + d.asyncDispatchCheckInterval + time.Second
			if waitTimeout > maxTestAsyncDispatchDrain {
				waitTimeout = maxTestAsyncDispatchDrain
			}
			err := utils.WaitForCondition(func() bool {
				return d.asyncDispatchCount.Load() == 0
			}, testAsyncDispatchPollPeriod, waitTimeout)
			assert.NilError(t, err)
		}
	}
	once.Do(func() {}) // run nop, so that functions like RegisterEventHandler() won't run initDispatcher() again
	initDispatcher()
}

func TestAsyncDispatchCannotEnqueueAcrossStop(t *testing.T) {
	run := &dispatcherRun{stopChan: make(chan struct{}), doneChan: make(chan struct{})}
	d := &Dispatcher{
		currentRun:                 run,
		eventChan:                  make(chan events.SchedulingEvent, 1),
		asyncDispatchLimit:         10,
		asyncDispatchCheckInterval: time.Second,
		dispatchTimeout:            time.Second,
	}
	d.eventChan <- TestAppEvent{appID: "queued", eventType: RunApplication}
	func() {
		// Hold the same lock as Stop while a previously admitted async sender
		// gets space in the queue. It must recheck its run before enqueueing.
		d.lifecycleLock.Lock()
		defer d.lifecycleLock.Unlock()
		defer func() {
			run.stopping = true
			close(run.stopChan)
		}()
		d.asyncDispatch(TestAppEvent{appID: "async", eventType: RunApplication}, run)
		<-d.eventChan
		select {
		case <-d.eventChan:
			t.Error("async sender bypassed the lifecycle lock during shutdown")
		case <-time.After(50 * time.Millisecond):
		}
	}()
	err := utils.WaitForCondition(func() bool {
		return d.asyncDispatchCount.Load() == 0
	}, time.Millisecond, time.Second)
	assert.NilError(t, err)
	assert.Equal(t, len(d.eventChan), 0, "stopped run must not enqueue into the shared event channel")
}

func TestAsyncDispatchResumesWhenQueueHasSpace(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	d := dispatcher
	d.eventChan = make(chan events.SchedulingEvent, 1)
	// Freeing queue space must wake senders without waiting for this interval.
	d.asyncDispatchCheckInterval = time.Hour
	d.dispatchTimeout = 2 * time.Hour
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()
	processed := make(chan string, 3)
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(obj interface{}) {
		event, ok := obj.(TestAppEvent)
		if !ok {
			return
		}
		if event.appID == "blocked" {
			close(blocked)
			<-release
		}
		processed <- event.appID
	})
	Start()
	assert.NilError(t, d.dispatch(TestAppEvent{appID: "blocked", eventType: RunApplication}))
	waitDispatcherSignal(t, blocked, "handler did not start")
	assert.NilError(t, d.dispatch(TestAppEvent{appID: "queued", eventType: RunApplication}))
	assert.NilError(t, d.dispatch(TestAppEvent{appID: "async", eventType: RunApplication}))
	assert.Equal(t, d.asyncDispatchCount.Load(), int32(1))
	releaseHandler()
	for _, expected := range []string{"blocked", "queued", "async"} {
		select {
		case appID := <-processed:
			assert.Equal(t, appID, expected)
		case <-time.After(time.Second):
			t.Fatalf("queue space did not promptly release event %s", expected)
		}
	}
}

func TestDispatcherConcurrentStopTimeout(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	d := dispatcher
	d.stopTimeout = 50 * time.Millisecond
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()
	RegisterEventHandler("TestAppHandler", EventTypeApp, func(_ interface{}) {
		close(blocked)
		<-release
	})
	Start()
	run := d.currentRun
	assert.NilError(t, d.dispatch(TestAppEvent{appID: "blocked", eventType: RunApplication}))
	waitDispatcherSignal(t, blocked, "handler did not start")

	const callers = 16
	start := make(chan struct{})
	var stopped sync.WaitGroup
	stopped.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer stopped.Done()
			<-start
			d.stop()
		}()
	}
	stopReturned := make(chan struct{})
	go func() {
		stopped.Wait()
		close(stopReturned)
	}()
	close(start)
	waitDispatcherSignal(t, stopReturned, "concurrent Stop callers exceeded their timeout")
	assert.Assert(t, !d.isRunning())
	select {
	case <-run.doneChan:
		t.Fatal("timeout must not report worker completion")
	default:
	}
	releaseHandler()
	waitDispatcherSignal(t, run.doneChan, "worker did not finish after handler returned")
}
