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
	createDispatcher(t)
	defer createDispatcher(t)
	dispatcher.stopTimeout = 50 * time.Millisecond

	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseHandler()

	processed := make(chan string, 2)
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
	Dispatch(TestAppEvent{appID: "blocked", eventType: RunApplication})
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not start processing the blocking event")
	}

	Stop()
	assert.Assert(t, !dispatcher.isRunning())

	Start()
	Dispatch(TestAppEvent{appID: "replacement", eventType: RunApplication})
	select {
	case appID := <-processed:
		assert.Equal(t, appID, "replacement")
	case <-time.After(time.Second):
		t.Fatal("replacement dispatcher did not process an event after shutdown timed out")
	}

	releaseHandler()
	select {
	case appID := <-processed:
		assert.Equal(t, appID, "blocked")
	case <-time.After(time.Second):
		t.Fatal("stale dispatcher handler did not return")
	}

	assert.Assert(t, dispatcher.isRunning(), "stale dispatcher changed the replacement lifecycle state")
}

func TestDispatcherStartWaitsForConcurrentStop(t *testing.T) {
	createDispatcher(t)
	defer createDispatcher(t)
	dispatcher.stopTimeout = 200 * time.Millisecond

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

	stopReturned := make(chan struct{})
	go func() {
		Stop()
		close(stopReturned)
	}()
	err := utils.WaitForCondition(func() bool {
		return !dispatcher.isRunning()
	}, time.Millisecond, time.Second)
	assert.NilError(t, err)

	startReturned := make(chan struct{})
	go func() {
		Start()
		close(startReturned)
	}()
	select {
	case <-startReturned:
		t.Fatal("Start returned before the concurrent Stop completed")
	case <-time.After(25 * time.Millisecond):
	}

	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after its timeout")
	}
	select {
	case <-startReturned:
	case <-time.After(time.Second):
		t.Fatal("Start did not create a replacement after Stop completed")
	}
	assert.Assert(t, dispatcher.isRunning())
	releaseHandler()
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
		if d.isRunning() {
			Stop()
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
