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

const RunApplication string = "RunApplication"

func TestRegisterEventHandler(t *testing.T) {
	createDispatcher()
	defer createDispatcher()

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
	createDispatcher()
	defer createDispatcher()
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

// Test sending events from multiple senders in parallel,
// verify that events won't be lost
func TestEventWillNotBeLostWhenEventChannelIsFull(t *testing.T) {
	createDispatcher()
	defer createDispatcher()
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
	assert.Assert(t, asyncDispatchCount.Load() > 0)

	// wait until all events are handled
	dispatcher.drain()

	// stop the dispatcher
	Stop()

	// assert all event are handled
	assert.Equal(t, recorder.size(), numEvents)
	assert.Assert(t, asyncDispatchCount.Load() == 0)

	// ensure state is stopped
	assert.Equal(t, dispatcher.isRunning(), false)
}

// Test dispatch timeout, verify that Dispatcher#asyncDispatch is called when event channel is full
// and will disappear after timeout.
func TestDispatchTimeout(t *testing.T) {
	createDispatcher()
	defer createDispatcher()
	// reset event channel with small capacity for testing
	dispatcher.eventChan = make(chan events.SchedulingEvent, 1)
	AsyncDispatchCheckInterval = 100 * time.Millisecond
	DispatchTimeout = 500 * time.Millisecond

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
	assert.Equal(t, asyncDispatchCount.Load(), int32(1))

	// verify Dispatcher#asyncDispatch is called
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	assert.Assert(t, strings.Contains(string(buf), "asyncDispatch"))

	// wait until async dispatch routine times out
	err := utils.WaitForCondition(func() bool {
		return asyncDispatchCount.Load() == int32(0)
	}, 100*time.Millisecond, DispatchTimeout+AsyncDispatchCheckInterval)
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
	createDispatcher()
	defer createDispatcher()

	// reset event channel with small capacity for testing
	dispatcher.eventChan = make(chan events.SchedulingEvent, 1)
	AsyncDispatchLimit = 1
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

func createDispatcher() {
	once.Do(func() {}) // run nop, so that functions like RegisterEventHandler() won't run initDispatcher() again
	initDispatcher()
}
