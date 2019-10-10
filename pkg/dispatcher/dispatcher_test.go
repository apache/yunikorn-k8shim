/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dispatcher

import (
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"gotest.tools/assert"
	"sync"
	"testing"
)

// app event for testing
type TestAppEvent struct {
	appId     string
	eventType events.ApplicationEventType
}

func (t TestAppEvent) GetApplicationId() string {
	return t.appId
}

func (t TestAppEvent) GetEvent() events.ApplicationEventType {
	return t.eventType
}

func (t TestAppEvent) GetArgs() []interface{} {
	return nil
}

func TestRegisterEventHandler(t *testing.T) {
	RegisterEventHandler(EventTypeApp, func(obj interface{}) {})
	RegisterEventHandler(EventTypeTask, func(obj interface{}) {})
	RegisterEventHandler(EventTypeTask, func(obj interface{}) {})
	assert.Equal(t, len(dispatcher.handlers), 2)
}

type appEventsRecorder struct {
	apps []string
	lock *sync.RWMutex
}

func (a *appEventsRecorder) addApp(appId string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.apps = append(a.apps, appId)
}

func (a *appEventsRecorder) contains(appId string) bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	for _, existingAppId := range a.apps {
		if existingAppId == appId {
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
	// thread safe
	recorder := &appEventsRecorder{
		apps: make([]string, 0),
		lock: &sync.RWMutex{},
	}

	RegisterEventHandler(EventTypeApp, func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			recorder.addApp(event.GetApplicationId())
		}
	})

	// start the dispatcher
	Start()

	// dispatch an event
	Dispatch(TestAppEvent{
		appId: "test-app-001",
		eventType: events.RunApplication,
	})
	Dispatch(TestAppEvent{
		appId: "test-app-002",
		eventType: events.RunApplication,
	})

	// wait until all events are handled
	dispatcher.drain()

	assert.Equal(t, recorder.size(), 2)
	assert.Equal(t, recorder.contains("test-app-001"), true)
	assert.Equal(t, recorder.contains("test-app-002"), true)

	// stop the dispatcher,
	Stop()

	// ensure state is stopped
	assert.Equal(t, dispatcher.isRunning(), false)

	// dispatch new events should fail
	if err := dispatcher.dispatch(TestAppEvent{
		appId: "test-app-002",
		eventType: events.RunApplication,
	}); err == nil {
		t.Fatalf("dispatch is not running, this should fail")
	} else {
		t.Logf("seen expected error: %v", err)
	}
}

// Test sending events from multiple senders in parallel,
// verify that events won't be lost
func TestDispatcherEventChannelFull(t *testing.T) {
	// capacity of event channel
	eventChannelCapacity := cap(dispatcher.eventChan)
	numSenders := 5

	// thread safe
	recorder := &appEventsRecorder{
		apps: make([]string, 0),
		lock: &sync.RWMutex{},
	}
	// pretend to be an time-consuming event-handler
	RegisterEventHandler(EventTypeApp, func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			recorder.addApp(event.GetApplicationId())
		}
	})

	// start the dispatcher
	Start()

	// send events
	wg := sync.WaitGroup{}
	wg.Add(numSenders)
	sendFunc := func (senderNo int) {
		for i := 0; i < eventChannelCapacity; i++ {
			Dispatch(TestAppEvent{
				appId: "test",
				eventType: events.RunApplication,
			})
		}
		wg.Done()
	}
	var fullFlag bool
	checkFullFunc := func () {
		for {
			if len(dispatcher.eventChan) == eventChannelCapacity {
				fullFlag = true
				break
			}
		}
	}
	go checkFullFunc()
	for i := 0; i < numSenders; i++ {
		go sendFunc(i)
	}

	// wait until all events are sent
	wg.Wait()

	// check event channel has been exhausted for a while
	assert.Equal(t, true, fullFlag)

	// wait until all events are handled
	dispatcher.drain()

	// assert all event are handled
	assert.Equal(t, recorder.size(), numSenders * eventChannelCapacity)

	// stop the dispatcher
	Stop()

	// ensure state is stopped
	assert.Equal(t, dispatcher.isRunning(), false)
}