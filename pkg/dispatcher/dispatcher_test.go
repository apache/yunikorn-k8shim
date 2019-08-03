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