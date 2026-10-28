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

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
)

// newAcceptedTestApp returns an application in the accepted state with one task group defined,
// which is the state Schedule sends through postAppAccepted and the shape that makes
// skipReservationStage walk the task map.
func newAcceptedTestApp(t *testing.T, appID string) *Application {
	t.Helper()
	app := NewApplication(appID, "root.abc", "test-user", testGroups, map[string]string{}, newMockSchedulerAPI())
	app.setTaskGroups([]TaskGroup{
		{
			Name:      "test-group-1",
			MinMember: 2,
			MinResource: map[string]resource.Quantity{
				v1.ResourceCPU.String():    resource.MustParse("500m"),
				v1.ResourceMemory.String(): resource.MustParse("500Mi"),
			},
		},
	})
	app.SetState(ApplicationStates().Accepted)
	return app
}

func newAcceptedTestTask(app *Application, index int) *Task {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("pod-%d", index),
			Namespace: "default",
			UID:       types.UID(fmt.Sprintf("pod-uid-%d", index)),
		},
		Spec: v1.PodSpec{},
	}
	return NewTask(fmt.Sprintf("task-%d", index), app, nil, pod)
}

// TestPostAppAcceptedConcurrentTaskAdd runs the scheduling loop against the task adds that arrive
// from the informers. Schedule calls postAppAccepted outside any state machine transition, so
// nothing holds the application lock for it, while addTask writes the task map under the lock.
// Reading the map there is a data race, and iterating it while it is written is a fatal
// "concurrent map iteration and map write".
//
// Run with -race. On the unfixed code this reports a data race between the read in
// skipReservationStage and the write in addTask.
func TestPostAppAcceptedConcurrentTaskAdd(t *testing.T) {
	app := newAcceptedTestApp(t, "app-accepted-race")
	// a couple of tasks up front so the map walk has something to iterate
	for i := 0; i < 4; i++ {
		app.addTask(newAcceptedTestTask(app, i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// the informer path: keeps adding tasks to the application under the lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 4; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			app.addTask(newAcceptedTestTask(app, i))
		}
	}()

	// the scheduling loop: calls Schedule on every interval
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			app.Schedule()
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestPostAppAcceptedReservationDecision checks the event dispatched by Schedule for each
// reservation decision, including recovered tasks and placeholders.
func TestPostAppAcceptedReservationDecision(t *testing.T) {
	for _, tc := range []struct {
		name        string
		noGroups    bool
		taskState   string
		placeholder bool
		wantEvent   ApplicationEventType
	}{
		{name: "no task groups", noGroups: true, wantEvent: RunApplication},
		{name: "no tasks", wantEvent: TryReserve},
		{name: "new task", taskState: TaskStates().New, wantEvent: TryReserve},
		{name: "allocated task", taskState: TaskStates().Allocated, wantEvent: RunApplication},
		{name: "bound task", taskState: TaskStates().Bound, wantEvent: RunApplication},
		{name: "bound placeholder", taskState: TaskStates().Bound, placeholder: true, wantEvent: TryReserve},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newAcceptedTestApp(t, "app-accepted-decision")
			if tc.noGroups {
				app.setTaskGroups(nil)
			}
			if tc.taskState != "" {
				task := newAcceptedTestTask(app, 0)
				task.sm.SetState(tc.taskState)
				task.placeholder = tc.placeholder
				app.addTask(task)
			}

			dispatched := make(chan events.ApplicationEvent, 1)
			const handlerID = "TestPostAppAcceptedReservationDecision"
			dispatcher.RegisterEventHandler(handlerID, dispatcher.EventTypeApp, func(event interface{}) {
				if appEvent, ok := event.(events.ApplicationEvent); ok && appEvent.GetApplicationID() == app.applicationID {
					dispatched <- appEvent
				}
			})
			dispatcher.Start()
			t.Cleanup(func() {
				dispatcher.Stop()
				dispatcher.UnregisterEventHandler(handlerID, dispatcher.EventTypeApp)
			})

			app.Schedule()
			select {
			case event := <-dispatched:
				assert.Equal(t, event.GetEvent(), tc.wantEvent.String())
			case <-time.After(3 * time.Second):
				t.Fatal("Schedule did not dispatch an application event")
			}
		})
	}
}
