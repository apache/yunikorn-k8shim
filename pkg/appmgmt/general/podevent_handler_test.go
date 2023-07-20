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

package general

import (
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sEvents "k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

const appID = "app00001"

func TestHandleAsyncEventDuringRecovery(t *testing.T) {
	amProtocol := cache.NewMockedAMProtocol()
	podEventHandler := NewPodEventHandler(amProtocol, true)
	pod1 := newPod("pod1")
	pod2 := newPod("pod2")
	pod3 := newPod("pod3")

	app1 := podEventHandler.HandleEvent(AddPod, Informers, pod1)
	app2 := podEventHandler.HandleEvent(UpdatePod, Informers, pod2)
	app3 := podEventHandler.HandleEvent(AddPod, Recovery, pod3)

	assert.Equal(t, len(podEventHandler.asyncEvents), 2)
	assert.Equal(t, podEventHandler.asyncEvents[0].pod, pod1)
	assert.Equal(t, int(podEventHandler.asyncEvents[0].eventType), AddPod)
	assert.Equal(t, podEventHandler.asyncEvents[1].pod, pod2)
	assert.Equal(t, int(podEventHandler.asyncEvents[1].eventType), UpdatePod)
	assert.Equal(t, nil, app1)
	assert.Equal(t, nil, app2)
	assert.Equal(t, cache.ApplicationStates().Recovering, app3.GetApplicationState())
}

func TestHandleAsyncEventWhenNotRecovering(t *testing.T) {
	amProtocol := cache.NewMockedAMProtocol()
	podEventHandler := NewPodEventHandler(amProtocol, false)

	pod1 := newPod("pod1")
	pod2 := newPod("pod2")

	app1 := podEventHandler.HandleEvent(AddPod, Informers, pod1)
	app2 := podEventHandler.HandleEvent(UpdatePod, Informers, pod2)
	app3 := podEventHandler.HandleEvent(DeletePod, Informers, pod2)

	assert.Equal(t, len(podEventHandler.asyncEvents), 0)
	assert.Assert(t, app1 != nil)
	assert.Assert(t, app2 != nil)
	assert.Assert(t, app3 != nil)
}

func TestRecoveryDone(t *testing.T) {
	amProtocol := cache.NewMockedAMProtocol()
	podEventHandler := NewPodEventHandler(amProtocol, true)

	pod1 := newPod("pod1")
	pod2 := newPod("pod2")

	podEventHandler.HandleEvent(AddPod, Informers, pod1)
	podEventHandler.HandleEvent(AddPod, Informers, pod2)
	podEventHandler.HandleEvent(DeletePod, Informers, pod1)

	seenPods := map[string]bool{
		string(pod2.UID): true, // should not be added
	}
	podEventHandler.RecoveryDone(seenPods)

	assert.Equal(t, len(podEventHandler.asyncEvents), 0)
	app := amProtocol.GetApplication(appID)

	task, err := app.GetTask("pod1")
	assert.NilError(t, err)
	assert.Equal(t, cache.TaskStates().Completed, task.GetTaskState())

	_, err = app.GetTask("pod2")
	assert.ErrorContains(t, err, "task pod2 doesn't exist in application")

	assert.Equal(t, false, podEventHandler.recoveryRunning)
}

func TestSingleUserPerApplication(t *testing.T) {
	conf.GetSchedulerConf().SetTestMode(true)
	api := client.NewMockedAPIProvider(false)
	cache.NewContext(api)
	recorder, ok := events.GetRecorder().(*k8sEvents.FakeRecorder)
	if !ok {
		t.Fatal("the EventRecorder is expected to be of type FakeRecorder")
	}

	amprotocol := cache.NewMockedAMProtocol()
	podEvent := NewPodEventHandler(amprotocol, false)

	am := NewManager(client.NewMockedAPIProvider(false), podEvent)

	pod1 := v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00001",
			Namespace: "default",
			UID:       "UID-POD-00001",
			Labels: map[string]string{
				"queue":                        "root.a",
				"yunikorn.apache.org/username": "test",
				"applicationId":                appID,
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// submit the app
	am.AddPod(&pod1)

	pod := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod00002",
			Namespace: "default",
			UID:       "UID-POD-00002",
			Labels: map[string]string{
				"queue":                        "root.a",
				"yunikorn.apache.org/username": "test1",
				"applicationId":                appID,
			},
		},
		Spec: v1.PodSpec{SchedulerName: constants.SchedulerName},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	// submit the same app with different user
	am.AddPod(pod)

	message := "Rejecting pod because application ID " + appID + " belongs to a different user"

	// ensure there is no event
	err := utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, message) {
					return true
				}
			default:
				return false
			}
		}
	}, 50*time.Millisecond, time.Second)
	assert.Error(t, err, "timeout waiting for condition")

	// set SingleUserPerApplication to true
	err = conf.UpdateConfigMaps([]*v1.ConfigMap{
		{Data: map[string]string{conf.CMSvcSingleUserPerApplication: "true"}},
	}, true)
	assert.NilError(t, err, "UpdateConfigMap failed")

	// submit the same app with different user and ensure specific rejection event has been published
	am.AddPod(pod)

	err = utils.WaitForCondition(func() bool {
		for {
			select {
			case event := <-recorder.Events:
				if strings.Contains(event, message) {
					return true
				}
			default:
				return false
			}
		}
	}, 50*time.Millisecond, time.Second)
	assert.NilError(t, err, "event should have been emitted")
}

func newPod(name string) *v1.Pod {
	return newPodByUser(name, "nobody")
}

func newPodByUser(name string, user string) *v1.Pod {
	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name),
			Labels: map[string]string{
				"queue":                    "root.a",
				"applicationId":            appID,
				constants.DefaultUserLabel: user,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
		},
	}
}
