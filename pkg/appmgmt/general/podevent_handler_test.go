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
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
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
	podEventHandler.HandleEvent(AddPod, Informers, pod1)
	podEventHandler.HandleEvent(DeletePod, Informers, pod1)

	podEventHandler.RecoveryDone()

	assert.Equal(t, len(podEventHandler.asyncEvents), 0)
	app := amProtocol.GetApplication(appID)
	task, err := app.GetTask("pod1")
	assert.NilError(t, err)
	assert.Equal(t, cache.TaskStates().Completed, task.GetTaskState())
	assert.Equal(t, false, podEventHandler.recoveryRunning)
}

func newPod(name string) *v1.Pod {
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
				"queue":         "root.a",
				"applicationId": appID,
			},
		},
	}
}
