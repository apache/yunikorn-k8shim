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

package events

import (
	"sync"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

var eventRecorder events.EventRecorder = events.NewFakeRecorder(1024)
var once sync.Once
var lock sync.RWMutex

func GetRecorder() events.EventRecorder {
	lock.Lock()
	defer lock.Unlock()
	once.Do(func() {
		// note, the initiation of the event recorder requires on a workable Kubernetes client,
		// in test mode we should skip this and just use a fake recorder instead.
		configs := conf.GetSchedulerConf()
		if !configs.IsTestMode() {
			k8sClient := client.NewKubeClient(configs.KubeConfig)
			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{
				Interface: k8sClient.GetClientSet().EventsV1()})
			eventBroadcaster.StartRecordingToSink(make(<-chan struct{}))
			eventRecorder = eventBroadcaster.NewRecorder(scheme.Scheme, constants.SchedulerName)
		}
	})

	return eventRecorder
}

func SetRecorder(recorder events.EventRecorder) {
	lock.Lock()
	defer lock.Unlock()
	eventRecorder = recorder
	once.Do(func() {}) // make sure Do() doesn't fire elsewhere
}

func SetRecorderForTest(recorder events.EventRecorder) {
	lock.Lock()
	defer lock.Unlock()
	eventRecorder = recorder
}
