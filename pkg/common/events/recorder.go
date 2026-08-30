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
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

var eventRecorder atomic.Pointer[events.EventRecorder]

func init() {
	r := events.EventRecorder(NewMockedRecorder())
	eventRecorder.Store(&r)
}

func GetRecorder() events.EventRecorder {
	return *eventRecorder.Load()
}

func SetRecorder(recorder events.EventRecorder) {
	eventRecorder.Store(&recorder)
}

// levelFilteredRecorder drops the events below the configured event level
type levelFilteredRecorder struct {
	inner events.EventRecorder
}

// NewLevelFilteredRecorder wraps a recorder and drops the events below the configured
// kubernetes.eventLevel before they reach the broadcaster: nothing is copied, cached or
// written for a suppressed event.
func NewLevelFilteredRecorder(inner events.EventRecorder) events.EventRecorder {
	return &levelFilteredRecorder{inner: inner}
}

// Eventf reads the level on every call: the setting is hot-reloadable
func (r *levelFilteredRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	switch conf.GetSchedulerConf().KubeEventLevel {
	case conf.EventLevelNone:
		return
	case conf.EventLevelWarning:
		if eventtype != v1.EventTypeWarning {
			return
		}
	}
	r.inner.Eventf(regarding, related, eventtype, reason, action, note, args...)
}
