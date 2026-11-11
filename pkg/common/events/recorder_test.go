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
	"reflect"
	"testing"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

func TestInit(t *testing.T) {
	// simply test the get won't fail
	recorder := GetRecorder()
	assert.Equal(t, reflect.TypeOf(recorder).String(), "*events.MockedRecorder")
}

// recordingRecorder collects the type of every event which reaches it
type recordingRecorder struct {
	recorded []string
}

func (r *recordingRecorder) Eventf(_ runtime.Object, _ runtime.Object, eventtype, _, _, _ string, _ ...interface{}) {
	r.recorded = append(r.recorded, eventtype)
}

// the level filter drops the events below the configured level before they are recorded
func TestLevelFilteredRecorder(t *testing.T) {
	testCases := []struct {
		level    string
		expected []string
	}{
		{conf.EventLevelNormal, []string{v1.EventTypeNormal, v1.EventTypeWarning}},
		{conf.EventLevelWarning, []string{v1.EventTypeWarning}},
	}

	original := conf.GetSchedulerConf()
	defer conf.SetSchedulerConf(original)

	for _, tc := range testCases {
		t.Run(tc.level, func(t *testing.T) {
			levelConf := conf.CreateDefaultConfig()
			levelConf.KubeEventLevel = tc.level
			conf.SetSchedulerConf(levelConf)

			inner := &recordingRecorder{}
			recorder := NewLevelFilteredRecorder(inner)
			recorder.Eventf(nil, nil, v1.EventTypeNormal, "reason", "action", "note")
			recorder.Eventf(nil, nil, v1.EventTypeWarning, "reason", "action", "note")

			assert.DeepEqual(t, tc.expected, inner.recorded)
		})
	}
}
