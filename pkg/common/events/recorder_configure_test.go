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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type testRecorder struct{}

func (tr *testRecorder) Event(runtime.Object, string, string, string) {}

func (tr *testRecorder) Eventf(runtime.Object, runtime.Object, string, string, string, string, ...interface{}) {
}

func (tr *testRecorder) AnnotatedEventf(runtime.Object, map[string]string, string, string, string, ...interface{}) {
}

func (tr *testRecorder) PastEventf(runtime.Object, metav1.Time, string, string, string, ...interface{}) {
}

func TestConfigureRecorderDisabledUsesMockedRecorder(t *testing.T) {
	defer UseMockedRecorder()

	ConfigureRecorder(&testRecorder{}, true)

	assert.Equal(t, reflect.TypeOf(GetRecorder()), reflect.TypeOf(&MockedRecorder{}))
}

func TestConfigureRecorderEnabledUsesProvidedRecorder(t *testing.T) {
	defer UseMockedRecorder()

	ConfigureRecorder(&testRecorder{}, false)

	assert.Equal(t, reflect.TypeOf(GetRecorder()), reflect.TypeOf(&testRecorder{}))
}
