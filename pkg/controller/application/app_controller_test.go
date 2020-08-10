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

package application

import (
	"bytes"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	"net/url"
	"strings"
	"testing"

	"gotest.tools/assert"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	appv1 "github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
)

const defaultName = "example"
const defaultNamespace = "default"
const defaultQueue = "root.default"

// MemorySink implements zap.Sink by writing all messages to a buffer.
type MemorySink struct {
	*bytes.Buffer
}

// Implement Close and Sync as no-ops to satisfy the interface. The Write
// method is provided by the embedded buffer.

func (s *MemorySink) Close() error { return nil }
func (s *MemorySink) Sync() error  { return nil }

func TestHandleApplicationUpdate(t *testing.T) {
	appID := constructAppID(defaultName, defaultNamespace)
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())
	updateHandler := am.HandleApplicationStateUpdate()
	//successMsg := "Application status changed"

	testCases := []struct {
		name          string
		event         events.SchedulingEvent
		expectedWarnMsg       string
	}{
		{"Not application event", cache.NewSubmitTaskEvent(appID, "taskID"), "not an Application event"},
	}
	for _, tc := range testCases {
		sink := &MemorySink{new(bytes.Buffer)}
		zap.RegisterSink("memory", func(*url.URL) (zap.Sink, error) {
			return sink, nil
		})
		conf := zap.NewProductionConfig()
		// Redirect all messages to the MemorySink.
		conf.OutputPaths = []string{"memory://"}

		l, err := conf.Build()
		if err != nil {
			t.Fatal(err)
		}
		*log.Logger = *l
		t.Run(tc.name, func(t *testing.T) {
			updateHandler(tc.event)
			if len(tc.expectedWarnMsg) > 0 {
				output := sink.String()
				assert.Assert(t, strings.Contains(output, tc.expectedWarnMsg))
			} else {
				//assert.Assert(t, strings.Contains(buf.String(), successMsg))
			}
		})
	}

}

func TestDeleteApp(t *testing.T) {
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())
	app := createApp(defaultName, defaultNamespace, defaultQueue)
	appID := constructAppID(defaultName, defaultNamespace)
	am.addApp(&app)
	managedApp := am.amProtocol.GetApplication(appID)
	assert.Assert(t, managedApp != nil)

	am.deleteApp(&app)
	deletedApp := am.amProtocol.GetApplication(appID)
	assert.Assert(t, deletedApp == nil)
}

func TestAddApp(t *testing.T) {
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())
	app := createApp(defaultName, defaultNamespace, defaultQueue)
	am.apiProvider.GetAPIs().AppClient.ApacheV1alpha1().Applications(defaultNamespace).Create(&app)
	am.addApp(&app)
	appID := constructAppID(defaultName, defaultNamespace)
	managedApp := am.amProtocol.GetApplication(appID)
	assert.Assert(t, managedApp != nil)
	assert.Equal(t, managedApp.GetApplicationState(), events.States().Application.New)
	assert.Equal(t, managedApp.GetQueue(), app.Spec.Queue)
	assert.Equal(t, managedApp.GetApplicationID(), appID)
}

func TestGetAppMetadata(t *testing.T) {

	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider())
	app := createApp(defaultName, defaultNamespace, defaultQueue)
	metadata, ok := am.getAppMetadata(&app)
	assert.Equal(t, ok, true)
	assert.Equal(t, metadata.ApplicationID, defaultNamespace+appIDDelimiter+defaultName)
	assert.Equal(t, metadata.QueueName, defaultQueue)
	assert.Equal(t, metadata.User, "")
	assert.DeepEqual(t, metadata.Tags, map[string]string{"namespace": defaultNamespace})
}

func TestGetNameFromAppID(t *testing.T) {
	unknownFormatError := "unknown appID format"
	emptyAppIDError := "appID should not be empty"

	testCases := []struct {
		name          string
		appID         string
		appName       string
		errorExpected bool
		expectedError string
	}{
		{"Valid case", defaultNamespace + appIDDelimiter + defaultName, defaultName, false, ""},
		{"Missing delimiter", defaultNamespace + defaultName, "", true, unknownFormatError},
		{"Missing defaultName", defaultNamespace + appIDDelimiter, "", false, ""},
		{"Missing defaultNamespace", appIDDelimiter + defaultName, defaultName, false, ""},
		{"Wrong delimiter", defaultNamespace + "_" + defaultName, "", true, unknownFormatError},
		{"Empty appID", "", "", true, emptyAppIDError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			name, err := getNameFromAppID(tc.appID)
			if tc.errorExpected {
				assert.ErrorContains(t, err, tc.expectedError)
			} else {
				assert.NilError(t, err)
			}
			assert.Equal(t, tc.appName, name)
		})
	}
}

func TestConvertShimAppStateToAppCRDState(t *testing.T) {
	testCases := []struct {
		name      string
		coreState string
		expected  appv1.ApplicationStateType
	}{
		{"New", "New", appv1.NewApplicationState},
		{"Accepted", "Accepted", appv1.AcceptedState},
		{"Starting", "Starting", appv1.StartingState},
		{"Running", "Running", appv1.RunningState},
		{"Waiting", "Waiting", appv1.WaitingState},
		{"Rejected", "Rejected", appv1.RejectedState},
		{"Completed", "Completed", appv1.CompletedState},
		{"Invalid", "invalidState", appv1.ApplicationStateType(undefinedState)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := convertShimAppStateToAppCRDState(tc.coreState)
			assert.Equal(t, tc.expected, state)
		})
	}
}

func createApp(name string, namespace string, queue string) appv1.Application {
	app := appv1.Application{
		TypeMeta: apis.TypeMeta{
			Kind:       "Application",
			APIVersion: "v1alpha1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       "UID-APP-00001",
		},
		Spec: appv1.ApplicationSpec{
			MinMember: 0,
			Queue:     queue,
		},
	}
	return app
}
