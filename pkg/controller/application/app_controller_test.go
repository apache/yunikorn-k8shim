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
	"context"
	"testing"

	"gotest.tools/assert"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	appv1 "github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
)

const defaultName = "example"
const defaultNamespace = "default"
const defaultQueue = "root.default"
const defaultUser = "default"

func TestDeleteApp(t *testing.T) {
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))
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
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))
	app := createApp(defaultName, defaultNamespace, defaultQueue)
	savedApp, err := am.apiProvider.GetAPIs().AppClient.ApacheV1alpha1().Applications(defaultNamespace).Create(context.Background(), &app, apis.CreateOptions{})
	assert.NilError(t, err)
	assert.Equal(t, savedApp.Name, app.Name)
	am.addApp(&app)
	appID := constructAppID(defaultName, defaultNamespace)
	managedApp := am.amProtocol.GetApplication(appID)
	assert.Assert(t, managedApp != nil)
	assert.Equal(t, managedApp.GetApplicationState(), cache.ApplicationStates().New)
	assert.Equal(t, managedApp.GetQueue(), app.Spec.Queue)
	assert.Equal(t, managedApp.GetApplicationID(), appID)
}

func TestGetAppMetadata(t *testing.T) {
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))
	app := createApp(defaultName, defaultNamespace, defaultQueue)
	metadata, ok := am.getAppMetadata(&app)
	assert.Equal(t, ok, true)
	assert.Equal(t, metadata.ApplicationID, defaultNamespace+appIDDelimiter+defaultName)
	assert.Equal(t, metadata.QueueName, defaultQueue)
	assert.Equal(t, metadata.User, defaultUser)
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
		{"Killed", "Killed", appv1.KilledState},
		{"Invalid", "invalidState", appv1.ApplicationStateType(undefinedState)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := convertShimAppStateToAppCRDState(tc.coreState)
			assert.Equal(t, tc.expected, state)
		})
	}
}

func TestHandleApplicationStateUpdate(t *testing.T) {
	am := NewAppManager(cache.NewMockedAMProtocol(), client.NewMockedAPIProvider(false))
	app := createApp(defaultName, defaultNamespace, defaultQueue)
	am.addApp(&app)
	handleFunc := am.HandleApplicationStateUpdate()
	appID := constructAppID(defaultName, defaultNamespace)
	testCases := []struct {
		name  string
		event events.SchedulingEvent
	}{
		{"Not application event", cache.NewBindTaskEvent(appID, "taskID")},
		{"Not AppStateChange event", cache.NewSimpleApplicationEvent(appID, cache.AcceptApplication)},
		{"AppStateChange event", cache.NewApplicationStatusChangeEvent(appID, cache.AppStateChange, "New")},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handleFunc(tc.event)
			// no panic
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
			Queue: queue,
			SchedulingPolicy: appv1.SchedulingPolicy{
				Type: appv1.TryReserve,
			},
			TaskGroups: []appv1.TaskGroup{
				{
					Name:      "test-task-001",
					MinMember: 0,
				},
			},
		},
	}
	return app
}
