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

package appmgmt

import (
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"gotest.tools/assert"
)

func TestAppManagerRecoveryState(t *testing.T) {
	amProtocol := cache.NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider()
	amService := NewAMService(amProtocol, apiProvider)
	amService.register(&mockedAppManager{})

	// this should timeout
	apps := amService.kickoffRecovery()
	assert.Equal(t, len(apps), 2)

	for appId, app := range apps {
		assert.Assert(t, appId == "app01" || appId == "app02")
		assert.Equal(t, app.GetApplicationState(), events.States().Application.Recovering)
	}
}

func TestAppManagerRecoveryTimeout(t *testing.T) {
	amProtocol := cache.NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider()
	amService := NewAMService(amProtocol, apiProvider)
	amService.register(&mockedAppManager{})

	// this should timeout
	apps := amService.kickoffRecovery()
	assert.Equal(t, len(apps), 2)

	err := amService.waitForAppRecovery(apps, 3*time.Second)
	assert.ErrorContains(t, err, "timeout waiting for app recovery")
}

func TestAppManagerRecoveryExitCondition(t *testing.T) {
	amProtocol := cache.NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider()
	amService := NewAMService(amProtocol, apiProvider)
	amService.register(&mockedAppManager{})

	apps := amService.kickoffRecovery()
	assert.Equal(t, len(apps), 2)

	// simulate app recovery succeed
	for _, app := range apps {
		app.SetState(events.States().Application.Accepted)
	}

	// this should not timeout
	err := amService.waitForAppRecovery(apps, 3*time.Second)
	assert.Equal(t, err, nil)
}

type mockedAppManager struct {

}

func (ma *mockedAppManager) Name() string {
	return "mocked-app-manager"
}


func (ma *mockedAppManager) ServiceInit() error {
	return nil
}

func (ma *mockedAppManager) Start() error {
	return nil
}

func (ma *mockedAppManager) Stop() error {
	return nil
}

func (ma *mockedAppManager) ListApplications() (map[string]cache.ApplicationMetadata, error) {
	apps := make(map[string]cache.ApplicationMetadata)
	apps["app01"] = cache.ApplicationMetadata{
		ApplicationID: "app01",
		QueueName:     "root.a",
		User:          "",
		Tags:          nil,
	}
	apps["app02"] = cache.ApplicationMetadata{
		ApplicationID: "app02",
		QueueName:     "root.a",
		User:          "",
		Tags:          nil,
	}
	return apps, nil
}