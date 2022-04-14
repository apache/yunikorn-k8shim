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

	"gotest.tools/assert"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

func TestAppManagementService_GetManagerByName(t *testing.T) {
	conf.GetSchedulerConf().OperatorPlugins = "mocked-app-manager"
	amProtocol := cache.NewMockedAMProtocol()
	apiProvider := client.NewMockedAPIProvider(false)
	amService := NewAMService(amProtocol, apiProvider)
	amService.register(&mockedAppManager{})

	testCases := []struct {
		name       string
		appMgrName string
		found      bool
	}{
		{"registered", "mocked-app-manager", true},
		{"not registered", "not-registered-mgr", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			appMgr := amService.GetManagerByName(tc.appMgrName)
			if tc.found {
				assert.Assert(t, appMgr != nil)
				assert.Equal(t, appMgr.Name(), tc.appMgrName)
			} else {
				assert.Assert(t, appMgr == nil)
			}
		})
	}
}
