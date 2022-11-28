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

package conf

import (
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"

	schedulerconf "github.com/apache/yunikorn-k8shim/pkg/conf"
)

func TestConfigMapVars(t *testing.T) {
	// test valid settings
	conf := NewAdmissionControllerConf([]*v1.ConfigMap{nil, {Data: map[string]string{
		schedulerconf.CMSvcPolicyGroup:   "testPolicyGroup",
		AMWebHookAMServiceName:           "testYunikornService",
		AMWebHookSchedulerServiceAddress: "testAddress",
		AMFilteringProcessNamespaces:     "testProcessNamespaces",
		AMFilteringBypassNamespaces:      "testBypassNamespaces",
		AMFilteringLabelNamespaces:       "testLabelNamespaces",
		AMFilteringNoLabelNamespaces:     "testNolabelNamespaces",
		AMAccessControlBypassAuth:        "true",
		AMAccessControlSystemUsers:       "^systemuser$",
		AMAccessControlExternalUsers:     "^yunikorn$",
		AMAccessControlExternalGroups:    "^devs$",
		AMAccessControlTrustControllers:  "false",
	}}})
	assert.Equal(t, conf.GetPolicyGroup(), "testPolicyGroup")
	assert.Equal(t, conf.GetAmServiceName(), "testYunikornService")
	assert.Equal(t, conf.GetSchedulerServiceAddress(), "testAddress")
	assert.Equal(t, conf.GetProcessNamespaces()[0].String(), "testProcessNamespaces")
	assert.Equal(t, conf.GetBypassNamespaces()[0].String(), "testBypassNamespaces")
	assert.Equal(t, conf.GetLabelNamespaces()[0].String(), "testLabelNamespaces")
	assert.Equal(t, conf.GetNoLabelNamespaces()[0].String(), "testNolabelNamespaces")
	assert.Equal(t, conf.GetBypassAuth(), true)
	assert.Equal(t, conf.GetSystemUsers()[0].String(), "^systemuser$")
	assert.Equal(t, conf.GetExternalUsers()[0].String(), "^yunikorn$")
	assert.Equal(t, conf.GetExternalGroups()[0].String(), "^devs$")
	assert.Equal(t, conf.GetTrustControllers(), false)

	// test missing settings
	conf = NewAdmissionControllerConf([]*v1.ConfigMap{nil, nil})
	assert.Equal(t, conf.GetPolicyGroup(), schedulerconf.DefaultPolicyGroup)
	assert.Equal(t, conf.GetNamespace(), schedulerconf.DefaultNamespace)
	assert.Equal(t, conf.GetAmServiceName(), DefaultWebHookAmServiceName)
	assert.Equal(t, conf.GetSchedulerServiceAddress(), DefaultWebHookSchedulerServiceAddress)
	assert.Equal(t, 0, len(conf.GetProcessNamespaces()))
	assert.Equal(t, conf.GetBypassNamespaces()[0].String(), DefaultFilteringBypassNamespaces)
	assert.Equal(t, 0, len(conf.GetLabelNamespaces()))
	assert.Equal(t, 0, len(conf.GetNoLabelNamespaces()))
	assert.Equal(t, conf.GetBypassAuth(), DefaultAccessControlBypassAuth)
	assert.Equal(t, conf.GetSystemUsers()[0].String(), DefaultAccessControlSystemUsers)
	assert.Equal(t, 0, len(conf.GetExternalUsers()))
	assert.Equal(t, 0, len(conf.GetExternalGroups()))
	assert.Equal(t, conf.GetTrustControllers(), DefaultAccessControlTrustControllers)

	// test faulty settings for boolean values
	conf = NewAdmissionControllerConf([]*v1.ConfigMap{nil, {Data: map[string]string{
		AMAccessControlBypassAuth:       "xyz",
		AMAccessControlTrustControllers: "xyz",
	}}})
	assert.Equal(t, conf.GetBypassAuth(), DefaultAccessControlBypassAuth)
	assert.Equal(t, conf.GetTrustControllers(), DefaultAccessControlTrustControllers)

	// test disable / enable of config hot refresh
	conf = NewAdmissionControllerConf([]*v1.ConfigMap{nil, nil})

	// test set (hot refresh enabled)
	conf.updateConfigMaps([]*v1.ConfigMap{nil, {
		Data: map[string]string{
			schedulerconf.CMSvcPolicyGroup: "testPolicyGroup",
		},
	}}, false)
	assert.Equal(t, conf.GetPolicyGroup(), "testPolicyGroup")

	// update performed at the same time as disabling hot refresh should still work
	conf.updateConfigMaps([]*v1.ConfigMap{nil, {
		Data: map[string]string{
			schedulerconf.CMSvcEnableConfigHotRefresh: "false",
			schedulerconf.CMSvcPolicyGroup:            "testPolicyGroup2",
		},
	}}, false)
	assert.Equal(t, conf.GetPolicyGroup(), "testPolicyGroup2")

	// update performed even when resetting hot refresh should not work
	conf.updateConfigMaps([]*v1.ConfigMap{nil, {
		Data: map[string]string{
			schedulerconf.CMSvcEnableConfigHotRefresh: "true",
			schedulerconf.CMSvcPolicyGroup:            "testPolicyGroup3",
		},
	}}, false)

	// update again should also not change, as hot-refresh is one-way disable
	assert.Equal(t, conf.GetPolicyGroup(), "testPolicyGroup2")
	conf.updateConfigMaps([]*v1.ConfigMap{nil, {
		Data: map[string]string{
			schedulerconf.CMSvcEnableConfigHotRefresh: "true",
			schedulerconf.CMSvcPolicyGroup:            "testPolicyGroup4",
		},
	}}, false)
	assert.Equal(t, conf.GetPolicyGroup(), "testPolicyGroup2")
}
