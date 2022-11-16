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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

func TestDefaultValues(t *testing.T) {
	conf := GetSchedulerConf()
	assertDefaults(t, conf)
}

func TestNilConfigMaps(t *testing.T) {
	err := UpdateConfigMaps([]*v1.ConfigMap{nil, nil}, true)
	assert.NilError(t, err, "UpdateConfigMap failed")

	conf := GetSchedulerConf()
	assertDefaults(t, conf)
}

func TestEmptyConfigMap(t *testing.T) {
	err := UpdateConfigMaps([]*v1.ConfigMap{nil, {Data: map[string]string{}}}, true)
	assert.NilError(t, err, "UpdateConfigMap failed")

	conf := GetSchedulerConf()
	assertDefaults(t, conf)
}

func TestOverriddenConfigMap(t *testing.T) {
	err := UpdateConfigMaps([]*v1.ConfigMap{
		{Data: map[string]string{CMSvcClusterID: "default"}},
		{Data: map[string]string{CMSvcClusterID: "override"}},
	}, true)
	assert.NilError(t, err, "UpdateConfigMap failed")

	conf := GetSchedulerConf()
	assert.Equal(t, "override", conf.ClusterID)
}

func assertDefaults(t *testing.T, conf *SchedulerConf) {
	assert.Equal(t, conf.ClusterID, DefaultClusterID)
	assert.Equal(t, conf.PolicyGroup, DefaultPolicyGroup)
	assert.Equal(t, conf.ClusterVersion, BuildVersion)
	assert.Equal(t, conf.LoggingLevel, DefaultLoggingLevel)
	assert.Equal(t, conf.EventChannelCapacity, DefaultEventChannelCapacity)
	assert.Equal(t, conf.DispatchTimeout, DefaultDispatchTimeout)
	assert.Equal(t, conf.KubeQPS, DefaultKubeQPS)
	assert.Equal(t, conf.KubeBurst, DefaultKubeBurst)
	assert.Equal(t, conf.UserLabelKey, constants.DefaultUserLabel)
}

func TestParseConfigMap(t *testing.T) {
	testCases := []struct {
		name  string
		field string
		value interface{}
	}{
		{CMSvcClusterID, "ClusterID", "test-cluster"},
		{CMSvcPolicyGroup, "PolicyGroup", "test-policy-group"},
		{CMSvcSchedulingInterval, "Interval", 5 * time.Second},
		{CMSvcVolumeBindTimeout, "VolumeBindTimeout", 15 * time.Second},
		{CMSvcEventChannelCapacity, "EventChannelCapacity", 1234},
		{CMSvcDispatchTimeout, "DispatchTimeout", 3 * time.Minute},
		{CMSvcOperatorPlugins, "OperatorPlugins", "test-operators"},
		{CMSvcDisableGangScheduling, "DisableGangScheduling", true},
		{CMSvcEnableConfigHotRefresh, "EnableConfigHotRefresh", false},
		{CMSvcPlaceholderImage, "PlaceHolderImage", "test-image"},
		{CMLogLevel, "LoggingLevel", -1},
		{CMKubeQPS, "KubeQPS", 2345},
		{CMKubeBurst, "KubeBurst", 3456},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prev := CreateDefaultConfig()
			seen := make(map[string]string)
			conf, errs := parseConfig(map[string]string{tc.name: fmt.Sprintf("%v", tc.value)}, prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Assert(t, errs == nil, errs)
			assert.Equal(t, tc.value, getConfValue(t, conf, tc.field))
		})
	}
}

func TestUpdateConfigMapNonReloadable(t *testing.T) {
	testCases := []struct {
		name       string
		field      string
		value      interface{}
		reloadable bool
	}{
		{CMSvcClusterID, "ClusterID", "test-cluster", false},
		{CMSvcPolicyGroup, "PolicyGroup", "test-policy-group", false},
		{CMSvcSchedulingInterval, "Interval", 5 * time.Second, false},
		{CMSvcVolumeBindTimeout, "VolumeBindTimeout", 15 * time.Second, false},
		{CMSvcEventChannelCapacity, "EventChannelCapacity", 1234, false},
		{CMSvcDispatchTimeout, "DispatchTimeout", 3 * time.Minute, false},
		{CMSvcOperatorPlugins, "OperatorPlugins", "test-operators", false},
		{CMSvcDisableGangScheduling, "DisableGangScheduling", true, false},
		{CMSvcPlaceholderImage, "PlaceHolderImage", "test-image", false},
		{CMLogLevel, "LoggingLevel", -1, true},
		{CMKubeQPS, "KubeQPS", 2345, false},
		{CMKubeBurst, "KubeBurst", 3456, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				err := UpdateConfigMaps([]*v1.ConfigMap{nil, nil}, true)
				assert.NilError(t, err, "failed to reset configmap")
			}()

			err := UpdateConfigMaps([]*v1.ConfigMap{nil, nil}, true)
			assert.NilError(t, err, "failed to set configmap")
			oldConf := GetSchedulerConf()

			err = UpdateConfigMaps([]*v1.ConfigMap{nil, {Data: map[string]string{tc.name: fmt.Sprintf("%v", tc.value)}}}, false)
			assert.NilError(t, err, "failed to update configmap")
			newConf := GetSchedulerConf()
			if tc.reloadable {
				assert.Equal(t, tc.value, getConfValue(t, newConf, tc.field), "reloadable field not updated")
			} else {
				assert.Equal(t, getConfValue(t, oldConf, tc.field), getConfValue(t, newConf, tc.field), "non-reloadable field updated")
			}
		})
	}
}

func TestParseConfigMapWithUnknownKeyDoesNotFail(t *testing.T) {
	prev := CreateDefaultConfig()
	seen := make(map[string]string)
	conf, errs := parseConfig(map[string]string{"key": "value"}, prev, seen)
	assert.Assert(t, conf != nil)
	assert.Assert(t, errs == nil, errs)
}

func TestParseConfigMapWithInvalidInt(t *testing.T) {
	prev := CreateDefaultConfig()
	seen := make(map[string]string)
	conf, errs := parseConfig(map[string]string{CMSvcEventChannelCapacity: "x"}, prev, seen)
	assert.Assert(t, conf == nil, "conf exists")
	assert.Equal(t, 1, len(errs), "wrong error count")
	assert.ErrorContains(t, errs[0], "invalid syntax", "wrong error type")
}

func TestParseConfigMapWithInvalidBool(t *testing.T) {
	prev := CreateDefaultConfig()
	seen := make(map[string]string)
	conf, errs := parseConfig(map[string]string{CMSvcEnableConfigHotRefresh: "x"}, prev, seen)
	assert.Assert(t, conf == nil, "conf exists")
	assert.Equal(t, 1, len(errs), "wrong error count")
	assert.ErrorContains(t, errs[0], "invalid syntax", "wrong error type")
}

func TestParseConfigMapWithInvalidDuration(t *testing.T) {
	prev := CreateDefaultConfig()
	seen := make(map[string]string)
	conf, errs := parseConfig(map[string]string{CMSvcSchedulingInterval: "x"}, prev, seen)
	assert.Assert(t, conf == nil, "conf exists")
	assert.Equal(t, 1, len(errs), "wrong error count")
	assert.ErrorContains(t, errs[0], "invalid duration", "wrong error type")
}

func TestParseEnv(t *testing.T) {
	testCases := []struct {
		name     string
		value    string
		obsolete bool
		getter   func(conf *SchedulerConf) string
	}{
		{DeprecatedEnvUserLabelKey, "test-user-label-key", true, nil},
		{DeprecatedEnvOperatorPlugins, "test-plugins", false, func(c *SchedulerConf) string { return c.OperatorPlugins }},
		{DeprecatedEnvPlaceholderImage, "test-image", false, func(c *SchedulerConf) string { return c.PlaceHolderImage }},
	}

	oldLookup := setEnvLookupFunc(envLookupFunc)
	defer setEnvLookupFunc(oldLookup)

	for _, tc := range testCases {
		env := map[string]string{tc.name: tc.value}
		_ = setEnvLookupFunc(func(s string) (string, bool) {
			val, ok := env[s]
			return val, ok
		})

		t.Run(tc.name, func(t *testing.T) {
			prev := CreateDefaultConfig()
			seen := make(map[string]string)
			conf, warns := parseEnvConfig(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			if tc.obsolete {
				assert.Equal(t, 1, len(warns), "wrong warning count")
				assert.Assert(t, strings.Contains(warns[0], "Obsolete"))
			} else {
				assert.Assert(t, warns == nil, warns)
			}
			if tc.getter != nil {
				assert.Equal(t, tc.value, tc.getter(conf))
			}
		})
	}
}
func TestParseEnvDeprecated(t *testing.T) {
	testCases := []struct {
		name  string
		key   string
		value string
		field string
	}{
		{DeprecatedEnvOperatorPlugins, CMSvcOperatorPlugins, "test-plugins", "OperatorPlugins"},
		{DeprecatedEnvPlaceholderImage, CMSvcPlaceholderImage, "test-image", "PlaceHolderImage"},
	}

	oldLookup := setEnvLookupFunc(envLookupFunc)
	defer setEnvLookupFunc(oldLookup)

	for _, tc := range testCases {
		env := map[string]string{tc.name: tc.value}
		_ = setEnvLookupFunc(func(s string) (string, bool) {
			val, ok := env[s]
			return val, ok
		})

		t.Run(tc.name, func(t *testing.T) {
			prev := CreateDefaultConfig()
			seen := make(map[string]string)

			// check for consistent override
			cm, errs := parseConfig(map[string]string{tc.key: tc.value}, prev, seen)
			assert.Assert(t, errs == nil, errs)
			conf, warns := parseEnvConfig(cm, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, tc.value, getConfValue(t, conf, tc.field))
			assert.Assert(t, warns == nil, warns)

			// check for inconsistent override
			cm, errs = parseConfig(map[string]string{tc.key: "original-value"}, prev, seen)
			assert.Assert(t, errs == nil, errs)
			conf, warns = parseEnvConfig(cm, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, "original-value", getConfValue(t, conf, tc.field))
			assert.Equal(t, 1, len(warns), "warning not present")
			assert.Assert(t, strings.Contains(warns[0], "inconsistent"), "foo", "wrong warning")
		})
	}
}

func TestParseCliDefaults(t *testing.T) {
	prev := CreateDefaultConfig()
	seen := make(map[string]string)
	conf, warns := parseCliConfig(prev, seen)
	assert.Assert(t, warns == nil, warns)
	assertDefaults(t, conf)
}

func TestParseCli(t *testing.T) {
	testCases := []struct {
		name      string
		key       string
		field     string
		value     interface{}
		prevValue interface{}
	}{
		{DeprecatedCliClusterID, CMSvcClusterID, "ClusterID", "test-cluster", "prev-cluster"},
		{DeprecatedCliPolicyGroup, CMSvcPolicyGroup, "PolicyGroup", "test-policy-group", "prev-policy-group"},
		{DeprecatedCliInterval, CMSvcSchedulingInterval, "Interval", 5 * time.Second, 15 * time.Second},
		{DeprecatedCliLogLevel, CMLogLevel, "LoggingLevel", -1, 0},
		{DeprecatedCliVolumeBindTimeout, CMSvcVolumeBindTimeout, "VolumeBindTimeout", 3 * time.Minute, 5 * time.Minute},
		{DeprecatedCliEventChannelCapacity, CMSvcEventChannelCapacity, "EventChannelCapacity", 1234, 4321},
		{DeprecatedCliDispatchTimeout, CMSvcDispatchTimeout, "DispatchTimeout", 15 * time.Second, 5 * time.Second},
		{DeprecatedCliKubeQPS, CMKubeQPS, "KubeQPS", 2345, 5432},
		{DeprecatedCliKubeBurst, CMKubeBurst, "KubeBurst", 3456, 6543},
		{DeprecatedCliOperatorPlugins, CMSvcOperatorPlugins, "OperatorPlugins", "test-operators", "prev-operators"},
		{DeprecatedCliEnableConfigHotRefresh, CMSvcEnableConfigHotRefresh, "EnableConfigHotRefresh", false, true},
		{DeprecatedCliDisableGangScheduling, CMSvcDisableGangScheduling, "DisableGangScheduling", true, false},
		{DeprecatedCliPlaceHolderImage, CMSvcPlaceholderImage, "PlaceHolderImage", "test-image", "prev-image"},
	}

	oldLookup := setCliLookupFunc(cliArgsFunc)
	defer setCliLookupFunc(oldLookup)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setCliLookupFunc(func() (string, []string) {
				return "test", []string{fmt.Sprintf("-%s=%v", tc.name, tc.value)}
			})

			// validate updates
			prev := CreateDefaultConfig()
			seen := make(map[string]string)
			conf, warns := parseCliConfig(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Assert(t, warns == nil, warns)
			assert.Equal(t, tc.value, getConfValue(t, conf, tc.field))

			// validate conflicting overrides
			prev = CreateDefaultConfig()
			seen[tc.key] = tc.key
			setConfValue(t, prev, tc.field, tc.prevValue)
			conf, warns = parseCliConfig(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, 1, len(warns), "warnings not present")
			assert.Assert(t, strings.Contains(warns[0], "inconsistent"), warns[0])
			assert.Equal(t, tc.prevValue, getConfValue(t, conf, tc.field))
		})
	}
}

func TestParseCliObsolete(t *testing.T) {
	testCases := []struct {
		name  string
		key   string
		field string
		value interface{}
	}{
		{DeprecatedCliClusterVersion, DeprecatedCliClusterVersion, "ClusterVersion", "1.0"},
		{DeprecatedCliUserLabelKey, DeprecatedCliUserLabelKey, "UserLabelKey", "test-user-label"},
		{DeprecatedCliLogEncoding, DeprecatedCliLogEncoding, "", "test-encoding"},
		{DeprecatedCliLogFile, DeprecatedCliLogFile, "", "test-log-file"},
	}

	oldLookup := setCliLookupFunc(cliArgsFunc)
	defer setCliLookupFunc(oldLookup)

	for _, tc := range testCases {
		setCliLookupFunc(func() (string, []string) {
			return "test", []string{fmt.Sprintf("-%s=%v", tc.key, tc.value)}
		})
		t.Run(tc.name, func(t *testing.T) {
			prev := CreateDefaultConfig()
			seen := make(map[string]string)
			conf, warns := parseCliConfig(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, 1, len(warns), "warnings not present")
			assert.Assert(t, strings.Contains(warns[0], "Obsolete"), warns[0])
			if tc.field != "" {
				assert.Equal(t, getConfValue(t, prev, tc.field), getConfValue(t, conf, tc.field))
			}
		})
	}
}

// set a configuration value by field name
func setConfValue(t *testing.T, conf *SchedulerConf, name string, value interface{}) {
	val := reflect.ValueOf(conf).Elem().FieldByName(name)
	assert.Assert(t, val.IsValid(), "Field not valid: "+name)
	val.Set(reflect.ValueOf(value))
}

// get a configuration value by field name
func getConfValue(t *testing.T, conf *SchedulerConf, name string) interface{} {
	val := reflect.ValueOf(conf).Elem().FieldByName(name)
	assert.Assert(t, val.IsValid(), "Field not valid: "+name)
	return val.Interface()
}
