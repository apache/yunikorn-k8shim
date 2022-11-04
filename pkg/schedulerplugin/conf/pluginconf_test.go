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

	"github.com/spf13/cobra"
	"gotest.tools/assert"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	defaults "github.com/apache/yunikorn-k8shim/pkg/conf"
)

func TestParsePluginCliDefaultValues(t *testing.T) {
	cmd := &cobra.Command{}
	InitCliFlagSet(cmd)

	seen := make(map[string]string)
	defConfig := defaults.CreateDefaultConfig()
	conf := defConfig.Clone()

	ParsePluginCli(defConfig, seen)

	assert.Equal(t, conf.ClusterID, defaults.DefaultClusterID, "wrong cluster id")
	assert.Equal(t, conf.ClusterVersion, defaults.BuildVersion, "wrong cluster version")
	assert.Equal(t, conf.PolicyGroup, defaults.DefaultPolicyGroup, "wrong policy group")
	assert.Equal(t, conf.Interval, defaults.DefaultSchedulingInterval, "wrong scheduling interval")
	assert.Equal(t, conf.KubeConfig, defaults.GetDefaultKubeConfigPath(), "wrong kube config path")
	assert.Equal(t, conf.LoggingLevel, defaults.DefaultLoggingLevel, "wrong logging level")
	assert.Equal(t, conf.EventChannelCapacity, defaults.DefaultEventChannelCapacity, "wrong event channel capacity")
	assert.Equal(t, conf.DispatchTimeout, defaults.DefaultDispatchTimeout, "wrong dispatch timeout")
	assert.Equal(t, conf.KubeQPS, defaults.DefaultKubeQPS, "wrong kube qps")
	assert.Equal(t, conf.KubeBurst, defaults.DefaultKubeBurst, "wrong kube burst")
	assert.Equal(t, conf.OperatorPlugins, defaults.DefaultOperatorPlugins, "wrong operator plugins")
	assert.Equal(t, conf.EnableConfigHotRefresh, defaults.DefaultEnableConfigHotRefresh, "config hot refresh not enabled")
	assert.Equal(t, conf.DisableGangScheduling, defaults.DefaultDisableGangScheduling, "gang scheduling not enabled")
	assert.Equal(t, conf.UserLabelKey, constants.DefaultUserLabel, "wrong user label")
	assert.Equal(t, conf.SchedulerName, DefaultSchedulerName, "wrong scheduler name")
}

func TestParsePluginCliDeprecatedValues(t *testing.T) {
	testCases := []struct {
		name      string
		key       string
		field     string
		value     interface{}
		prevValue interface{}
	}{
		{DeprecatedArgClusterID, defaults.CMSvcClusterID, "ClusterID", "test-cluster-id", "cluster-id"},
		{DeprecatedArgPolicyGroup, defaults.CMSvcPolicyGroup, "PolicyGroup", "test-policy-group", "policy-group"},
		{DeprecatedArgSchedulingInterval, defaults.CMSvcSchedulingInterval, "Interval", 5 * time.Second, 3 * time.Second},
		{DeprecatedArgKubeConfig, defaults.EnvKubeConfig, "KubeConfig", "test-kube-config", "kube-config"},
		{DeprecatedArgLogLevel, defaults.CMLogLevel, "LoggingLevel", -1, 0},
		{DeprecatedArgEventChannelCapacity, defaults.CMSvcEventChannelCapacity, "EventChannelCapacity", 2345, 1234},
		{DeprecatedArgDispatchTimeout, defaults.CMSvcDispatchTimeout, "DispatchTimeout", 20 * time.Second, 10 * time.Second},
		{DeprecatedArgKubeQPS, defaults.CMKubeQPS, "KubeQPS", 2345, 1234},
		{DeprecatedArgKubeBurst, defaults.CMKubeBurst, "KubeBurst", 2345, 1234},
		{DeprecatedArgOperatorPlugins, defaults.CMSvcOperatorPlugins, "OperatorPlugins", "test-plugins", "plugins"},
		{DeprecatedArgEnableConfigHotRefresh, defaults.CMSvcEnableConfigHotRefresh, "EnableConfigHotRefresh", false, true},
		{DeprecatedArgDisableGangScheduling, defaults.CMSvcDisableGangScheduling, "DisableGangScheduling", true, false},
		{DeprecatedArgPlaceHolderImage, defaults.CMSvcOperatorPlugins, "PlaceHolderImage", "test-image", "image"},
	}

	oldFlags := pluginFlags
	defer func() { pluginFlags = oldFlags }()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// validate no overrides
			InitCliFlagSet(&cobra.Command{})
			args := []string{
				fmt.Sprintf("--%s=%v", tc.name, tc.value),
			}
			err := pluginFlags.Parse(args)
			assert.NilError(t, err, "errors in parsing")
			prev := defaults.CreateDefaultConfig()
			seen := make(map[string]string)
			conf, warns := ParsePluginCli(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, 0, len(warns), "warnings present")
			assert.Equal(t, tc.value, getConfValue(t, conf, tc.field))

			// validate incompatible overrides
			InitCliFlagSet(&cobra.Command{})
			err = pluginFlags.Parse(args)
			assert.NilError(t, err, "errors in parsing")
			setConfValue(t, prev, tc.field, tc.prevValue)
			seen[tc.key] = tc.key
			conf, warns = ParsePluginCli(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, 1, len(warns), "warning not present")
			assert.Assert(t, strings.Contains(warns[0], "inconsistent"), warns[0])
			assert.Equal(t, getConfValue(t, prev, tc.field), getConfValue(t, conf, tc.field))
		})
	}
}

func TestParsePluginCliObsoleteValues(t *testing.T) {
	testCases := []struct {
		name  string
		value interface{}
		field string
	}{
		{DeprecatedArgSchedulerName, "test-scheduler-name", "SchedulerName"},
		{DeprecatedArgClusterVersion, "test-cluster-version", "ClusterVersion"},
		{DeprecatedArgUserLabelKey, "test-user-label-key", "UserLabelKey"},
		{DeprecatedArgLogEncoding, "test-log-encoding", ""},
		{DeprecatedArgLogFile, "test-log-file", ""},
	}

	oldFlags := pluginFlags
	defer func() { pluginFlags = oldFlags }()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			InitCliFlagSet(cmd)
			args := []string{
				fmt.Sprintf("--%s=%v", tc.name, tc.value),
			}
			err := pluginFlags.Parse(args)
			assert.NilError(t, err, "errors in parsing")
			prev := defaults.CreateDefaultConfig()
			seen := make(map[string]string)
			conf, warns := ParsePluginCli(prev, seen)
			assert.Assert(t, conf != nil, "conf was nil")
			assert.Equal(t, 1, len(warns), "warning not present")
			assert.Assert(t, strings.Contains(warns[0], "Obsolete"), warns[0])
			if tc.field != "" {
				assert.Equal(t, getConfValue(t, prev, tc.field), getConfValue(t, conf, tc.field))
			}
		})
	}
}

// set a configuration value by field name
func setConfValue(t *testing.T, conf *defaults.SchedulerConf, name string, value interface{}) {
	val := reflect.ValueOf(conf).Elem().FieldByName(name)
	assert.Assert(t, val.IsValid(), "Field not valid: "+name)
	val.Set(reflect.ValueOf(value))
}

// get a configuration value by field name
func getConfValue(t *testing.T, conf *defaults.SchedulerConf, name string) interface{} {
	val := reflect.ValueOf(conf).Elem().FieldByName(name)
	assert.Assert(t, val.IsValid(), "Field not valid: "+name)
	return val.Interface()
}
