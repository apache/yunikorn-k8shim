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
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
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
	assert.Equal(t, conf.ClusterVersion, buildVersion)
	assert.Equal(t, conf.EventChannelCapacity, DefaultEventChannelCapacity)
	assert.Equal(t, conf.DispatchTimeout, DefaultDispatchTimeout)
	assert.Equal(t, conf.KubeQPS, DefaultKubeQPS)
	assert.Equal(t, conf.KubeBurst, DefaultKubeBurst)
	assert.Equal(t, conf.UserLabelKey, constants.DefaultUserLabel)
}

func TestDecompress(t *testing.T) {
	var b bytes.Buffer
	gzWriter := gzip.NewWriter(&b)
	if _, err := gzWriter.Write([]byte(configs.DefaultSchedulerConfig)); err != nil {
		assert.NilError(t, err, "expected nil, got error while compressing test schedulerConfig")
	}
	if err := gzWriter.Close(); err != nil {
		assert.NilError(t, err, "expected nil, got error")
		t.Fatal("expected nil, got error")
	}
	key, decodedConfigString := Decompress("queues.yaml."+constants.GzipSuffix, b.Bytes())
	assert.Equal(t, "queues.yaml", key)
	assert.Equal(t, configs.DefaultSchedulerConfig, decodedConfigString)
}

func TestDecompressUnknownKey(t *testing.T) {
	encodedConfigString := make([]byte, base64.StdEncoding.EncodedLen(len([]byte(configs.DefaultSchedulerConfig))))
	base64.StdEncoding.Encode(encodedConfigString, []byte(configs.DefaultSchedulerConfig))
	key, decodedConfigString := Decompress("queues.yaml.bin", []byte(configs.DefaultSchedulerConfig))
	assert.Equal(t, "queues.yaml", key)
	assert.Assert(t, len(decodedConfigString) == 0, "expected decodedConfigString to be nil")
}

func TestDecompressBadCompression(t *testing.T) {
	encodedConfigString := make([]byte, base64.StdEncoding.EncodedLen(len([]byte(configs.DefaultSchedulerConfig))))
	base64.StdEncoding.Encode(encodedConfigString, []byte(configs.DefaultSchedulerConfig))
	key, decodedConfigString := Decompress("queues.yaml."+constants.GzipSuffix, encodedConfigString)
	assert.Equal(t, "", key)
	assert.Assert(t, !strings.EqualFold(configs.DefaultSchedulerConfig, decodedConfigString), "expected decodedConfigString to be nil")
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
		{CMSvcDisableGangScheduling, "DisableGangScheduling", true},
		{CMSvcEnableConfigHotRefresh, "EnableConfigHotRefresh", false},
		{CMSvcPlaceholderImage, "PlaceHolderImage", "test-image"},
		{CMSvcNodeInstanceTypeNodeLabelKey, "InstanceTypeNodeLabelKey", "node.kubernetes.io/instance-type"},
		{CMKubeQPS, "KubeQPS", 2345},
		{CMKubeBurst, "KubeBurst", 3456},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prev := CreateDefaultConfig()
			conf, errs := parseConfig(map[string]string{tc.name: fmt.Sprintf("%v", tc.value)}, prev)
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
		{CMSvcDisableGangScheduling, "DisableGangScheduling", true, false},
		{CMSvcPlaceholderImage, "PlaceHolderImage", "test-image", false},
		{CMSvcNodeInstanceTypeNodeLabelKey, "InstanceTypeNodeLabelKey", "node.kubernetes.io/instance-type", false},
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
	conf, errs := parseConfig(map[string]string{"key": "value"}, prev)
	assert.Assert(t, conf != nil)
	assert.Assert(t, errs == nil, errs)
}

func TestParseConfigMapWithInvalidInt(t *testing.T) {
	prev := CreateDefaultConfig()
	conf, errs := parseConfig(map[string]string{CMSvcEventChannelCapacity: "x"}, prev)
	assert.Assert(t, conf == nil, "conf exists")
	assert.Equal(t, 1, len(errs), "wrong error count")
	assert.ErrorContains(t, errs[0], "invalid syntax", "wrong error type")
}

func TestParseConfigMapWithInvalidBool(t *testing.T) {
	prev := CreateDefaultConfig()
	conf, errs := parseConfig(map[string]string{CMSvcEnableConfigHotRefresh: "x"}, prev)
	assert.Assert(t, conf == nil, "conf exists")
	assert.Equal(t, 1, len(errs), "wrong error count")
	assert.ErrorContains(t, errs[0], "invalid syntax", "wrong error type")
}

func TestParseConfigMapWithInvalidDuration(t *testing.T) {
	prev := CreateDefaultConfig()
	conf, errs := parseConfig(map[string]string{CMSvcSchedulingInterval: "x"}, prev)
	assert.Assert(t, conf == nil, "conf exists")
	assert.Equal(t, 1, len(errs), "wrong error count")
	assert.ErrorContains(t, errs[0], "invalid duration", "wrong error type")
}

// get a configuration value by field name
func getConfValue(t *testing.T, conf *SchedulerConf, name string) interface{} {
	val := reflect.ValueOf(conf).Elem().FieldByName(name)
	assert.Assert(t, val.IsValid(), "Field not valid: "+name)
	return val.Interface()
}
