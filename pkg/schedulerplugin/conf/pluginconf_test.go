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

	"github.com/spf13/cobra"
	"gotest.tools/assert"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	defaults "github.com/apache/yunikorn-k8shim/pkg/conf"
)

func TestDefaultValues(t *testing.T) {
	cmd := &cobra.Command{}
	InitCliFlagSet(cmd)
	conf := NewSchedulerConf()

	assert.Equal(t, conf.ClusterID, defaults.DefaultClusterID, "wrong cluster id")
	assert.Equal(t, conf.ClusterVersion, defaults.DefaultClusterVersion, "wrong cluster version")
	assert.Equal(t, conf.PolicyGroup, defaults.DefaultPolicyGroup, "wrong policy group")
	assert.Equal(t, conf.Interval, defaults.DefaultSchedulingInterval, "wrong scheduling interval")
	assert.Equal(t, conf.KubeConfig, "", "wrong kube config path")
	assert.Equal(t, conf.LoggingLevel, defaults.DefaultLoggingLevel, "wrong logging level")
	assert.Equal(t, conf.LogEncoding, defaults.DefaultLogEncoding, "wrong log encoding")
	assert.Equal(t, conf.LogFile, "", "wrong log file")
	assert.Equal(t, conf.EventChannelCapacity, defaults.DefaultEventChannelCapacity, "wrong event channel capacity")
	assert.Equal(t, conf.DispatchTimeout, defaults.DefaultDispatchTimeout, "wrong dispatch timeout")
	assert.Equal(t, conf.KubeQPS, defaults.DefaultKubeQPS, "wrong kube qps")
	assert.Equal(t, conf.KubeBurst, defaults.DefaultKubeBurst, "wrong kube burst")
	assert.Equal(t, conf.OperatorPlugins, "general,yunikorn-app", "wrong operator plugins")
	assert.Equal(t, conf.EnableConfigHotRefresh, true, "config hot refresh not enabled")
	assert.Equal(t, conf.DisableGangScheduling, false, "gang scheduling not enabled")
	assert.Equal(t, conf.UserLabelKey, constants.DefaultUserLabel, "wrong user label")
	assert.Equal(t, conf.SchedulerName, DefaultSchedulerName, "wrong scheduler name")
}
