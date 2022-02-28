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
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/component-base/cli/flag"
	"k8s.io/component-base/term"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
)

// default configuration options, can be overridden by command-line args
const (
	DefaultSchedulerName = "yunikorn"
)

// command line argument names
const (
	ArgClusterID              = "yk-cluster-id"
	ArgClusterVersion         = "yk-cluster-version"
	ArgPolicyGroup            = "yk-policy-group"
	ArgSchedulingInterval     = "yk-scheduling-interval"
	ArgKubeConfig             = "yk-kube-config"
	ArgLogLevel               = "yk-log-level"
	ArgLogEncoding            = "yk-log-encoding"
	ArgLogFile                = "yk-log-file"
	ArgEventChannelCapacity   = "yk-event-channel-capacity"
	ArgDispatchTimeout        = "yk-dispatcher-timeout"
	ArgKubeQPS                = "yk-kube-qps"
	ArgKubeBurst              = "yk-kube-burst"
	ArgOperatorPlugins        = "yk-operator-plugins"
	ArgEnableConfigHotRefresh = "yk-enable-config-hot-refresh"
	ArgDisableGangScheduling  = "yk-disable-gang-scheduling"
	ArgUserLabelKey           = "yk-user-label-key"
	ArgSchedulerName          = "yk-scheduler-name"
)

func getStringArg(flag *pflag.Flag, defaultValue string) string {
	value := flag.Value.String()
	if value != "" {
		return value
	}
	return defaultValue
}

func getTimeDurationArg(flag *pflag.Flag, defaultValue time.Duration) time.Duration {
	if parsedValue, err := time.ParseDuration(flag.Value.String()); err == nil {
		return parsedValue
	}
	return defaultValue
}

func getIntArg(flag *pflag.Flag, defaultValue int) int {
	if parsedValue, err := strconv.Atoi(flag.Value.String()); err == nil {
		return parsedValue
	}
	return defaultValue
}

func getBoolArg(flag *pflag.Flag, defaultValue bool) bool {
	if parsedValue, err := strconv.ParseBool(flag.Value.String()); err == nil {
		return parsedValue
	}
	return defaultValue
}

// SchedulerConfFactory implementation
func NewSchedulerConf() *conf.SchedulerConf {
	configuration := &conf.SchedulerConf{}

	if pluginFlags == nil {
		// too early to call logger here
		panic("Plugin flags not initialized")
	}

	pluginFlags.VisitAll(func(flag *pflag.Flag) {
		switch flag.Name {
		case ArgSchedulerName:
			configuration.SchedulerName = getStringArg(flag, DefaultSchedulerName)
		case ArgClusterID:
			configuration.ClusterID = getStringArg(flag, conf.DefaultClusterID)
		case ArgClusterVersion:
			configuration.ClusterVersion = getStringArg(flag, conf.DefaultClusterVersion)
		case ArgPolicyGroup:
			configuration.PolicyGroup = getStringArg(flag, conf.DefaultPolicyGroup)
		case ArgSchedulingInterval:
			configuration.Interval = getTimeDurationArg(flag, conf.DefaultSchedulingInterval)
		case ArgKubeConfig:
			configuration.KubeConfig = getStringArg(flag, "")
		case ArgLogEncoding:
			configuration.LogEncoding = getStringArg(flag, conf.DefaultLogEncoding)
		case ArgLogLevel:
			configuration.LoggingLevel = getIntArg(flag, conf.DefaultLoggingLevel)
		case ArgLogFile:
			configuration.LogFile = getStringArg(flag, "")
		case ArgEventChannelCapacity:
			configuration.EventChannelCapacity = getIntArg(flag, conf.DefaultEventChannelCapacity)
		case ArgDispatchTimeout:
			configuration.DispatchTimeout = getTimeDurationArg(flag, conf.DefaultDispatchTimeout)
		case ArgKubeQPS:
			configuration.KubeQPS = getIntArg(flag, conf.DefaultKubeQPS)
		case ArgKubeBurst:
			configuration.KubeBurst = getIntArg(flag, conf.DefaultKubeBurst)
		case ArgEnableConfigHotRefresh:
			configuration.EnableConfigHotRefresh = getBoolArg(flag, true)
		case ArgDisableGangScheduling:
			configuration.DisableGangScheduling = getBoolArg(flag, false)
		case ArgUserLabelKey:
			configuration.UserLabelKey = getStringArg(flag, constants.DefaultUserLabel)
		case ArgOperatorPlugins:
			configuration.OperatorPlugins = getStringArg(flag, "general,"+constants.AppManagerHandlerName)
		}
	})

	return configuration
}

// handle to the initialized flags so that we can read them later
var pluginFlags *pflag.FlagSet

func InitCliFlagSet(command *cobra.Command) {
	ykFlags := pflag.NewFlagSet("yunikorn", pflag.ExitOnError)
	ykFlags.String(ArgSchedulerName, DefaultSchedulerName, "scheduler name to register YuniKorn under")
	ykFlags.String(ArgClusterID, conf.DefaultClusterID, "cluster id")
	ykFlags.String(ArgClusterVersion, conf.DefaultClusterVersion, "cluster version")
	ykFlags.String(ArgPolicyGroup, conf.DefaultPolicyGroup, "policy group")
	ykFlags.Duration(ArgSchedulingInterval, conf.DefaultSchedulingInterval, "scheduling interval in seconds")
	ykFlags.String(ArgKubeConfig, "", "absolute path to the kubeconfig file")
	ykFlags.Int(ArgLogLevel, conf.DefaultLoggingLevel, "logging level, available range [-1, 5], from DEBUG to FATAL.")
	ykFlags.String(ArgLogEncoding, conf.DefaultLogEncoding, "log encoding, json or console")
	ykFlags.String(ArgLogFile, "", "absolute log file path")
	ykFlags.Int(ArgEventChannelCapacity, conf.DefaultEventChannelCapacity, "event channel capacity of dispatcher")
	ykFlags.Duration(ArgDispatchTimeout, conf.DefaultDispatchTimeout, "timeout in seconds when dispatching an event")
	ykFlags.Int(ArgKubeQPS, conf.DefaultKubeQPS, "the maximum QPS to kubernetes master from this client")
	ykFlags.Int(ArgKubeBurst, conf.DefaultKubeBurst, "the maximum burst for throttle to kubernetes master from this client")
	ykFlags.Bool(ArgEnableConfigHotRefresh, true, "enable auto-reload of scheduler configmap")
	ykFlags.Bool(ArgDisableGangScheduling, false, "disable gang scheduling")
	ykFlags.String(ArgUserLabelKey, constants.DefaultUserLabel, "provide pod label key to be used to identify an user")
	ykFlags.String(ArgOperatorPlugins, "general,"+constants.AppManagerHandlerName,
		"comma-separated list of operator plugin names, currently, only \"spark-k8s-operator\" and \""+
			constants.AppManagerHandlerName+"\" is supported.")

	ykNamedFlagSets := flag.NamedFlagSets{
		Order: []string{"YuniKorn"},
		FlagSets: map[string]*pflag.FlagSet{
			"YuniKorn": ykFlags,
		},
	}

	cols, _, err := term.TerminalSize(command.OutOrStdout())
	if err != nil {
		cols = 0
	}

	helpFunc := command.HelpFunc()
	command.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		helpFunc(cmd, args)
		flag.PrintSections(cmd.OutOrStdout(), ykNamedFlagSets, cols)
	})

	usageFunc := command.UsageFunc()
	command.SetUsageFunc(func(cmd *cobra.Command) error {
		err := usageFunc(cmd)
		flag.PrintSections(cmd.OutOrStderr(), ykNamedFlagSets, cols)
		return err
	})

	command.Flags().AddFlagSet(ykFlags)
	pluginFlags = ykFlags
}
