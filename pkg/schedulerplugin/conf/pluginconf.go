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
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/component-base/cli/flag"
	"k8s.io/component-base/term"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

const (
	DefaultSchedulerName = "yunikorn"
)

// command line argument names
const (
	DeprecatedArgClusterID              = "yk-cluster-id"
	DeprecatedArgClusterVersion         = "yk-cluster-version"
	DeprecatedArgPolicyGroup            = "yk-policy-group"
	DeprecatedArgSchedulingInterval     = "yk-scheduling-interval"
	DeprecatedArgKubeConfig             = "yk-kube-config"
	DeprecatedArgLogLevel               = "yk-log-level"
	DeprecatedArgLogEncoding            = "yk-log-encoding"
	DeprecatedArgLogFile                = "yk-log-file"
	DeprecatedArgEventChannelCapacity   = "yk-event-channel-capacity"
	DeprecatedArgDispatchTimeout        = "yk-dispatcher-timeout"
	DeprecatedArgKubeQPS                = "yk-kube-qps"
	DeprecatedArgKubeBurst              = "yk-kube-burst"
	DeprecatedArgOperatorPlugins        = "yk-operator-plugins"
	DeprecatedArgEnableConfigHotRefresh = "yk-enable-config-hot-refresh"
	DeprecatedArgDisableGangScheduling  = "yk-disable-gang-scheduling"
	DeprecatedArgUserLabelKey           = "yk-user-label-key"
	DeprecatedArgSchedulerName          = "yk-scheduler-name"
	DeprecatedArgPlaceHolderImage       = "yk-placeholder-image"
)

func getStringArg(flag *pflag.Flag, defaultValue string) string {
	value := flag.Value.String()
	if value != "" {
		return value
	}
	return defaultValue
}

func getDurationArg(flag *pflag.Flag, defaultValue time.Duration) time.Duration {
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

type cliParser struct {
	seen     map[string]string
	warnings []string
}

func newCliParser(seen map[string]string) *cliParser {
	return &cliParser{
		seen:     seen,
		warnings: make([]string, 0),
	}
}

func (p *cliParser) obsoleteString(flag *pflag.Flag, name string, defValue string) {
	val := getStringArg(flag, defValue)
	p.warnings = append(p.warnings, fmt.Sprintf("Obsolete CLI option '%s' found. Ignoring value: '%s'", name, val))
}

func (p *cliParser) deprecatedString(flag *pflag.Flag, name string, seenKey string, defValue string, prevValue string, setter func(string)) {
	val := getStringArg(flag, defValue)
	if _, ok := p.seen[seenKey]; ok {
		if val != prevValue {
			p.warnings = append(p.warnings, fmt.Sprintf(
				"Deprecated CLI option '%s' found with inconsistent value. Provided: '%s', used: '%s'", name, val, prevValue))
		}
	} else {
		setter(val)
		p.seen[seenKey] = name
	}
}

func (p *cliParser) deprecatedInt(flag *pflag.Flag, name string, seenKey string, defValue int, prevValue int, setter func(int)) {
	val := getIntArg(flag, defValue)
	if _, ok := p.seen[seenKey]; ok {
		if val != prevValue {
			p.warnings = append(p.warnings, fmt.Sprintf(
				"Deprecated CLI option '%s' found with inconsistent value. Provided: '%d', used: '%d'", name, val, prevValue))
		}
	} else {
		setter(val)
		p.seen[seenKey] = name
	}
}

func (p *cliParser) deprecatedBool(flag *pflag.Flag, name string, seenKey string, defValue bool, prevValue bool, setter func(bool)) {
	val := getBoolArg(flag, defValue)
	if _, ok := p.seen[seenKey]; ok {
		if val != prevValue {
			p.warnings = append(p.warnings, fmt.Sprintf(
				"Deprecated CLI option '%s' found with inconsistent value. Provided: '%t', used: '%t'", name, val, prevValue))
		}
	} else {
		setter(val)
		p.seen[seenKey] = name
	}
}

func (p *cliParser) deprecatedDuration(flag *pflag.Flag, name string, seenKey string, defValue time.Duration, prevValue time.Duration, setter func(duration time.Duration)) {
	val := getDurationArg(flag, defValue)
	if _, ok := p.seen[seenKey]; ok {
		if val != prevValue {
			p.warnings = append(p.warnings, fmt.Sprintf(
				"Deprecated CLI option '%s' found with inconsistent value. Provided: '%s', used: '%s'", name, val, prevValue))
		}
	} else {
		setter(val)
		p.seen[seenKey] = name
	}
}

func ParsePluginCli(prev *conf.SchedulerConf, seen map[string]string) (*conf.SchedulerConf, []string) {
	configuration := prev.Clone()

	if pluginFlags == nil {
		// too early to call logger here
		panic("Plugin flags not initialized")
	}

	parser := newCliParser(seen)

	pluginFlags.Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case DeprecatedArgSchedulerName:
			parser.obsoleteString(flag, DeprecatedArgSchedulerName, DefaultSchedulerName)
		case DeprecatedArgClusterID:
			parser.deprecatedString(flag, DeprecatedArgClusterID, conf.CMSvcClusterID, conf.DefaultClusterID, configuration.ClusterID,
				func(v string) { configuration.ClusterID = v })
		case DeprecatedArgClusterVersion:
			parser.obsoleteString(flag, DeprecatedArgClusterVersion, conf.BuildVersion)
		case DeprecatedArgPolicyGroup:
			parser.deprecatedString(flag, DeprecatedArgPolicyGroup, conf.CMSvcPolicyGroup, conf.DefaultPolicyGroup, configuration.PolicyGroup,
				func(v string) { configuration.PolicyGroup = v })
		case DeprecatedArgSchedulingInterval:
			parser.deprecatedDuration(flag, DeprecatedArgSchedulingInterval, conf.CMSvcSchedulingInterval, conf.DefaultSchedulingInterval, configuration.Interval,
				func(v time.Duration) { configuration.Interval = v })
		case DeprecatedArgKubeConfig:
			parser.deprecatedString(flag, DeprecatedArgKubeConfig, conf.EnvKubeConfig, conf.GetDefaultKubeConfigPath(), configuration.KubeConfig,
				func(v string) { configuration.KubeConfig = v })
		case DeprecatedArgLogEncoding:
			parser.obsoleteString(flag, DeprecatedArgLogEncoding, conf.DefaultLogEncoding)
		case DeprecatedArgLogLevel:
			parser.deprecatedInt(flag, DeprecatedArgLogLevel, conf.CMLogLevel, conf.DefaultLoggingLevel, configuration.LoggingLevel,
				func(v int) { configuration.LoggingLevel = v })
		case DeprecatedArgLogFile:
			parser.obsoleteString(flag, DeprecatedArgLogFile, "")
		case DeprecatedArgEventChannelCapacity:
			parser.deprecatedInt(flag, DeprecatedArgEventChannelCapacity, conf.CMSvcEventChannelCapacity, conf.DefaultEventChannelCapacity, configuration.EventChannelCapacity,
				func(v int) { configuration.EventChannelCapacity = v })
		case DeprecatedArgDispatchTimeout:
			parser.deprecatedDuration(flag, DeprecatedArgDispatchTimeout, conf.CMSvcDispatchTimeout, conf.DefaultDispatchTimeout, configuration.DispatchTimeout,
				func(v time.Duration) { configuration.DispatchTimeout = v })
		case DeprecatedArgKubeQPS:
			parser.deprecatedInt(flag, DeprecatedArgKubeQPS, conf.CMKubeQPS, conf.DefaultKubeQPS, configuration.KubeQPS,
				func(v int) { configuration.KubeQPS = v })
		case DeprecatedArgKubeBurst:
			parser.deprecatedInt(flag, DeprecatedArgKubeBurst, conf.CMKubeBurst, conf.DefaultKubeBurst, configuration.KubeBurst,
				func(v int) { configuration.KubeBurst = v })
		case DeprecatedArgEnableConfigHotRefresh:
			parser.deprecatedBool(flag, DeprecatedArgEnableConfigHotRefresh, conf.CMSvcEnableConfigHotRefresh, conf.DefaultEnableConfigHotRefresh, configuration.EnableConfigHotRefresh,
				func(v bool) { configuration.EnableConfigHotRefresh = v })
		case DeprecatedArgDisableGangScheduling:
			parser.deprecatedBool(flag, DeprecatedArgDisableGangScheduling, conf.CMSvcDisableGangScheduling, conf.DefaultDisableGangScheduling, configuration.DisableGangScheduling,
				func(v bool) { configuration.DisableGangScheduling = v })
		case DeprecatedArgUserLabelKey:
			parser.obsoleteString(flag, DeprecatedArgUserLabelKey, constants.DefaultUserLabel)
		case DeprecatedArgOperatorPlugins:
			parser.deprecatedString(flag, DeprecatedArgOperatorPlugins, conf.CMSvcOperatorPlugins, conf.DefaultOperatorPlugins, configuration.OperatorPlugins,
				func(v string) { configuration.OperatorPlugins = v })
		case DeprecatedArgPlaceHolderImage:
			parser.deprecatedString(flag, DeprecatedArgPlaceHolderImage, conf.CMSvcPlaceholderImage, constants.PlaceholderContainerImage, configuration.PlaceHolderImage,
				func(v string) { configuration.PlaceHolderImage = v })
		}
	})

	if len(parser.warnings) > 0 {
		return configuration, parser.warnings
	}
	return configuration, nil
}

// handle to the initialized flags so that we can read them later
var pluginFlags *pflag.FlagSet

func InitCliFlagSet(command *cobra.Command) {
	ykFlags := pflag.NewFlagSet("yunikorn", pflag.ExitOnError)

	ykFlags.String(DeprecatedArgSchedulerName, DefaultSchedulerName, "DEPRECATED: YuniKorn scheduler name (ignored)")
	ykFlags.String(DeprecatedArgClusterID, conf.DefaultClusterID,
		fmt.Sprintf("DEPRECATED: cluster id (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcClusterID))
	ykFlags.String(DeprecatedArgClusterVersion, conf.BuildVersion, "DEPRECATED: cluster version (ignored)")
	ykFlags.String(DeprecatedArgPolicyGroup, conf.DefaultPolicyGroup, "policy group")
	ykFlags.Duration(DeprecatedArgSchedulingInterval, conf.DefaultSchedulingInterval,
		fmt.Sprintf("DEPRECATED: scheduling interval in seconds (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcSchedulingInterval))
	ykFlags.String(DeprecatedArgKubeConfig, conf.GetDefaultKubeConfigPath(),
		fmt.Sprintf("DEPRECATED: absolute path to the kubeconfig file (use env: %s)", conf.EnvKubeConfig))
	ykFlags.Int(DeprecatedArgLogLevel, conf.DefaultLoggingLevel,
		fmt.Sprintf("DEPRECATED: logging level, from DEBUG (-1) to FATAL (5) (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMLogLevel))
	ykFlags.String(DeprecatedArgLogEncoding, conf.DefaultLogEncoding, "DEPRECATED: log encoding (ignored)")
	ykFlags.String(DeprecatedArgLogFile, "", "DEPRECATED: log file path (ignored)")
	ykFlags.Int(DeprecatedArgEventChannelCapacity, conf.DefaultEventChannelCapacity,
		fmt.Sprintf("DEPRECATED: event channel capacity of dispatcher (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcEventChannelCapacity))
	ykFlags.Duration(DeprecatedArgDispatchTimeout, conf.DefaultDispatchTimeout,
		fmt.Sprintf("DEPRECATED: timeout in seconds when dispatching an event (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcDispatchTimeout))
	ykFlags.Int(DeprecatedArgKubeQPS, conf.DefaultKubeQPS,
		fmt.Sprintf("DEPRECATED: maximum QPS to Kubernetes master from this client (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMKubeQPS))
	ykFlags.Int(DeprecatedArgKubeBurst, conf.DefaultKubeBurst,
		fmt.Sprintf("DEPRECATED: maximum burst for throttle to Kubernetes master from this client (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMKubeBurst))
	ykFlags.Bool(DeprecatedArgEnableConfigHotRefresh, conf.DefaultEnableConfigHotRefresh,
		fmt.Sprintf("DEPRECATED: enable automatic configuration reloading (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcEnableConfigHotRefresh))
	ykFlags.Bool(DeprecatedArgDisableGangScheduling, conf.DefaultDisableGangScheduling,
		fmt.Sprintf("DEPRECATED: disable gang scheduling (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcDisableGangScheduling))
	ykFlags.String(DeprecatedArgUserLabelKey, constants.DefaultUserLabel,
		"DEPRECATED: pod label key to be used to identify a user (ignored)")
	ykFlags.String(DeprecatedArgOperatorPlugins, conf.DefaultOperatorPlugins,
		fmt.Sprintf("DEPRECATED: comma-separated list of operator plugin names [general,spark-k8s-operator,%s] (use configmap: %s/%s)",
			constants.AppManagerHandlerName, conf.CMSvcOperatorPlugins, constants.ConfigMapName))
	ykFlags.String(DeprecatedArgPlaceHolderImage, constants.PlaceholderContainerImage,
		fmt.Sprintf("DEPRECATED: Docker image of the placeholder pod (use configmap: %s/%s)",
			constants.ConfigMapName, conf.CMSvcPlaceholderImage))

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
