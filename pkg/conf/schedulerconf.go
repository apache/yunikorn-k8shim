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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type SchedulerConfFactory = func() *SchedulerConf

const (
	// env vars
	EnvHome       = "HOME"
	EnvKubeConfig = "KUBECONFIG"
	EnvNamespace  = "NAMESPACE"

	// deprecated env vars
	DeprecatedEnvUserLabelKey     = "USER_LABEL_KEY"
	DeprecatedEnvOperatorPlugins  = "OPERATOR_PLUGINS"
	DeprecatedEnvPlaceholderImage = "PLACEHOLDER_IMAGE"

	// deprecated cli options
	DeprecatedCliClusterID              = "clusterId"
	DeprecatedCliClusterVersion         = "clusterVersion"
	DeprecatedCliDisableGangScheduling  = "disableGangScheduling"
	DeprecatedCliDispatchTimeout        = "dispatchTimeout"
	DeprecatedCliEnableConfigHotRefresh = "enableConfigHotRefresh"
	DeprecatedCliEventChannelCapacity   = "eventChannelCapacity"
	DeprecatedCliInterval               = "interval"
	DeprecatedCliKubeQPS                = "kubeQPS"
	DeprecatedCliKubeBurst              = "kubeBurst"
	DeprecatedCliKubeConfig             = "kubeConfig"
	DeprecatedCliLogEncoding            = "logEncoding"
	DeprecatedCliLogFile                = "logFile"
	DeprecatedCliLogLevel               = "logLevel"
	DeprecatedCliOperatorPlugins        = "operatorPlugins"
	DeprecatedCliPlaceHolderImage       = "placeHolderImage"
	DeprecatedCliPolicyGroup            = "policyGroup"
	DeprecatedCliUserLabelKey           = "userLabelKey"
	DeprecatedCliVolumeBindTimeout      = "volumeBindTimeout"

	// configmap prefixes
	PrefixService    = "service."
	PrefixLog        = "log."
	PrefixKubernetes = "kubernetes."

	// configmap service
	CMSvcClusterID              = PrefixService + "clusterId"
	CMSvcPolicyGroup            = PrefixService + "policyGroup"
	CMSvcSchedulingInterval     = PrefixService + "schedulingInterval"
	CMSvcVolumeBindTimeout      = PrefixService + "volumeBindTimeout"
	CMSvcEventChannelCapacity   = PrefixService + "eventChannelCapacity"
	CMSvcDispatchTimeout        = PrefixService + "dispatchTimeout"
	CMSvcOperatorPlugins        = PrefixService + "operatorPlugins"
	CMSvcDisableGangScheduling  = PrefixService + "disableGangScheduling"
	CMSvcEnableConfigHotRefresh = PrefixService + "enableConfigHotRefresh"
	CMSvcPlaceholderImage       = PrefixService + "placeholderImage"

	// configmap log
	CMLogLevel = PrefixLog + "level"

	// configmap kubernetes
	CMKubeQPS   = PrefixKubernetes + "qps"
	CMKubeBurst = PrefixKubernetes + "burst"

	// defaults
	DefaultNamespace              = "default"
	DefaultClusterID              = "mycluster"
	DefaultPolicyGroup            = "queues"
	DefaultSchedulingInterval     = time.Second
	DefaultVolumeBindTimeout      = 10 * time.Second
	DefaultEventChannelCapacity   = 1024 * 1024
	DefaultDispatchTimeout        = 300 * time.Second
	DefaultOperatorPlugins        = "general"
	DefaultDisableGangScheduling  = false
	DefaultEnableConfigHotRefresh = true
	DefaultLoggingLevel           = 0
	DefaultLogEncoding            = "console"
	DefaultKubeQPS                = 1000
	DefaultKubeBurst              = 1000
)

var (
	BuildVersion    string
	BuildDate       string
	IsPluginVersion bool
)

var once sync.Once
var confHolder atomic.Value
var cliParserFunc = parseCliConfig
var envLookupFunc = os.LookupEnv
var cliArgsFunc = getCommandLineArgs

type OptionParser func(prev *SchedulerConf, seen map[string]string) (*SchedulerConf, []string)
type envLookup func(string) (string, bool)
type cliLookup func() (string, []string)

// SetCliParser replaces the default CLI parser with an alternate (used for scheduler plugin configuration)
func SetCliParser(parser OptionParser) OptionParser {
	oldParser := cliParserFunc
	cliParserFunc = parser
	return oldParser
}

// used for testing
func setEnvLookupFunc(lookup envLookup) envLookup {
	prev := envLookupFunc
	envLookupFunc = lookup
	return prev
}

// used for testing
func setCliLookupFunc(lookup cliLookup) cliLookup {
	prev := cliArgsFunc
	cliArgsFunc = lookup
	return prev
}

type SchedulerConf struct {
	SchedulerName          string        `json:"schedulerName"`
	ClusterID              string        `json:"clusterId"`
	ClusterVersion         string        `json:"clusterVersion"`
	PolicyGroup            string        `json:"policyGroup"`
	Interval               time.Duration `json:"schedulingIntervalSecond"`
	KubeConfig             string        `json:"absoluteKubeConfigFilePath"`
	LoggingLevel           int           `json:"loggingLevel"`
	VolumeBindTimeout      time.Duration `json:"volumeBindTimeout"`
	TestMode               bool          `json:"testMode"`
	EventChannelCapacity   int           `json:"eventChannelCapacity"`
	DispatchTimeout        time.Duration `json:"dispatchTimeout"`
	KubeQPS                int           `json:"kubeQPS"`
	KubeBurst              int           `json:"kubeBurst"`
	OperatorPlugins        string        `json:"operatorPlugins"`
	EnableConfigHotRefresh bool          `json:"enableConfigHotRefresh"`
	DisableGangScheduling  bool          `json:"disableGangScheduling"`
	UserLabelKey           string        `json:"userLabelKey"`
	PlaceHolderImage       string        `json:"placeHolderImage"`
	Namespace              string        `json:"namespace"`
	sync.RWMutex
}

func (conf *SchedulerConf) Clone() *SchedulerConf {
	conf.RLock()
	defer conf.RUnlock()

	return &SchedulerConf{
		SchedulerName:          conf.SchedulerName,
		ClusterID:              conf.ClusterID,
		ClusterVersion:         conf.ClusterVersion,
		PolicyGroup:            conf.PolicyGroup,
		Interval:               conf.Interval,
		KubeConfig:             conf.KubeConfig,
		LoggingLevel:           conf.LoggingLevel,
		VolumeBindTimeout:      conf.VolumeBindTimeout,
		TestMode:               conf.TestMode,
		EventChannelCapacity:   conf.EventChannelCapacity,
		DispatchTimeout:        conf.DispatchTimeout,
		KubeQPS:                conf.KubeQPS,
		KubeBurst:              conf.KubeBurst,
		OperatorPlugins:        conf.OperatorPlugins,
		EnableConfigHotRefresh: conf.EnableConfigHotRefresh,
		DisableGangScheduling:  conf.DisableGangScheduling,
		UserLabelKey:           conf.UserLabelKey,
		PlaceHolderImage:       conf.PlaceHolderImage,
		Namespace:              conf.Namespace,
	}
}

func UpdateConfigMaps(configMaps []*v1.ConfigMap, initial bool) error {
	log.Logger().Info("reloading configuration")

	// start with defaults
	prev := CreateDefaultConfig()
	seen := make(map[string]string)

	// convert configmaps to map
	config := FlattenConfigMaps(configMaps)

	// parse values from configmaps
	cmConf, cmErrors := parseConfig(config, prev, seen)
	if cmErrors != nil {
		for _, err := range cmErrors {
			log.Logger().Error("failed to parse configmap entry", zap.Error(err))
		}
		return errors.New("failed to load configmap")
	}

	// parse values from CLI
	cliConf, cliWarnings := cliParserFunc(cmConf, seen)
	for _, warn := range cliWarnings {
		log.Logger().Warn("warning while parsing CLI entry", zap.String("warning", warn))
	}

	// parse values from ENV
	newConf, envWarnings := parseEnvConfig(cliConf, seen)
	for _, warn := range envWarnings {
		log.Logger().Warn("warning while parsing env configuration", zap.String("warning", warn))
	}

	// check for settings which cannot be hot-reloaded
	if !initial {
		oldConf := GetSchedulerConf()
		handleNonReloadableConfig(oldConf, newConf)
	}

	// update scheduler config with merged version
	SetSchedulerConf(newConf)
	conf := GetSchedulerConf()

	// update logger configuration
	log.GetZapConfigs().Level.SetLevel(zapcore.Level(conf.LoggingLevel))

	// update Kubernetes logger configuration
	updateKubeLogger(conf)

	// dump new scheduler configuration
	DumpConfiguration()

	return nil
}

func handleNonReloadableConfig(old *SchedulerConf, new *SchedulerConf) {
	// warn about and revert any settings which cannot be hot-reloaded
	checkNonReloadableString(CMSvcClusterID, &old.ClusterID, &new.ClusterID)
	checkNonReloadableString(CMSvcPolicyGroup, &old.PolicyGroup, &new.PolicyGroup)
	checkNonReloadableDuration(CMSvcSchedulingInterval, &old.Interval, &new.Interval)
	checkNonReloadableDuration(CMSvcVolumeBindTimeout, &old.VolumeBindTimeout, &new.VolumeBindTimeout)
	checkNonReloadableInt(CMSvcEventChannelCapacity, &old.EventChannelCapacity, &new.EventChannelCapacity)
	checkNonReloadableDuration(CMSvcDispatchTimeout, &old.DispatchTimeout, &new.DispatchTimeout)
	checkNonReloadableInt(CMKubeQPS, &old.KubeQPS, &new.KubeQPS)
	checkNonReloadableInt(CMKubeBurst, &old.KubeBurst, &new.KubeBurst)
	checkNonReloadableString(CMSvcOperatorPlugins, &old.OperatorPlugins, &new.OperatorPlugins)
	checkNonReloadableBool(CMSvcDisableGangScheduling, &old.DisableGangScheduling, &new.DisableGangScheduling)
	checkNonReloadableString(CMSvcPlaceholderImage, &old.PlaceHolderImage, &new.PlaceHolderImage)
}

const warningNonReloadable = "ignoring non-reloadable configuration change (restart required to update)"

func checkNonReloadableString(name string, old *string, new *string) {
	if *old != *new {
		log.Logger().Warn(warningNonReloadable, zap.String("config", name), zap.String("existing", *old), zap.String("new", *new))
		*new = *old
	}
}

func checkNonReloadableDuration(name string, old *time.Duration, new *time.Duration) {
	if *old != *new {
		log.Logger().Warn(warningNonReloadable, zap.String("config", name), zap.Duration("existing", *old), zap.Duration("new", *new))
		*new = *old
	}
}

func checkNonReloadableInt(name string, old *int, new *int) {
	if *old != *new {
		log.Logger().Warn(warningNonReloadable, zap.String("config", name), zap.Int("existing", *old), zap.Int("new", *new))
		*new = *old
	}
}

func checkNonReloadableBool(name string, old *bool, new *bool) {
	if *old != *new {
		log.Logger().Warn(warningNonReloadable, zap.String("config", name), zap.Bool("existing", *old), zap.Bool("new", *new))
		*new = *old
	}
}

func getCommandLineArgs() (string, []string) {
	args := make([]string, 0)
	if len(os.Args) > 1 {
		for _, arg := range os.Args[1:] {
			// workaround: ignore test args
			if strings.HasPrefix(arg, "-test") || strings.HasPrefix(arg, "--test") {
				continue
			}
			args = append(args, arg)
		}
	}
	return os.Args[0], args
}

func GetSchedulerConf() *SchedulerConf {
	once.Do(createConfigs)
	return confHolder.Load().(*SchedulerConf)
}

func SetSchedulerConf(conf *SchedulerConf) {
	// this is just to ensure that the original is in place first
	once.Do(createConfigs)
	confHolder.Store(conf)
}

func (conf *SchedulerConf) SetTestMode(testMode bool) {
	conf.Lock()
	defer conf.Unlock()
	conf.TestMode = testMode
}

func (conf *SchedulerConf) IsTestMode() bool {
	conf.RLock()
	defer conf.RUnlock()
	return conf.TestMode
}

func (conf *SchedulerConf) GetSchedulingInterval() time.Duration {
	conf.RLock()
	defer conf.RUnlock()
	return conf.Interval
}

func (conf *SchedulerConf) GetKubeConfigPath() string {
	conf.RLock()
	defer conf.RUnlock()
	return conf.KubeConfig
}

func (conf *SchedulerConf) IsOperatorPluginEnabled(name string) bool {
	conf.RLock()
	defer conf.RUnlock()
	if conf.OperatorPlugins == "" {
		return false
	}

	plugins := strings.Split(conf.OperatorPlugins, ",")
	for _, p := range plugins {
		if p == name {
			return true
		}
	}

	return false
}

func GetSchedulerNamespace() string {
	if value, ok := os.LookupEnv(EnvNamespace); ok {
		return value
	}
	return DefaultNamespace
}

func createConfigs() {
	confHolder.Store(CreateDefaultConfig())
}

func GetDefaultKubeConfigPath() string {
	conf, ok := os.LookupEnv(EnvKubeConfig)
	if ok {
		return conf
	}
	home, ok := os.LookupEnv(EnvHome)
	if !ok {
		home = ""
	}
	return fmt.Sprintf("%s/.kube/config", home)
}

// CreateDefaultConfig creates and returns a configuration representing all default values
func CreateDefaultConfig() *SchedulerConf {
	return &SchedulerConf{
		SchedulerName:          constants.SchedulerName,
		Namespace:              GetSchedulerNamespace(),
		ClusterID:              DefaultClusterID,
		ClusterVersion:         BuildVersion,
		PolicyGroup:            DefaultPolicyGroup,
		Interval:               DefaultSchedulingInterval,
		KubeConfig:             GetDefaultKubeConfigPath(),
		LoggingLevel:           DefaultLoggingLevel,
		VolumeBindTimeout:      DefaultVolumeBindTimeout,
		TestMode:               false,
		EventChannelCapacity:   DefaultEventChannelCapacity,
		DispatchTimeout:        DefaultDispatchTimeout,
		KubeQPS:                DefaultKubeQPS,
		KubeBurst:              DefaultKubeBurst,
		OperatorPlugins:        DefaultOperatorPlugins,
		EnableConfigHotRefresh: DefaultEnableConfigHotRefresh,
		DisableGangScheduling:  DefaultDisableGangScheduling,
		UserLabelKey:           constants.DefaultUserLabel,
		PlaceHolderImage:       constants.PlaceholderContainerImage,
	}
}

func parseConfig(config map[string]string, prev *SchedulerConf, seen map[string]string) (*SchedulerConf, []error) {
	conf := prev.Clone()

	if len(config) == 0 {
		// no changes
		return conf, nil
	}

	parser := newConfigParser(config, seen)

	// service
	parser.stringVar(&conf.ClusterID, CMSvcClusterID)
	parser.stringVar(&conf.PolicyGroup, CMSvcPolicyGroup)
	parser.durationVar(&conf.Interval, CMSvcSchedulingInterval)
	parser.durationVar(&conf.VolumeBindTimeout, CMSvcVolumeBindTimeout)
	parser.intVar(&conf.EventChannelCapacity, CMSvcEventChannelCapacity)
	parser.durationVar(&conf.DispatchTimeout, CMSvcDispatchTimeout)
	parser.stringVar(&conf.OperatorPlugins, CMSvcOperatorPlugins)
	parser.boolVar(&conf.DisableGangScheduling, CMSvcDisableGangScheduling)
	parser.boolVar(&conf.EnableConfigHotRefresh, CMSvcEnableConfigHotRefresh)
	parser.stringVar(&conf.PlaceHolderImage, CMSvcPlaceholderImage)

	// log
	parser.intVar(&conf.LoggingLevel, CMLogLevel)

	// kubernetes
	parser.intVar(&conf.KubeQPS, CMKubeQPS)
	parser.intVar(&conf.KubeBurst, CMKubeBurst)

	if len(parser.errors) > 0 {
		return nil, parser.errors
	}
	return conf, nil
}

type configParser struct {
	errors []error
	config map[string]string
	seen   map[string]string
}

func newConfigParser(config map[string]string, seen map[string]string) *configParser {
	return &configParser{
		errors: make([]error, 0),
		config: config,
		seen:   seen,
	}
}

func (cp *configParser) stringVar(p *string, name string) {
	if newValue, ok := cp.config[name]; ok {
		*p = newValue
		cp.seen[name] = name
	}
}

func (cp *configParser) intVar(p *int, name string) {
	if newValue, ok := cp.config[name]; ok {
		int64Value, err := strconv.ParseInt(newValue, 10, 32)
		intValue := int(int64Value)
		if err != nil {
			log.Logger().Error("Unable to parse configmap entry", zap.String("key", name), zap.String("value", newValue), zap.Error(err))
			cp.errors = append(cp.errors, err)
			return
		}
		*p = intValue
		cp.seen[name] = name
	}
}

func (cp *configParser) boolVar(p *bool, name string) {
	if newValue, ok := cp.config[name]; ok {
		boolValue, err := strconv.ParseBool(newValue)
		if err != nil {
			log.Logger().Error("Unable to parse configmap entry", zap.String("key", name), zap.String("value", newValue), zap.Error(err))
			cp.errors = append(cp.errors, err)
			return
		}
		*p = boolValue
		cp.seen[name] = name
	}
}

func (cp *configParser) durationVar(p *time.Duration, name string) {
	if newValue, ok := cp.config[name]; ok {
		durationValue, err := time.ParseDuration(newValue)
		if err != nil {
			log.Logger().Error("Unable to parse configmap entry", zap.String("key", name), zap.String("value", newValue), zap.Error(err))
			cp.errors = append(cp.errors, err)
			return
		}
		*p = durationValue
		cp.seen[name] = name
	}
}

type envParser struct {
	warnings []string
	seen     map[string]string
	lookup   envLookup
}

func newEnvParser(seen map[string]string, lookup envLookup) *envParser {
	return &envParser{
		warnings: make([]string, 0),
		seen:     seen,
		lookup:   lookup,
	}
}

func (ep *envParser) obsoleteStringVar(name string) {
	if newValue, ok := ep.lookup(name); ok {
		ep.warnings = append(ep.warnings, fmt.Sprintf("Obsolete environment variable '%s' found. Ignoring value: '%s'", name, newValue))
	}
}

func (ep *envParser) deprecatedStringVar(p *string, d *string, name string, cName string) {
	if newValue, ok := ep.lookup(name); ok {
		if _, prev := ep.seen[cName]; prev {
			if newValue != *d {
				ep.warnings = append(ep.warnings, fmt.Sprintf("Deprecated environment variable '%s' found with inconsistent value. Provided: '%s', used: '%s'", name, newValue, *d))
				return
			}
		} else {
			*p = newValue
			ep.seen[cName] = name
		}
	}
}

type cliParser struct {
	warnings []string
	seen     map[string]string
	visitors map[string]func(f *flag.Flag)
	flags    *flag.FlagSet
}

func newCliParser(flags *flag.FlagSet, seen map[string]string) *cliParser {
	return &cliParser{
		warnings: make([]string, 0),
		seen:     seen,
		visitors: make(map[string]func(f *flag.Flag)),
		flags:    flags,
	}
}

func (cp *cliParser) obsoleteStringVar(name string, value string, usage string) {
	var newValue string
	cp.flags.StringVar(&newValue, name, value, usage)
	cp.visitors[name] = func(f *flag.Flag) {
		cp.warnings = append(cp.warnings, fmt.Sprintf("Obsolete CLI option '%s' found. Ignoring value: '%s'", name, newValue))
	}
}

func (cp *cliParser) deprecatedStringVar(p *string, d *string, name string, cName string, value string, usage string) {
	var newValue string
	cp.flags.StringVar(&newValue, name, value, usage)
	cp.visitors[name] = func(f *flag.Flag) {
		if _, prev := cp.seen[cName]; prev {
			if newValue != *d {
				cp.warnings = append(cp.warnings, fmt.Sprintf("Deprecated CLI option '%s' found with inconsistent value. Provided: '%s', used: '%s'", name, newValue, *d))
			}
		} else {
			cp.seen[cName] = name
			*p = newValue
		}
	}
}

func (cp *cliParser) deprecatedIntVar(p *int, d *int, name string, cName string, value int, usage string) {
	var newValue int
	cp.flags.IntVar(&newValue, name, value, usage)
	cp.visitors[name] = func(f *flag.Flag) {
		if _, prev := cp.seen[cName]; prev {
			if newValue != *d {
				cp.warnings = append(cp.warnings, fmt.Sprintf("Deprecated CLI option '%s' found with inconsistent value. Provided: '%d', used: '%d'", name, newValue, *d))
			}
		} else {
			cp.seen[cName] = name
			*p = newValue
		}
	}
}

func (cp *cliParser) deprecatedBoolVar(p *bool, d *bool, name string, cName string, value bool, usage string) {
	var newValue bool
	cp.flags.BoolVar(&newValue, name, value, usage)
	cp.visitors[name] = func(f *flag.Flag) {
		if _, prev := cp.seen[cName]; prev {
			if newValue != *d {
				cp.warnings = append(cp.warnings, fmt.Sprintf("Deprecated CLI option '%s' found with inconsistent value. Provided: '%t', used: '%t'", name, newValue, *d))
			}
		} else {
			cp.seen[cName] = name
			*p = newValue
		}
	}
}

func (cp *cliParser) deprecatedDurationVar(p *time.Duration, d *time.Duration, name string, cName string, value time.Duration, usage string) {
	var newValue time.Duration
	cp.flags.DurationVar(&newValue, name, value, usage)
	cp.visitors[name] = func(f *flag.Flag) {
		if _, prev := cp.seen[cName]; prev {
			if newValue != *d {
				cp.warnings = append(cp.warnings, fmt.Sprintf("Deprecated CLI option '%s' found with inconsistent value. Provided: '%s', used: '%s'", name, newValue, *d))
			}
		} else {
			cp.seen[cName] = name
			*p = newValue
		}
	}
}

func (cp *cliParser) finish() {
	cp.flags.Visit(func(f *flag.Flag) {
		if visitor, ok := cp.visitors[f.Name]; ok {
			visitor(f)
		}
	})
}

// parseEnvConfig parses and returns a configuration populated from environment variables
func parseEnvConfig(prev *SchedulerConf, seen map[string]string) (*SchedulerConf, []string) {
	conf := prev.Clone()

	parser := newEnvParser(seen, envLookupFunc)

	parser.obsoleteStringVar(DeprecatedEnvUserLabelKey)
	parser.deprecatedStringVar(&conf.OperatorPlugins, &prev.OperatorPlugins, DeprecatedEnvOperatorPlugins, CMSvcOperatorPlugins)
	parser.deprecatedStringVar(&conf.PlaceHolderImage, &prev.PlaceHolderImage, DeprecatedEnvPlaceholderImage, CMSvcPlaceholderImage)

	if len(parser.warnings) > 0 {
		return conf, parser.warnings
	}
	return conf, nil
}

// parseCliConfig parses and returns a configuration populated from command-line arguments
func parseCliConfig(prev *SchedulerConf, seen map[string]string) (*SchedulerConf, []string) {
	conf := prev.Clone()

	cmd, args := cliArgsFunc()
	flags := flag.NewFlagSet(cmd, flag.ExitOnError)

	parser := newCliParser(flags, seen)

	// scheduler options
	parser.deprecatedStringVar(&conf.KubeConfig, &prev.KubeConfig,
		DeprecatedCliKubeConfig, EnvKubeConfig, GetDefaultKubeConfigPath(),
		fmt.Sprintf("DEPRECATED: absolute path to the kubeconfig file (use env: %s)", EnvKubeConfig))
	parser.deprecatedDurationVar(&conf.Interval, &prev.Interval,
		DeprecatedCliInterval, CMSvcSchedulingInterval, DefaultSchedulingInterval,
		fmt.Sprintf("DEPRECATED: scheduling interval in seconds (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcSchedulingInterval))
	parser.deprecatedStringVar(&conf.ClusterID, &prev.ClusterID,
		DeprecatedCliClusterID, CMSvcClusterID, DefaultClusterID,
		fmt.Sprintf("DEPRECATED: cluster id (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcClusterID))
	parser.obsoleteStringVar(DeprecatedCliClusterVersion, BuildVersion,
		"DEPRECATED: cluster version (ignored)")
	parser.deprecatedStringVar(&conf.PolicyGroup, &prev.PolicyGroup,
		DeprecatedCliPolicyGroup, CMSvcPolicyGroup, DefaultPolicyGroup,
		fmt.Sprintf("DEPRECATED: policy group (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcPolicyGroup))
	parser.deprecatedDurationVar(&conf.VolumeBindTimeout, &prev.VolumeBindTimeout,
		DeprecatedCliVolumeBindTimeout, CMSvcVolumeBindTimeout, DefaultVolumeBindTimeout,
		fmt.Sprintf("DEPRECATED: timeout in seconds when binding a volume (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcVolumeBindTimeout))
	parser.deprecatedIntVar(&conf.EventChannelCapacity, &prev.EventChannelCapacity,
		DeprecatedCliEventChannelCapacity, CMSvcEventChannelCapacity, DefaultEventChannelCapacity,
		fmt.Sprintf("DEPRECATED: event channel capacity of dispatcher (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcEventChannelCapacity))
	parser.deprecatedDurationVar(&conf.DispatchTimeout, &prev.DispatchTimeout,
		DeprecatedCliDispatchTimeout, CMSvcDispatchTimeout, DefaultDispatchTimeout,
		fmt.Sprintf("DEPRECATED: timeout in seconds when dispatching an event (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcDispatchTimeout))
	parser.deprecatedIntVar(&conf.KubeQPS, &prev.KubeQPS,
		DeprecatedCliKubeQPS, CMKubeQPS, DefaultKubeQPS,
		fmt.Sprintf("DEPRECATED: maximum QPS to Kubernetes master from this client (use configmap: %s/%s)",
			constants.ConfigMapName, CMKubeQPS))
	parser.deprecatedIntVar(&conf.KubeBurst, &prev.KubeBurst,
		DeprecatedCliKubeBurst, CMKubeBurst, DefaultKubeBurst,
		fmt.Sprintf("DEPRECATED: maximum burst for throttle to Kubernetes master from this client (use configmap: %s/%s)",
			constants.ConfigMapName, CMKubeBurst))
	parser.deprecatedStringVar(&conf.OperatorPlugins, &prev.OperatorPlugins,
		DeprecatedCliOperatorPlugins, CMSvcOperatorPlugins, DefaultOperatorPlugins,
		fmt.Sprintf("DEPRECATED: comma-separated list of operator plugin names [general,spark-k8s-operator,%s] (use configmap: %s/%s)",
			constants.AppManagerHandlerName, CMSvcOperatorPlugins, constants.ConfigMapName))
	parser.deprecatedStringVar(&conf.PlaceHolderImage, &prev.PlaceHolderImage,
		DeprecatedCliPlaceHolderImage, CMSvcPlaceholderImage, constants.PlaceholderContainerImage,
		fmt.Sprintf("DEPRECATED: Docker image of the placeholder pod (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcPlaceholderImage))

	// logging options
	parser.deprecatedIntVar(&conf.LoggingLevel, &prev.LoggingLevel,
		DeprecatedCliLogLevel, CMLogLevel, DefaultLoggingLevel,
		fmt.Sprintf("DEPRECATED: logging level, from DEBUG (-1) to FATAL (5) (use configmap: %s/%s)",
			constants.ConfigMapName, CMLogLevel))
	parser.obsoleteStringVar(DeprecatedCliLogEncoding, "", "DEPRECATED: log encoding (ignored)")
	parser.obsoleteStringVar(DeprecatedCliLogFile, "", "DEPRECATED: log file (ignored)")

	parser.deprecatedBoolVar(&conf.EnableConfigHotRefresh, &prev.EnableConfigHotRefresh,
		DeprecatedCliEnableConfigHotRefresh, CMSvcEnableConfigHotRefresh, DefaultEnableConfigHotRefresh,
		fmt.Sprintf("DEPRECATED: enable automatic configuration reloading (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcEnableConfigHotRefresh))
	parser.deprecatedBoolVar(&conf.DisableGangScheduling, &prev.DisableGangScheduling,
		DeprecatedCliDisableGangScheduling, CMSvcDisableGangScheduling, DefaultDisableGangScheduling,
		fmt.Sprintf("DEPRECATED: disable gang scheduling (use configmap: %s/%s)",
			constants.ConfigMapName, CMSvcDisableGangScheduling))
	parser.obsoleteStringVar(DeprecatedCliUserLabelKey, constants.DefaultUserLabel,
		"DEPRECATED: pod label key to be used to identify a user (ignored)")

	err := flags.Parse(args)
	if err != nil {
		// this will not happen as we parse with ExitOnError
		panic(err)
	}

	parser.finish()
	if len(parser.warnings) > 0 {
		return conf, parser.warnings
	}
	return conf, nil
}

func updateKubeLogger(conf *SchedulerConf) {
	// if log level is debug, enable klog and set its log level verbosity to 4 (represents debug level),
	// For details refer to the Logging Conventions of klog at
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md
	if zapcore.Level(conf.LoggingLevel).Enabled(zapcore.DebugLevel) {
		klog.InitFlags(nil)
		// cannot really handle the error here ignore it
		//nolint:errcheck
		_ = flag.Set("v", "4")
	}
}

func DumpConfiguration() {
	configs := GetSchedulerConf()
	c, err := json.MarshalIndent(configs, "", " ")

	logger := log.Logger()

	//nolint:errcheck
	defer logger.Sync()

	if err != nil {
		logger.Info("scheduler configuration, json conversion failed", zap.Any("configs", configs))
	} else {
		logger.Info("scheduler configuration, pretty print", zap.ByteString("configs", c))
	}
}

func FlattenConfigMaps(configMaps []*v1.ConfigMap) map[string]string {
	result := make(map[string]string)
	for _, configMap := range configMaps {
		if configMap != nil {
			for k, v := range configMap.Data {
				result[k] = v
			}
		}
	}
	return result
}
