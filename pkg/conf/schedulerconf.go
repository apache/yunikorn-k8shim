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

	// prefixes
	PrefixService    = "service."
	PrefixLog        = "log."
	PrefixKubernetes = "kubernetes."

	// service
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

	// log
	CMLogLevel = PrefixLog + "level"

	// kubernetes
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

	// flatten configmap entries to single map
	config := FlattenConfigMaps(configMaps)

	// parse values from configmaps
	newConf, cmErrors := parseConfig(config, prev)
	if cmErrors != nil {
		for _, err := range cmErrors {
			log.Logger().Error("failed to parse configmap entry", zap.Error(err))
		}
		return errors.New("failed to load configmap")
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

func parseConfig(config map[string]string, prev *SchedulerConf) (*SchedulerConf, []error) {
	conf := prev.Clone()

	if len(config) == 0 {
		// no changes
		return conf, nil
	}

	parser := newConfigParser(config)

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
}

func newConfigParser(config map[string]string) *configParser {
	return &configParser{
		errors: make([]error, 0),
		config: config,
	}
}

func (cp *configParser) stringVar(p *string, name string) {
	if newValue, ok := cp.config[name]; ok {
		*p = newValue
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
	}
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
