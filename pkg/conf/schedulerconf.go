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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type SchedulerConfFactory = func() *SchedulerConf

const (
	// env vars
	EnvHome       = "HOME"
	EnvKubeConfig = "KUBECONFIG"
	EnvNamespace  = "NAMESPACE"

	// prefixes
	PrefixService             = "service."
	PrefixLog                 = "log."
	PrefixKubernetes          = "kubernetes."
	PrefixAdmissionController = "admissionController."

	// service
	CMSvcClusterID                    = PrefixService + "clusterId"
	CMSvcPolicyGroup                  = PrefixService + "policyGroup"
	CMSvcSchedulingInterval           = PrefixService + "schedulingInterval"
	CMSvcVolumeBindTimeout            = PrefixService + "volumeBindTimeout"
	CMSvcEventChannelCapacity         = PrefixService + "eventChannelCapacity"
	CMSvcDispatchTimeout              = PrefixService + "dispatchTimeout"
	CMSvcDisableGangScheduling        = PrefixService + "disableGangScheduling"
	CMSvcEnableConfigHotRefresh       = PrefixService + "enableConfigHotRefresh"
	CMSvcPlaceholderImage             = PrefixService + "placeholderImage"
	CMSvcNodeInstanceTypeNodeLabelKey = PrefixService + "nodeInstanceTypeNodeLabelKey"

	// kubernetes
	CMKubeQPS   = PrefixKubernetes + "qps"
	CMKubeBurst = PrefixKubernetes + "burst"

	// admissioncontroller
	PrefixAMFiltering               = PrefixAdmissionController + "filtering."
	AMFilteringGenerateUniqueAppIds = PrefixAMFiltering + "generateUniqueAppId"

	// defaults
	DefaultNamespace                       = "default"
	DefaultClusterID                       = "mycluster"
	DefaultPolicyGroup                     = "queues"
	DefaultSchedulingInterval              = time.Second
	DefaultVolumeBindTimeout               = 10 * time.Minute
	DefaultEventChannelCapacity            = 1024 * 1024
	DefaultDispatchTimeout                 = 300 * time.Second
	DefaultOperatorPlugins                 = "general"
	DefaultDisableGangScheduling           = false
	DefaultEnableConfigHotRefresh          = true
	DefaultKubeQPS                         = 1000
	DefaultKubeBurst                       = 1000
	DefaultAMFilteringGenerateUniqueAppIds = false
)

var (
	buildVersion    string
	buildDate       string
	isPluginVersion string
	goVersion       string
	arch            string
	coreSHA         string
	siSHA           string
	shimSHA         string
)

var once sync.Once
var confHolder atomic.Value

var kubeLoggerOnce sync.Once

type SchedulerConf struct {
	SchedulerName            string        `json:"schedulerName"`
	ClusterID                string        `json:"clusterId"`
	ClusterVersion           string        `json:"clusterVersion"`
	PolicyGroup              string        `json:"policyGroup"`
	Interval                 time.Duration `json:"schedulingIntervalSecond"`
	KubeConfig               string        `json:"absoluteKubeConfigFilePath"`
	VolumeBindTimeout        time.Duration `json:"volumeBindTimeout"`
	EventChannelCapacity     int           `json:"eventChannelCapacity"`
	DispatchTimeout          time.Duration `json:"dispatchTimeout"`
	KubeQPS                  int           `json:"kubeQPS"`
	KubeBurst                int           `json:"kubeBurst"`
	EnableConfigHotRefresh   bool          `json:"enableConfigHotRefresh"`
	DisableGangScheduling    bool          `json:"disableGangScheduling"`
	UserLabelKey             string        `json:"userLabelKey"`
	PlaceHolderImage         string        `json:"placeHolderImage"`
	InstanceTypeNodeLabelKey string        `json:"instanceTypeNodeLabelKey"`
	Namespace                string        `json:"namespace"`
	GenerateUniqueAppIds     bool          `json:"generateUniqueAppIds"`

	locking.RWMutex
}

func (conf *SchedulerConf) Clone() *SchedulerConf {
	conf.RLock()
	defer conf.RUnlock()

	return &SchedulerConf{
		SchedulerName:            conf.SchedulerName,
		ClusterID:                conf.ClusterID,
		ClusterVersion:           conf.ClusterVersion,
		PolicyGroup:              conf.PolicyGroup,
		Interval:                 conf.Interval,
		KubeConfig:               conf.KubeConfig,
		VolumeBindTimeout:        conf.VolumeBindTimeout,
		EventChannelCapacity:     conf.EventChannelCapacity,
		DispatchTimeout:          conf.DispatchTimeout,
		KubeQPS:                  conf.KubeQPS,
		KubeBurst:                conf.KubeBurst,
		EnableConfigHotRefresh:   conf.EnableConfigHotRefresh,
		DisableGangScheduling:    conf.DisableGangScheduling,
		UserLabelKey:             conf.UserLabelKey,
		PlaceHolderImage:         conf.PlaceHolderImage,
		InstanceTypeNodeLabelKey: conf.InstanceTypeNodeLabelKey,
		Namespace:                conf.Namespace,
		GenerateUniqueAppIds:     conf.GenerateUniqueAppIds,
	}
}

func UpdateConfigMaps(configMaps []*v1.ConfigMap, initial bool) error {
	log.Log(log.ShimConfig).Info("reloading configuration")

	// start with defaults
	prev := CreateDefaultConfig()

	// flatten configmap entries to single map
	config := FlattenConfigMaps(configMaps)

	// parse values from configmaps
	newConf, cmErrors := parseConfig(config, prev)
	if cmErrors != nil {
		for _, err := range cmErrors {
			log.Log(log.ShimConfig).Error("failed to parse configmap entry", zap.Error(err))
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
	_ = GetSchedulerConf()

	// update logger configuration
	log.UpdateLoggingConfig(config)

	// update Kubernetes logger configuration
	updateKubeLogger()

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
	checkNonReloadableBool(CMSvcDisableGangScheduling, &old.DisableGangScheduling, &new.DisableGangScheduling)
	checkNonReloadableString(CMSvcPlaceholderImage, &old.PlaceHolderImage, &new.PlaceHolderImage)
	checkNonReloadableString(CMSvcNodeInstanceTypeNodeLabelKey, &old.InstanceTypeNodeLabelKey, &new.InstanceTypeNodeLabelKey)
	checkNonReloadableBool(AMFilteringGenerateUniqueAppIds, &old.GenerateUniqueAppIds, &new.GenerateUniqueAppIds)
}

const warningNonReloadable = "ignoring non-reloadable configuration change (restart required to update)"

func checkNonReloadableString(name string, old *string, new *string) {
	if *old != *new {
		log.Log(log.ShimConfig).Warn(warningNonReloadable, zap.String("config", name), zap.String("existing", *old), zap.String("new", *new))
		*new = *old
	}
}

func checkNonReloadableDuration(name string, old *time.Duration, new *time.Duration) {
	if *old != *new {
		log.Log(log.ShimConfig).Warn(warningNonReloadable, zap.String("config", name), zap.Duration("existing", *old), zap.Duration("new", *new))
		*new = *old
	}
}

func checkNonReloadableInt(name string, old *int, new *int) {
	if *old != *new {
		log.Log(log.ShimConfig).Warn(warningNonReloadable, zap.String("config", name), zap.Int("existing", *old), zap.Int("new", *new))
		*new = *old
	}
}

func checkNonReloadableBool(name string, old *bool, new *bool) {
	if *old != *new {
		log.Log(log.ShimConfig).Warn(warningNonReloadable, zap.String("config", name), zap.Bool("existing", *old), zap.Bool("new", *new))
		*new = *old
	}
}

func GetSchedulerConf() *SchedulerConf {
	once.Do(createConfigs)
	return confHolder.Load().(*SchedulerConf) //nolint:errcheck
}

func SetSchedulerConf(conf *SchedulerConf) {
	// this is just to ensure that the original is in place first
	once.Do(createConfigs)
	confHolder.Store(conf)
}

func (conf *SchedulerConf) IsConfigReloadable() bool {
	conf.RLock()
	defer conf.RUnlock()
	return conf.EnableConfigHotRefresh
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
		SchedulerName:            constants.SchedulerName,
		Namespace:                GetSchedulerNamespace(),
		ClusterID:                DefaultClusterID,
		ClusterVersion:           buildVersion,
		PolicyGroup:              DefaultPolicyGroup,
		Interval:                 DefaultSchedulingInterval,
		KubeConfig:               GetDefaultKubeConfigPath(),
		VolumeBindTimeout:        DefaultVolumeBindTimeout,
		EventChannelCapacity:     DefaultEventChannelCapacity,
		DispatchTimeout:          DefaultDispatchTimeout,
		KubeQPS:                  DefaultKubeQPS,
		KubeBurst:                DefaultKubeBurst,
		EnableConfigHotRefresh:   DefaultEnableConfigHotRefresh,
		DisableGangScheduling:    DefaultDisableGangScheduling,
		UserLabelKey:             constants.DefaultUserLabel,
		PlaceHolderImage:         constants.PlaceholderContainerImage,
		InstanceTypeNodeLabelKey: constants.DefaultNodeInstanceTypeNodeLabelKey,
		GenerateUniqueAppIds:     DefaultAMFilteringGenerateUniqueAppIds,
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
	parser.boolVar(&conf.DisableGangScheduling, CMSvcDisableGangScheduling)
	parser.boolVar(&conf.EnableConfigHotRefresh, CMSvcEnableConfigHotRefresh)
	parser.stringVar(&conf.PlaceHolderImage, CMSvcPlaceholderImage)
	parser.stringVar(&conf.InstanceTypeNodeLabelKey, CMSvcNodeInstanceTypeNodeLabelKey)

	// kubernetes
	parser.intVar(&conf.KubeQPS, CMKubeQPS)
	parser.intVar(&conf.KubeBurst, CMKubeBurst)

	// admission controller
	parser.boolVar(&conf.GenerateUniqueAppIds, AMFilteringGenerateUniqueAppIds)

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
			log.Log(log.ShimConfig).Error("Unable to parse configmap entry", zap.String("key", name), zap.String("value", newValue), zap.Error(err))
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
			log.Log(log.ShimConfig).Error("Unable to parse configmap entry", zap.String("key", name), zap.String("value", newValue), zap.Error(err))
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
			log.Log(log.ShimConfig).Error("Unable to parse configmap entry", zap.String("key", name), zap.String("value", newValue), zap.Error(err))
			cp.errors = append(cp.errors, err)
			return
		}
		*p = durationValue
	}
}

func updateKubeLogger() {
	// if log level is debug, enable klog and set its log level verbosity to 4 (represents debug level),
	// For details refer to the Logging Conventions of klog at
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md

	// danger, this can only be called once!
	kubeLoggerOnce.Do(func() {
		if log.Log(log.Kubernetes).Core().Enabled(zapcore.DebugLevel) {
			klog.InitFlags(nil)
			// cannot really handle the error here ignore it
			//nolint:errcheck
			_ = flag.Set("v", "4")
		}
	})
}

func DumpConfiguration() {
	configs := GetSchedulerConf()
	c, err := json.MarshalIndent(configs, "", " ")

	logger := log.Log(log.ShimConfig)

	//nolint:errcheck
	defer logger.Sync()

	if err != nil {
		logger.Info("scheduler configuration, json conversion failed", zap.Any("configs", configs))
	} else {
		logger.Info("scheduler configuration, pretty print", zap.ByteString("configs", c))
	}
}

func Decompress(key string, value []byte) (string, string) {
	var uncompressedData string
	splitKey := strings.Split(key, ".")
	compressionAlgo := splitKey[len(splitKey)-1]
	if strings.EqualFold(compressionAlgo, constants.GzipSuffix) {
		gzReader, err := gzip.NewReader(bytes.NewReader(value))
		if err != nil {
			log.Log(log.ShimConfig).Error("failed to decompress decoded schedulerConfig entry", zap.Error(err))
			return "", ""
		}
		defer func() {
			if err = gzReader.Close(); err != nil {
				log.Log(log.ShimConfig).Debug("gzip Reader could not be closed ", zap.Error(err))
			}
		}()
		decompressedBytes, err := io.ReadAll(gzReader)
		if err != nil {
			log.Log(log.ShimConfig).Error("failed to decompress decoded schedulerConfig entry", zap.Error(err))
			return "", ""
		}
		uncompressedData = string(decompressedBytes)
	}
	strippedKey, _ := strings.CutSuffix(key, "."+compressionAlgo)
	return strippedKey, uncompressedData
}

func FlattenConfigMaps(configMaps []*v1.ConfigMap) map[string]string {
	result := make(map[string]string)
	for _, configMap := range configMaps {
		if configMap != nil {
			for k, v := range configMap.Data {
				result[k] = v
			}
			for k, v := range configMap.BinaryData {
				strippedKey, uncompressedData := Decompress(k, v)
				result[strippedKey] = uncompressedData
			}
		}
	}
	return result
}

func GetBuildInfoMap() map[string]string {
	return map[string]string{
		"buildVersion":    buildVersion,
		"buildDate":       buildDate,
		"isPluginVersion": isPluginVersion,
		"goVersion":       goVersion,
		"arch":            arch,
		"coreSHA":         coreSHA,
		"siSHA":           siSHA,
		"shimSHA":         shimSHA,
	}
}

func GetBuildInfoString() string {
	return fmt.Sprintf(
		"Build info: version=%s date=%s isPluginVersion=%s goVersion=%s arch=%s coreSHA=%s siSHA=%s shimSHA=%s",
		buildVersion, buildDate, isPluginVersion, goVersion, arch, coreSHA, siSHA, shimSHA,
	)
}
