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
	"flag"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
	"k8s.io/klog"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
)

type SchedulerConfFactory = func() *SchedulerConf

// default configuration values, these can be override by CLI options
const (
	DefaultClusterID            = "my-kube-cluster"
	DefaultClusterVersion       = "0.1"
	DefaultPolicyGroup          = "queues"
	DefaultLoggingLevel         = 0
	DefaultLogEncoding          = "console"
	DefaultVolumeBindTimeout    = 10 * time.Second
	DefaultSchedulingInterval   = time.Second
	DefaultEventChannelCapacity = 1024 * 1024
	DefaultDispatchTimeout      = 300 * time.Second
	DefaultKubeQPS              = 1000
	DefaultKubeBurst            = 1000
)

var once sync.Once
var configuration *SchedulerConf
var factory = initConfigs

var (
	BuildVersion    string
	BuildDate       string
	IsPluginVersion bool
)

type SchedulerConf struct {
	SchedulerName          string        `json:"schedulerName"`
	ClusterID              string        `json:"clusterId"`
	ClusterVersion         string        `json:"clusterVersion"`
	PolicyGroup            string        `json:"policyGroup"`
	Interval               time.Duration `json:"schedulingIntervalSecond"`
	KubeConfig             string        `json:"absoluteKubeConfigFilePath"`
	LoggingLevel           int           `json:"loggingLevel"`
	LogEncoding            string        `json:"logEncoding"`
	LogFile                string        `json:"logFilePath"`
	VolumeBindTimeout      time.Duration `json:"volumeBindTimeout"`
	TestMode               bool          `json:"testMode"`
	EventChannelCapacity   int           `json:"eventChannelCapacity"`
	DispatchTimeout        time.Duration `json:"dispatchTimeout"`
	KubeQPS                int           `json:"kubeQPS"`
	KubeBurst              int           `json:"kubeBurst"`
	Predicates             string        `json:"predicates"`
	OperatorPlugins        string        `json:"operatorPlugins"`
	EnableConfigHotRefresh bool          `json:"enableConfigHotRefresh"`
	DisableGangScheduling  bool          `json:"disableGangScheduling"`
	UserLabelKey           string        `json:"userLabelKey"`
	PlaceHolderImage       string        `json:"placeHolderImage"`
	sync.RWMutex
}

func SetSchedulerConfFactory(confFactory SchedulerConfFactory) {
	factory = confFactory
}

func GetSchedulerConf() *SchedulerConf {
	once.Do(createConfigs)
	return configuration
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

func createConfigs() {
	configuration = factory()
}

func initConfigs() *SchedulerConf {
	// scheduler options
	kubeConfig := flag.String("kubeConfig", "",
		"absolute path to the kubeconfig file")
	schedulingInterval := flag.Duration("interval", DefaultSchedulingInterval,
		"scheduling interval in seconds")
	clusterID := flag.String("clusterId", DefaultClusterID,
		"cluster id")
	clusterVersion := flag.String("clusterVersion", DefaultClusterVersion,
		"cluster version")
	policyGroup := flag.String("policyGroup", DefaultPolicyGroup,
		"policy group")
	volumeBindTimeout := flag.Duration("volumeBindTimeout", DefaultVolumeBindTimeout,
		"timeout in seconds when binding a volume")
	eventChannelCapacity := flag.Int("eventChannelCapacity", DefaultEventChannelCapacity,
		"event channel capacity of dispatcher")
	dispatchTimeout := flag.Duration("dispatchTimeout", DefaultDispatchTimeout,
		"timeout in seconds when dispatching an event")
	kubeQPS := flag.Int("kubeQPS", DefaultKubeQPS,
		"the maximum QPS to kubernetes master from this client")
	kubeBurst := flag.Int("kubeBurst", DefaultKubeBurst,
		"the maximum burst for throttle to kubernetes master from this client")
	operatorPluginList := flag.String("operatorPlugins", "general,"+constants.AppManagerHandlerName,
		"comma-separated list of operator plugin names, currently, only \"spark-k8s-operator\""+
			"and"+constants.AppManagerHandlerName+"is supported.")
	placeHolderImage := flag.String("placeHolderImage", constants.PlaceholderContainerImage,
		"docker image of the placeholder pod")

	// logging options
	logLevel := flag.Int("logLevel", DefaultLoggingLevel,
		"logging level, available range [-1, 5], from DEBUG to FATAL.")
	encode := flag.String("logEncoding", DefaultLogEncoding,
		"log encoding, json or console.")
	logFile := flag.String("logFile", "",
		"absolute log file path")
	enableConfigHotRefresh := flag.Bool("enableConfigHotRefresh", false, "Flag for enabling "+
		"configuration hot-refresh. If this value is set to true, the configuration updates in the configmap will be "+
		"automatically reloaded without restarting the scheduler.")
	disableGangScheduling := flag.Bool("disableGangScheduling", false, "Flag for disabling "+
		"gang scheduling. If this value is set to true, task-group metadata will be ignored by the scheduler.")
	userLabelKey := flag.String("userLabelKey", constants.DefaultUserLabel,
		"provide pod label key to be used to identify an user")

	flag.Parse()

	// if log level is debug, enable klog and set its log level verbosity to 4 (represents debug level),
	// For details refer to the Logging Conventions of klog at
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md
	if zapcore.Level(*logLevel).Enabled(zapcore.DebugLevel) {
		klog.InitFlags(nil)
		// cannot really handle the error here ignore it
		//nolint:errcheck
		_ = flag.Set("v", "4")
	}

	return &SchedulerConf{
		SchedulerName:          constants.SchedulerName,
		ClusterID:              *clusterID,
		ClusterVersion:         *clusterVersion,
		PolicyGroup:            *policyGroup,
		Interval:               *schedulingInterval,
		KubeConfig:             *kubeConfig,
		LoggingLevel:           *logLevel,
		LogEncoding:            *encode,
		LogFile:                *logFile,
		VolumeBindTimeout:      *volumeBindTimeout,
		EventChannelCapacity:   *eventChannelCapacity,
		DispatchTimeout:        *dispatchTimeout,
		KubeQPS:                *kubeQPS,
		KubeBurst:              *kubeBurst,
		OperatorPlugins:        *operatorPluginList,
		EnableConfigHotRefresh: *enableConfigHotRefresh,
		DisableGangScheduling:  *disableGangScheduling,
		UserLabelKey:           *userLabelKey,
		PlaceHolderImage:       *placeHolderImage,
	}
}
