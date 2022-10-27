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
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
	"k8s.io/klog"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

type SchedulerConfFactory = func() *SchedulerConf

// env vars
const (
	NamespaceEnv     = "NAMESPACE"
	DefaultNamespace = "default"
)

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
	Namespace              string        `json:"namespace"`
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

func GetSchedulerNamespace() string {
	if value, ok := os.LookupEnv(NamespaceEnv); ok {
		return value
	}
	return DefaultNamespace
}

func createConfigs() {
	configuration = factory()
}

func initConfigs() *SchedulerConf {
	conf := &SchedulerConf{
		SchedulerName: constants.SchedulerName,
		Namespace:     GetSchedulerNamespace(),
	}

	// scheduler options
	flag.StringVar(&conf.KubeConfig, "kubeConfig", "",
		"absolute path to the kubeconfig file")
	flag.DurationVar(&conf.Interval, "interval", DefaultSchedulingInterval,
		"scheduling interval in seconds")
	flag.StringVar(&conf.ClusterID, "clusterId", DefaultClusterID,
		"cluster id")
	flag.StringVar(&conf.ClusterVersion, "clusterVersion", DefaultClusterVersion,
		"cluster version")
	flag.StringVar(&conf.PolicyGroup, "policyGroup", DefaultPolicyGroup,
		"policy group")
	flag.DurationVar(&conf.VolumeBindTimeout, "volumeBindTimeout", DefaultVolumeBindTimeout,
		"timeout in seconds when binding a volume")
	flag.IntVar(&conf.EventChannelCapacity, "eventChannelCapacity", DefaultEventChannelCapacity,
		"event channel capacity of dispatcher")
	flag.DurationVar(&conf.DispatchTimeout, "dispatchTimeout", DefaultDispatchTimeout,
		"timeout in seconds when dispatching an event")
	flag.IntVar(&conf.KubeQPS, "kubeQPS", DefaultKubeQPS,
		"the maximum QPS to kubernetes master from this client")
	flag.IntVar(&conf.KubeBurst, "kubeBurst", DefaultKubeBurst,
		"the maximum burst for throttle to kubernetes master from this client")
	flag.StringVar(&conf.OperatorPlugins, "operatorPlugins", "general",
		"comma-separated list of operator plugin names, currently, only \"spark-k8s-operator\""+
			"and"+constants.AppManagerHandlerName+"is supported.")
	flag.StringVar(&conf.PlaceHolderImage, "placeHolderImage", constants.PlaceholderContainerImage,
		"docker image of the placeholder pod")

	// logging options
	flag.IntVar(&conf.LoggingLevel, "logLevel", DefaultLoggingLevel,
		"logging level, available range [-1, 5], from DEBUG to FATAL.")
	flag.StringVar(&conf.LogEncoding, "logEncoding", DefaultLogEncoding,
		"log encoding, json or console.")
	flag.StringVar(&conf.LogFile, "logFile", "",
		"absolute log file path")
	flag.BoolVar(&conf.EnableConfigHotRefresh, "enableConfigHotRefresh", false, "Flag for enabling "+
		"configuration hot-refresh. If this value is set to true, the configuration updates in the configmap will be "+
		"automatically reloaded without restarting the scheduler.")
	flag.BoolVar(&conf.DisableGangScheduling, "disableGangScheduling", false, "Flag for disabling "+
		"gang scheduling. If this value is set to true, task-group metadata will be ignored by the scheduler.")
	flag.StringVar(&conf.UserLabelKey, "userLabelKey", constants.DefaultUserLabel,
		"provide pod label key to be used to identify an user")

	flag.Parse()

	// if log level is debug, enable klog and set its log level verbosity to 4 (represents debug level),
	// For details refer to the Logging Conventions of klog at
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md
	if zapcore.Level(conf.LoggingLevel).Enabled(zapcore.DebugLevel) {
		klog.InitFlags(nil)
		// cannot really handle the error here ignore it
		//nolint:errcheck
		_ = flag.Set("v", "4")
	}

	return conf
}
