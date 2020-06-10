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
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
)

// default configuration values, these can be override by CLI options
const (
	DefaultClusterID            = "my-kube-cluster"
	DefaultClusterVersion       = "0.1"
	DefaultSchedulerName        = "yunikorn"
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

var configuration *SchedulerConf

type SchedulerConf struct {
	ClusterID            string        `json:"clusterId"`
	ClusterVersion       string        `json:"clusterVersion"`
	SchedulerName        string        `json:"schedulerName"`
	PolicyGroup          string        `json:"policyGroup"`
	Interval             time.Duration `json:"schedulingIntervalSecond"`
	KubeConfig           string        `json:"absoluteKubeConfigFilePath"`
	LoggingLevel         int           `json:"loggingLevel"`
	LogEncoding          string        `json:"logEncoding"`
	LogFile              string        `json:"logFilePath"`
	VolumeBindTimeout    time.Duration `json:"volumeBindTimeout"`
	TestMode             bool          `json:"testMode"`
	EventChannelCapacity int           `json:"eventChannelCapacity"`
	DispatchTimeout      time.Duration `json:"dispatchTimeout"`
	KubeQPS              int           `json:"kubeQPS"`
	KubeBurst            int           `json:"kubeBurst"`
	Predicates           string        `json:"predicates"`
	OperatorPlugins      string        `json:"operatorPlugins"`
	sync.RWMutex
}

func GetSchedulerConf() *SchedulerConf {
	return configuration
}

func (conf *SchedulerConf) SetTestMode(testMode bool) {
	conf.Lock()
	defer conf.Unlock()
	conf.TestMode = testMode
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

func init() {
	// scheduler options
	kubeConfig := flag.String("kubeConfig", "",
		"absolute path to the kubeconfig file")
	schedulingInterval := flag.Duration("interval", DefaultSchedulingInterval,
		"scheduling interval in seconds")
	clusterID := flag.String("clusterId", DefaultClusterID,
		"cluster id")
	clusterVersion := flag.String("clusterVersion", DefaultClusterVersion,
		"cluster version")
	schedulerName := flag.String("name", DefaultSchedulerName,
		"name of the scheduler")
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
	predicateList := flag.String("predicates", "",
		fmt.Sprintf("comma-separated list of predicates, valid predicates are: %s, "+
			"the program will exit if any invalid predicates exist.", predicates.Ordering()))
	operatorPluginList := flag.String("operatorPlugins", "general",
		"common-separated list of operator plugin names, currently, only \"spark-operator-service\" is supported.")

	// logging options
	logLevel := flag.Int("logLevel", DefaultLoggingLevel,
		"logging level, available range [-1, 5], from DEBUG to FATAL.")
	encode := flag.String("logEncoding", DefaultLogEncoding,
		"log encoding, json or console.")
	logFile := flag.String("logFile", "",
		"absolute log file path")

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

	configuration = &SchedulerConf{
		ClusterID:            *clusterID,
		ClusterVersion:       *clusterVersion,
		PolicyGroup:          *policyGroup,
		SchedulerName:        *schedulerName,
		Interval:             *schedulingInterval,
		KubeConfig:           *kubeConfig,
		LoggingLevel:         *logLevel,
		LogEncoding:          *encode,
		LogFile:              *logFile,
		VolumeBindTimeout:    *volumeBindTimeout,
		EventChannelCapacity: *eventChannelCapacity,
		DispatchTimeout:      *dispatchTimeout,
		KubeQPS:              *kubeQPS,
		KubeBurst:            *kubeBurst,
		Predicates:           *predicateList,
		OperatorPlugins:      *operatorPluginList,
	}
}
