/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
	"time"
)

// default configuration values, these can be override by CLI options
const (
	DefaultClusterId = "my-kube-cluster"
	DefaultClusterVersion = "0.1"
	DefaultSchedulerName = "yunikorn"
	DefaultPolicyGroup = "queues"
	DefaultLoggingLevel = 0
	DefaultLogEncoding = "console"
	DefaultVolumeBindTimeout = 10 * time.Second
	DefaultSchedulingInterval = time.Second
)

var configuration *SchedulerConf

type SchedulerConf struct {
	ClusterId         string        `json:"clusterId"`
	ClusterVersion    string        `json:"clusterVersion"`
	SchedulerName     string        `json:"schedulerName"`
	PolicyGroup       string        `json:"policyGroup"`
	Interval          time.Duration `json:"schedulingIntervalSecond"`
	KubeConfig        string        `json:"absoluteKubeConfigFilePath"`
	LoggingLevel      int           `json:"loggingLevel"`
	LogEncoding       string        `json:"logEncoding"`
	LogFile           string        `json:"logFilePath"`
	VolumeBindTimeout time.Duration `json:"volumeBindTimeout"`
	TestMode          bool          `json:"testMode"`
}

func GetSchedulerConf() *SchedulerConf {
	return configuration
}

// unit tests may need to override configuration
func Set(conf *SchedulerConf) {
	configuration = conf
}

func (conf *SchedulerConf) GetSchedulingInterval() time.Duration {
	return conf.Interval
}

func (conf *SchedulerConf) GetKubeConfigPath() string {
	return conf.KubeConfig
}

func init() {
	// scheduler options
	kubeConfig := flag.String("kubeConfig", "",
		"absolute path to the kubeconfig file")
	schedulingInterval := flag.Duration("interval", DefaultSchedulingInterval,
		"scheduling interval in seconds")
	clusterId := flag.String("clusterId", DefaultClusterId,
		"cluster id")
	clusterVersion := flag.String("clusterVersion", DefaultClusterVersion,
		"cluster version")
	schedulerName := flag.String("name", DefaultSchedulerName,
		"name of the scheduler")
	policyGroup := flag.String("policyGroup", DefaultPolicyGroup,
		"policy group")
	volumeBindTimeout := flag.Duration("volumeBindTimeout", DefaultVolumeBindTimeout,
		"timeout in seconds when binding a volume")

	// logging options
	logLevel := flag.Int("logLevel", DefaultLoggingLevel,
		"logging level, available range [-1, 5], from DEBUG to FATAL.")
	encode := flag.String("logEncoding", DefaultLogEncoding,
		"log encoding, json or console.")
	logFile := flag.String("logFile", "",
		"absolute log file path")

	flag.Parse()

	configuration = &SchedulerConf{
		ClusterId:         *clusterId,
		ClusterVersion:    *clusterVersion,
		PolicyGroup:       *policyGroup,
		SchedulerName:     *schedulerName,
		Interval:          *schedulingInterval,
		KubeConfig:        *kubeConfig,
		LoggingLevel:      *logLevel,
		LogEncoding:       *encode,
		LogFile:           *logFile,
		VolumeBindTimeout: *volumeBindTimeout,
	}
}
