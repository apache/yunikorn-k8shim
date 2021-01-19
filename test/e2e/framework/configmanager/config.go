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

package configmanager

import (
	"flag"
	"time"
)

// YuniKornTestConfigType holds all of the configurable elements of the testsuite
type YuniKornTestConfigType struct {
	JSONLogs    bool
	LogLevel    string
	Timeout     time.Duration
	KubeConfig  string
	LogFile     string
	YkNamespace string
	YkHost      string
	YkPort      string
	YkScheme    string
	LogDir      string
}

// YuniKornTestConfig holds the global configuration of commandline flags
// in the ginkgo-based testing environment.
var YuniKornTestConfig = YuniKornTestConfigType{}

// ParseFlags parses commandline flags relevant to testing.
func (c *YuniKornTestConfigType) ParseFlags() {
	flag.BoolVar(&c.JSONLogs, "json-logs-enabled", false,
		"Enables logging in json format")
	flag.StringVar(&c.LogLevel, "log-level", "debug",
		"log level(debug|info|error|critical")
	flag.StringVar(&c.KubeConfig, "kube-config", "~/.kube/config",
		"Kubeconfig to be used for tests")
	flag.StringVar(&c.LogFile, "log-file", "test-output.log",
		"Log location")
	flag.DurationVar(&c.Timeout, "timeout", 24*time.Hour,
		"Specifies timeout for test run")
	flag.StringVar(&c.YkNamespace, "yk-namespace", "yunikorn",
		"K8s Namespace in which YuniKorn service deployed")
	flag.StringVar(&c.YkHost, "yk-host", DefaultYuniKornHost,
		"Hostname/IP of YuniKorn service")
	flag.StringVar(&c.YkPort, "yk-port", DefaultYuniKornPort,
		"External Port of YuniKorn service")
	flag.StringVar(&c.YkScheme, "yk-scheme", DefaultYuniKornScheme,
		"Scheme of YuniKron Service")
	flag.StringVar(&c.LogDir, "log-dir", "/tmp/test-results/target/failsafe-reports",
		"Log location where the logs are stored")
}
