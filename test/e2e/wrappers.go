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

package e2e

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var k = k8s.KubeCtl{}

func UpdateConfigMapWrapper(oldConfigMap *v1.ConfigMap, schedPolicy string, annotation string) {
	By("Enabling new scheduling config")

	// Save old configMap
	Ω(k.SetClient()).To(BeNil())
	var c, err = k.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.GetConfigMapName())
	Ω(err).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())
	c.DeepCopyInto(oldConfigMap)
	Ω(c).Should(BeEquivalentTo(oldConfigMap))

	// Create fair configMap with timestamp
	sc := common.CreateBasicConfigMap()
	if schedPolicy != "" {
		err = common.SetSchedulingPolicy(sc, "default", "root", schedPolicy)
		Ω(err).NotTo(HaveOccurred())
	}
	ts, tsErr := common.SetQueueTimestamp(sc, "default", "root")
	Ω(tsErr).NotTo(HaveOccurred())
	configStr, yamlErr := common.ToYAML(sc)
	Ω(yamlErr).NotTo(HaveOccurred())
	c.Data[configmanager.DefaultPolicyGroup] = configStr
	var d, err3 = k.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(d).NotTo(BeNil())

	err = yunikorn.WaitForQueueTS("root", ts, 2*time.Minute)
	Ω(err).NotTo(HaveOccurred())
}

func RestoreConfigMapWrapper(oldConfigMap *v1.ConfigMap, annotation string) {
	Ω(k.SetClient()).To(BeNil())
	By("Restoring the old config maps")
	var c, err = k.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.GetConfigMapName())
	Ω(err).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())

	oldSC := new(configs.SchedulerConfig)
	err = yaml.Unmarshal([]byte(oldConfigMap.Data[configmanager.DefaultPolicyGroup]), oldSC)
	Ω(err).NotTo(HaveOccurred())
	ts, tsErr := common.SetQueueTimestamp(oldSC, "default", "root")
	Ω(tsErr).NotTo(HaveOccurred())
	c.Data[configmanager.DefaultPolicyGroup], err = common.ToYAML(oldSC)
	Ω(err).NotTo(HaveOccurred())

	var e, err3 = k.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(e).NotTo(BeNil())

	err = yunikorn.WaitForQueueTS("root", ts, 2*time.Minute)
	Ω(err).NotTo(HaveOccurred())
}

func LogTestClusterInfoWrapper(testName string, namespaces []string) {
	// var restClient yunikorn.RClient
	outputDir := filepath.Join(configmanager.YuniKornTestConfig.LogDir, testName)
	// dirErr := os.Mkdir(outputDir, 0777)
	By("step 33: ")
	// Ω(dirErr).NotTo(HaveOccurred())
	By("step 34: ")
	for _, ns := range namespaces {
		By("step 41: " + ns)
		logErr := k8s.LogNamespaceInfo(ns, outputDir)
		Ω(logErr).NotTo(HaveOccurred())
	}
	// logErr := restClient.LogAppsInfo(outputDir)
	// Ω(logErr).NotTo(HaveOccurred())
	// logErr = restClient.LogQueuesInfo(outputDir)
	// Ω(logErr).NotTo(HaveOccurred())
	// logErr = restClient.LogNodesInfo(outputDir)
	// Ω(logErr).NotTo(HaveOccurred())
}

// Writes Yunikorn container log "yk.log" to test log directory
func LogYunikornContainer(testName string) {
	var restClient yunikorn.RClient
	qInfo, getQErr := restClient.GetPartitions("default")

	fmt.Fprintf(ginkgo.GinkgoWriter, "%s partition name is  %s\n", testName, qInfo.Partition)

	if getQErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s Problem in getting queues info\n", testName)
	}
	qJSON, qJSONErr := json.MarshalIndent(qInfo, "", "    ")
	if qJSONErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s Problem in getting queues info\n", testName)
	}
	fmt.Fprintf(ginkgo.GinkgoWriter, "%s queues are %s\n", testName, string(qJSON))

	appsInfo, getAppErr := restClient.GetApps("default", "root.fifoq")
	if getAppErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s Problem in getting apps info\n", testName)
	}
	appJSON, appJSONErr := json.MarshalIndent(appsInfo, "", "    ")
	if appJSONErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "%s Problem in getting apps info\n", testName)
	}
	fmt.Fprintf(ginkgo.GinkgoWriter, "%s apps are %s\n", testName, string(appJSON))

	fmt.Fprintf(ginkgo.GinkgoWriter, "%s Log yk logs info from\n", testName)
	Ω(k.SetClient()).To(BeNil())
	ykSchedName, schedErr := yunikorn.GetSchedulerPodName(k)
	Ω(schedErr).NotTo(HaveOccurred(), "Get sched failed")
	logBytes, getErr := k.GetPodLogs(ykSchedName, configmanager.YuniKornTestConfig.YkNamespace, configmanager.YKSchedulerContainer)
	Ω(getErr).NotTo(HaveOccurred(), "Get logs failed")
	fmt.Fprintf(ginkgo.GinkgoWriter, "%s logs dump \n", testName)
	fmt.Fprintf(ginkgo.GinkgoWriter, "%s logs are %s\n", testName, string(logBytes))
	// ykLogFilePath := filepath.Join(configmanager.YuniKornTestConfig.LogDir, testName, "yk.log")
	// writeErr := ioutil.WriteFile(ykLogFilePath, logBytes, 0644) //nolint:gosec // Log file readable by all
	// Ω(writeErr).NotTo(HaveOccurred(), "File write failed")
}

var Describe = ginkgo.Describe
var It = ginkgo.It
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach

var Equal = gomega.Equal
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
var BeEquivalentTo = gomega.BeEquivalentTo
