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
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
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

func DumpClusterInfoIfSpecFailed(suiteName string, namespaces []string) {
	// should call this function in ginkgo.AfterEach
	// write cluster info to files by log type (ykFullStateDump, k8sClusterInfo, ykContainerLog)
	testDescription := ginkgo.CurrentSpecReport()
	if testDescription.Failed() {
		specName := testDescription.LeafNodeText
		fmt.Fprintf(ginkgo.GinkgoWriter, "Logging yk fullstatedump, spec: %s\n", specName)
		err := dumpYKFullStateDump(suiteName, specName)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Fail to log yk fullstatedump, spec: %s, err: %v\n", specName, err)
		}

		fmt.Fprintf(ginkgo.GinkgoWriter, "Logging k8s cluster info, spec: %s\n", specName)
		err = dumpKubernetesClusterInfo(suiteName, specName, namespaces)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Fail to log k8s cluster info, spec: %s, err: %v\n", specName, err)
		}

		fmt.Fprintf(ginkgo.GinkgoWriter, "Logging yk container logs, spec: %s\n", specName)
		err = dumpYunikornContainer(suiteName, specName)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Fail to log yk container logs, spec: %s, err: %v\n", specName, err)
		}
	}
}

func dumpYKFullStateDump(suiteName string, specName string) error {
	file, err := common.CreateLogFile(suiteName, specName, "ykFullStateDump", "json")
	if err != nil {
		return err
	}
	defer file.Close()

	var restClient yunikorn.RClient
	fullStateDumpJson, err := restClient.GetFullStateDump()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(file, fullStateDumpJson)
	return err
}

func dumpKubernetesClusterInfo(suiteName string, specName string, namespaces []string) error {
	file, err := common.CreateLogFile(suiteName, specName, "k8sClusterInfo", "txt")
	if err != nil {
		return err
	}
	defer file.Close()

	err = k.SetClient()
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		err = k.LogNamespaceInfo(file, ns)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to log namespace info, ns:%s, err: %v\n", ns, err)
		}
	}

	err = k.LogPodsInfo(file)
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to log pods info, err: %v\n", err)
	}

	err = k.LogNodesInfo(file)
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to log nodes info, err: %v\n", err)
	}

	return nil
}

func dumpYunikornContainer(suiteName string, specName string) error {
	file, err := common.CreateLogFile(suiteName, specName, "ykContainerLog", "txt")
	if err != nil {
		return err
	}
	defer file.Close()

	err = k.SetClient()
	if err != nil {
		return err
	}

	ykSchedName, schedErr := yunikorn.GetSchedulerPodName(k)
	if schedErr != nil {
		return schedErr
	}

	logBytes, getErr := k.GetPodLogs(ykSchedName, configmanager.YuniKornTestConfig.YkNamespace, configmanager.YKSchedulerContainer)
	if getErr != nil {
		return getErr
	}

	_, err = fmt.Fprintf(file, "Yunikorn Logs:\n%s\n", string(logBytes))
	return err
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
