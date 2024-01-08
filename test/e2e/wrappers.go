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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	fmt.Fprintf(ginkgo.GinkgoWriter, "%s Log test cluster info\n", testName)
	var restClient yunikorn.RClient
	err := k.SetClient()
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error setting k8s client: %v\n", err)
		return
	}

	for _, ns := range namespaces {
		logErr := k8s.LogNamespaceInfo(ns)
		if logErr != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Error logging namespace info: %v\n", logErr)
			continue
		}

		pods, err := k.GetPodsByOptions(metav1.ListOptions{})
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Error getting pods: %v\n", err)
		} else {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Pod count is %d\n", len(pods.Items))
			for _, pod := range pods.Items {
				fmt.Fprintf(ginkgo.GinkgoWriter, "Pod name is %s\n", pod.Name)
				fmt.Fprintf(ginkgo.GinkgoWriter, "Pod details: %s\n", pod.String())
			}
		}

		logErr = restClient.LogAppsInfo(ns)
		if logErr != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Error logging apps info: %v\n", logErr)
		}
	}
	logErr := restClient.LogQueuesInfo()
	if logErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error logging queues info: %v\n", logErr)
	}

	logErr = restClient.LogNodesInfo()
	if logErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error logging nodes info: %v\n", logErr)
	}

	nodes, err := k.GetNodes()
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error getting nodes: %v\n", err)
	} else {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Node count is %d\n", len(nodes.Items))
		for _, node := range nodes.Items {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Running describe node command for %s..\n", node.Name)
			err = k.DescribeNode(node)
			if err != nil {
				fmt.Fprintf(ginkgo.GinkgoWriter, "Error describing node: %v\n", err)
			}
		}
	}
}

func LogYunikornContainer(testName string) {
	fmt.Fprintf(ginkgo.GinkgoWriter, "%s Log yk logs info from\n", testName)
	err := k.SetClient()
	if err != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error setting k8s client: %v\n", err)
		return
	}
	ykSchedName, schedErr := yunikorn.GetSchedulerPodName(k)
	if schedErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get the scheduler pod name: %v\n", schedErr)
		return
	}

	logBytes, getErr := k.GetPodLogs(ykSchedName, configmanager.YuniKornTestConfig.YkNamespace, configmanager.YKSchedulerContainer)
	if getErr != nil {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get scheduler pod logs: %v\n", getErr)
		return
	}
	fmt.Fprintf(ginkgo.GinkgoWriter, "Yunikorn Logs:%s\n", string(logBytes))
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
