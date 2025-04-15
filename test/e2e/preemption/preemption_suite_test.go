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

package preemption_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestPreemption(t *testing.T) {
	ginkgo.ReportAfterSuite("TestPreemption", func(report ginkgo.Report) {
		err := reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-preemption_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestPreemption", ginkgo.Label("TestPreemption"))
}

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	var nodes *v1.NodeList
	nodes, err = kClient.GetNodes()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Nodes cant be empty")

	// Extract node allocatable resources
	for _, node := range nodes.Items {
		// skip master if it's marked as such
		node := node
		if k8s.IsMasterNode(&node) || !k8s.IsComputeNode(&node) {
			continue
		}
		if Worker == "" {
			Worker = node.Name
		} else {
			nodesToTaint = append(nodesToTaint, node.Name)
		}
	}
	Ω(Worker).NotTo(gomega.BeEmpty(), "Worker node not found")

	ginkgo.By("Tainting some nodes..")
	err = kClient.TaintNodes(nodesToTaint, taintKey, "value", v1.TaintEffectNoSchedule)
	Ω(err).NotTo(gomega.HaveOccurred())

	nodesDAOInfo, err := restClient.GetNodes(constants.DefaultPartition)
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(nodesDAOInfo).NotTo(gomega.BeNil())

	for _, node := range *nodesDAOInfo {
		if node.NodeID == Worker {
			WorkerMemRes = node.Available["memory"]
		}
	}
	WorkerMemRes /= (1000 * 1000) // change to M
	fmt.Fprintf(ginkgo.GinkgoWriter, "Worker node %s available memory %dM\n", Worker, WorkerMemRes)

	sleepPodMemLimit = int64(float64(WorkerMemRes) / 3)
	Ω(sleepPodMemLimit).NotTo(gomega.BeZero(), "Sleep pod memory limit cannot be zero")
	fmt.Fprintf(ginkgo.GinkgoWriter, "Sleep pod limit memory %dM\n", sleepPodMemLimit)

	sleepPodMemLimit2 = int64(float64(WorkerMemRes) / 4)
	Ω(sleepPodMemLimit2).NotTo(gomega.BeZero(), "Sleep pod memory limit cannot be zero")
	fmt.Fprintf(ginkgo.GinkgoWriter, "Sleep pod limit memory %dM\n", sleepPodMemLimit2)
})

var _ = ginkgo.AfterSuite(func() {

	ginkgo.By("Untainting some nodes")
	err := kClient.UntaintNodes(nodesToTaint, taintKey)
	Ω(err).NotTo(gomega.HaveOccurred(), "Could not remove taint from nodes "+strings.Join(nodesToTaint, ","))

	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
})
