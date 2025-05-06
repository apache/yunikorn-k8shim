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

package simple_preemptor_test

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestSimplePreemptor(t *testing.T) {
	ginkgo.ReportAfterSuite("TestSimplePreemptor", func(report ginkgo.Report) {
		err := reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-simple_preemptor_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestSimplePreemptor", ginkgo.Label("TestSimplePreemptor"))
}

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
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	var nodes *v1.NodeList
	nodes, err = kClient.GetNodes()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(len(nodes.Items)).NotTo(gomega.BeZero(), "Events cant be empty")

	// Extract node allocatable resources
	for _, node := range nodes.Items {
		// skip master if it's marked as such
		node := node
		if k8s.IsMasterNode(&node) || !k8s.IsComputeNode(&node) {
			continue
		}
		if Worker1 == "" {
			Worker1 = node.Name
			Worker1Res = node.Status.Allocatable.Memory()
		} else if Worker2 == "" {
			Worker2 = node.Name
			Worker2Res = node.Status.Allocatable.Memory()
		} else {
			nodesToTaint = append(nodesToTaint, node.Name)
		}
	}
	ginkgo.By("Worker1:" + Worker1)
	ginkgo.By("Worker2:" + Worker2)

	ginkgo.By("Tainting some nodes..")
	err = kClient.TaintNodes(nodesToTaint, taintKey, "value", v1.TaintEffectNoSchedule)
	Ω(err).NotTo(gomega.HaveOccurred())

	var pods *v1.PodList
	totalPodQuantity1 := *resource.NewQuantity(0, resource.DecimalSI)
	totalPodQuantity2 := *resource.NewQuantity(0, resource.DecimalSI)
	pods, err = kClient.GetPods("yunikorn")
	if err == nil {
		for _, pod := range pods.Items {
			for _, c := range pod.Spec.Containers {
				if pod.Spec.NodeName == Worker1 {
					totalPodQuantity1.Add(*resource.NewQuantity(c.Resources.Requests.Memory().Value(), resource.DecimalSI))
				} else if pod.Spec.NodeName == Worker2 {
					totalPodQuantity2.Add(*resource.NewQuantity(c.Resources.Requests.Memory().Value(), resource.DecimalSI))
				}
			}
		}
	}
	Worker1Res.Sub(totalPodQuantity1)
	sleepPodMemLimit1 = int64(float64(Worker1Res.Value())/3.5) / (1000 * 1000)
	Worker2Res.Sub(totalPodQuantity2)
	sleepPodMemLimit2 = int64(float64(Worker2Res.Value())/3.5) / (1000 * 1000)
})

var _ = ginkgo.AfterSuite(func() {

	ginkgo.By("Untainting some nodes")
	err := kClient.UntaintNodes(nodesToTaint, taintKey)
	Ω(err).NotTo(gomega.HaveOccurred(), "Could not remove taint from nodes "+strings.Join(nodesToTaint, ","))

	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred
var dev string
