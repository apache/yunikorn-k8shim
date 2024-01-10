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

package bin_packing

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestBinPacking(t *testing.T) {
	ginkgo.ReportAfterSuite("TestBinPacking", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-bin_packing_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestBinPacking", ginkgo.Label("TestBinPacking"))
}

var suiteName string
var oldConfigMap = new(v1.ConfigMap)
var annotation = "ann-" + common.RandSeq(10)
var kClient = k8s.KubeCtl{} //nolint

var _ = BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)

	Ω(kClient.SetClient()).To(BeNil())
	/* Sample configMap. Post-update, Yunikorn will use binpacking node sort and fair app sort
		partitions:
	      - name: default
	        queues:
	        - name: root
	          nodesortpolicy:
				binpacking
	          properties:
	            application.sort.policy: fifo
	            timestamp: "1619566965"
	          submitacl: '*'
	        placementrules:
	        - name: tag
	          create: true
	          value: namespace
	*/
	By("Enabling new scheduling config with binpacking node sort policy")
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fifo", annotation, func(sc *configs.SchedulerConfig) error {
		setErr := common.SetSchedulingPolicy(sc, "default", "root", "fifo")
		if setErr != nil {
			return setErr
		}
		setErr = common.SetNodeSortPolicy(sc, "default", configs.NodeSortingPolicy{Type: "binpacking"})
		if setErr != nil {
			return setErr
		}
		_, tsErr := common.SetQueueTimestamp(sc, "default", "root")
		if tsErr != nil {
			return tsErr
		}
		_, yamlErr := common.ToYAML(sc)
		if yamlErr != nil {
			return yamlErr
		}
		return nil
	})

	// Restart yunikorn and port-forward
	// Required to change node sort policy.
	ginkgo.By("Restart the scheduler pod")
	yunikorn.RestartYunikorn(&kClient)

	ginkgo.By("Port-forward scheduler pod after restart")
	yunikorn.RestorePortForwarding(&kClient)
})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

var Describe = ginkgo.Describe
var It = ginkgo.It
var By = ginkgo.By
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var BeNumerically = gomega.BeNumerically
var HaveOccurred = gomega.HaveOccurred
