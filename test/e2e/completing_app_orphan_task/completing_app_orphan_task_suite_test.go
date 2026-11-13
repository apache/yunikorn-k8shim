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

package completing_app_orphan_task

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestCompletingAppOrphanTask(t *testing.T) {
	ginkgo.ReportAfterSuite("TestCompletingAppOrphanTask", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-completing_app_orphan_task_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestCompletingAppOrphanTask", ginkgo.Label("TestCompletingAppOrphanTask"))
}

var suiteName string
var oldConfigMap = new(v1.ConfigMap)
var kClient k8s.KubeCtl

var _ = BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)

	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(Succeed())

	yunikorn.EnsureYuniKornConfigsPresent()
	By("Port-forward the scheduler pod")
	err := kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(HaveOccurred())
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo")
})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

var Describe = ginkgo.Describe
var It = ginkgo.It
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach

var Ω = gomega.Expect
var Equal = gomega.Equal
var BeEmpty = gomega.BeEmpty
var HaveOccurred = gomega.HaveOccurred
var Succeed = gomega.Succeed
