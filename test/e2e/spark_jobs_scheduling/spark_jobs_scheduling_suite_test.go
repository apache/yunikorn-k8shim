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

package spark_jobs_scheduling

import (
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

var oldConfigMap = new(v1.ConfigMap)
var annotation = "ann-" + common.RandSeq(10)
var _ = BeforeSuite(func() {
	annotation = "ann-" + common.RandSeq(10)
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "stateaware", annotation)
})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

func TestSparkJobs(t *testing.T) {
	ginkgo.ReportAfterSuite("TestSparkJobs", func(report ginkgo.Report) {
		err := reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-SparkJobs_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestSparkJobs", ginkgo.Label("TestSparkJobs"))
}

// Declarations for Ginkgo DSL
var Fail = ginkgo.Fail
var Describe = ginkgo.Describe
var It = ginkgo.It
var By = ginkgo.By
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite

// Declarations for Gomega DSL
var RegisterFailHandler = gomega.RegisterFailHandler

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
var BeEmpty = gomega.BeEmpty
