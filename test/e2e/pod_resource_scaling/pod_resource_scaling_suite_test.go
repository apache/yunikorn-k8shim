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

package pod_resource_scaling

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestPodResourceScaling(t *testing.T) {
	ginkgo.ReportAfterSuite("TestPodResourceScaling", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-pod_resource_scaling_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Pod Resource Scaling Suite")
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
})

var _ = ginkgo.AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred
var Equal = gomega.Equal
var By = ginkgo.By
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
