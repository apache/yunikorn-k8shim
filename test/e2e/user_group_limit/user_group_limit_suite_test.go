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

package user_group_limit_test

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

func TestUserGroupLimit(t *testing.T) {
	ginkgo.ReportAfterSuite("TestUserGroupLimit", func(report ginkgo.Report) {
		err := reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-user_group_limit_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestUserGroupLimit", ginkgo.Label("TestUserGroupLimit"))
}

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.Succeed())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
	ginkgo.By("Tearing down namespace: " + dev)
	err = kClient.TearDownNamespace(dev)
	Ω(err).NotTo(HaveOccurred())
})

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred
var dev string
