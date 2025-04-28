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

package recoveryandrestart_test

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

func TestRecoveryAndRestart(t *testing.T) {
	ginkgo.ReportAfterSuite("TestRecoveryAndRestart", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-recovery_and_restart_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(gomega.HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "RecoveryAndRestart Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}

	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")

	ginkgo.By("Restart the scheduler pod")
	yunikorn.RestartYunikorn(&kClient)

	ginkgo.By("Port-forward scheduler pod after restart")
	yunikorn.RestorePortForwarding(&kClient)
})

var _ = ginkgo.AfterSuite(func() {

	// call the healthCheck api to check scheduler health
	ginkgo.By("Check Yunikorn's health")
	checks, err2 := yunikorn.GetFailedHealthChecks()
	Ω(err2).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)

	ginkgo.By("Restoring the old config maps")
	var c, err1 = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.DefaultYuniKornConfigMap)
	Ω(err1).NotTo(gomega.HaveOccurred())
	Ω(c).NotTo(gomega.BeNil())
	c.Data = oldConfigMap.Data
	var e, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(gomega.HaveOccurred())
	Ω(e).NotTo(gomega.BeNil())
})

var Ω = gomega.Ω
var dev string
