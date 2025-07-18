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

package replication_test

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

func TestReplication(t *testing.T) {
	ginkgo.ReportAfterSuite("TestReplication", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-replication_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestReplication", ginkgo.Label("TestReplication"))
}

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var oldConfigMap = new(v1.ConfigMap)

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	
	// Initialize kubectl client
	kClient = k8s.KubeCtl{}
	gomega.Ω(kClient.SetClient()).To(gomega.Succeed())
	
	// Initialize rest client
	restClient = yunikorn.RClient{}
	
	yunikorn.EnsureYuniKornConfigsPresent()
	
	ginkgo.By("Port-forward the scheduler pod")
	err := kClient.PortForwardYkSchedulerPod()
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo")
})

var _ = ginkgo.AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

 