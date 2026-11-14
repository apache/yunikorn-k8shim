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

package apprejection_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestAppRejection(t *testing.T) {
	ginkgo.ReportAfterSuite("TestAppRejection", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-app_rejection_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestAppRejection", ginkgo.Label("TestAppRejection"))
}

var Ω = gomega.Ω
var HaveOccurred = gomega.HaveOccurred

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.Succeed())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	yunikorn.EnsureYuniKornConfigsPresent()
	ginkgo.By("Port-forward the scheduler pod")
	err := kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(HaveOccurred())

	// Rules never create queues and there is no root.default.
	// Root denies submit, so a queue without a submitacl denies everyone.
	yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "", func(sc *configs.SchedulerConfig) error {
		partition := &sc.Partitions[0]
		partition.PlacementRules = []configs.PlacementRule{
			{Name: "provided", Create: false},
			{Name: "tag", Value: "namespace", Create: false},
		}
		root := &partition.Queues[0]
		root.SubmitACL = ""
		root.Queues = []configs.QueueConfig{
			{Name: openQueue, SubmitACL: "*"},
			{Name: restrictedQueue, SubmitACL: "other-user"},
			{
				Name:      gangQueue,
				SubmitACL: "*",
				// task groups need a fifo queue, which is also the default for a leaf queue
				Properties: map[string]string{"application.sort.policy": "fifo"},
				Resources: configs.Resources{
					Max: map[string]string{"vcore": gangQueueMaxVcore, "memory": gangQueueMaxMemory},
				},
			},
		}
		return nil
	})
})

var _ = ginkgo.AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})
