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

package app

import (
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestApp(t *testing.T) {
	ginkgo.ReportAfterSuite("TestApp", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-app_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(gomega.HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "App Suite")
}

var Ω = gomega.Ω
