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

package predicates_test

import (
	"path/filepath"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/ginkgo/reporters"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

var k = k8s.KubeCtl{}
var oldConfigMap = new(v1.ConfigMap)
var annotation string

var _ = BeforeSuite(func() {
	annotation = "ann-" + common.RandSeq(10)
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo", annotation)
})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

func TestPredicates(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter(
		filepath.Join(configmanager.YuniKornTestConfig.LogDir, "Predicates_junit.xml"))
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "TestPredicates", []ginkgo.Reporter{junitReporter})
}

// type Benchmarker ginkgo.Benchmarker
var Fail = ginkgo.Fail

var Describe = ginkgo.Describe
var DescribeTable = table.DescribeTable
var Entry = table.Entry
var Context = ginkgo.Context
var It = ginkgo.It
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach

// Declarations for Gomega DSL
var RegisterFailHandler = gomega.RegisterFailHandler
var Ω = gomega.Ω

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var BeNil = gomega.BeNil
var BeTrue = gomega.BeTrue
var HaveOccurred = gomega.HaveOccurred
var MatchRegexp = gomega.MatchRegexp
var BeZero = gomega.BeZero
var BeEquivalentTo = gomega.BeEquivalentTo
var ContainElement = gomega.ContainElement
var HaveKeyWithValue = gomega.HaveKeyWithValue
