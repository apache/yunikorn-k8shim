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

package gangscheduling_test

import (
	"fmt"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	"github.com/onsi/ginkgo/extensions/table"
	v1 "k8s.io/api/core/v1"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/reporters"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestGangScheduling(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	junitReporter := reporters.NewJUnitReporter(filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-gang_scheduling_junit.xml"))
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "TestGangScheduling", []ginkgo.Reporter{junitReporter})
}

var oldConfigMap = new(v1.ConfigMap)
var annotation = "ann-" + common.RandSeq(10)
var kClient = k8s.KubeCtl{} //nolint
var fifoQName = "fifoq"
var saQName = "saq"

var _ = BeforeSuite(func() {
	By(fmt.Sprintf("Enabling new scheduling config with queues: %s, %s", fifoQName, saQName))
	Ω(kClient.SetClient()).To(BeNil())
	var c, getErr = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.GetConfigMapName())
	Ω(getErr).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())
	c.DeepCopyInto(oldConfigMap)

	sc := common.CreateBasicConfigMap()
	setErr := common.SetSchedulingPolicy(sc, "default", "root", "fifo")
	Ω(setErr).NotTo(HaveOccurred())
	fifoQConfig := configs.QueueConfig{
		Name: fifoQName, SubmitACL: "*",
		Properties: map[string]string{"application.sort.policy": "fifo"},
		Resources:  configs.Resources{Max: map[string]string{"memory": "300", "vcore": "300"}},
	}
	saQConfig := configs.QueueConfig{Name: saQName, SubmitACL: "*",
		Properties: map[string]string{"application.sort.policy": "stateaware"},
		Resources:  configs.Resources{Max: map[string]string{"memory": "300", "vcore": "300"}},
	}
	addQErr := common.AddQueue(sc, "default", "root", fifoQConfig)
	Ω(addQErr).NotTo(HaveOccurred())
	addQErr = common.AddQueue(sc, "default", "root", saQConfig)
	Ω(addQErr).NotTo(HaveOccurred())

	ts, tsErr := common.SetQueueTimestamp(sc, "default", "root")
	Ω(tsErr).NotTo(HaveOccurred())
	configStr, yamlErr := common.ToYAML(sc)
	Ω(yamlErr).NotTo(HaveOccurred())
	c.Data[configmanager.DefaultPolicyGroup] = configStr
	var d, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(d).NotTo(BeNil())

	tsErr = yunikorn.WaitForQueueTS("root", ts, 2*time.Minute)
	Ω(tsErr).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	tests.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

// Declarations for Ginkgo DSL
var Describe = ginkgo.Describe

var It = ginkgo.It
var PIt = ginkgo.PIt
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
var CurrentGinkgoTestDescription = ginkgo.CurrentGinkgoTestDescription
var DescribeTable = table.DescribeTable
var Entry = table.Entry

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var BeNumerically = gomega.BeNumerically
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
