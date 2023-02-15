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

package resourcefairness_test

import (
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

var oldConfigMap = new(v1.ConfigMap)
var annotation = "ann-" + common.RandSeq(10)
var kClient = k8s.KubeCtl{} //nolint
var _ = BeforeSuite(func() {
	Ω(kClient.SetClient()).To(BeNil())
	annotation = "ann-" + common.RandSeq(10)
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateCustomConfigMapWrapper(oldConfigMap, "fair", annotation, func(sc *configs.SchedulerConfig) error {
		// remove placement rules so we can control queue
		sc.Partitions[0].PlacementRules = nil
		if err := common.AddQueue(sc, "default", "root", configs.QueueConfig{
			Name:       "default",
			Parent:     false,
			Resources:  configs.Resources{Max: map[string]string{"vcore": "500m", "memory": "500M"}, Guaranteed: map[string]string{"vcore": "500m", "memory": "500M"}},
			Properties: map[string]string{"application.sort.policy": "fair"},
		}); err != nil {
			return err
		}
		return nil
	})

	// Restart yunikorn and port-forward
	// Required to change node sort policy.
	ginkgo.By("Restart the scheduler pod")
	yunikorn.RestartYunikorn(&kClient)

	ginkgo.By("Port-forward scheduler pod after restart")
	yunikorn.RestorePortForwarding(&kClient)

})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

func TestResourceFairness(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	junitReporter := reporters.NewJUnitReporter(filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-resourcefairness.xml"))
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "Resource Fairness", []ginkgo.Reporter{junitReporter})
}

// Declarations for Ginkgo DSL
var Describe = ginkgo.Describe
var PIt = ginkgo.PIt
var It = ginkgo.It
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
var CurrentGinkgoTestDescription = ginkgo.CurrentGinkgoTestDescription

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
