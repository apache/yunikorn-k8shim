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

package priority_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestPriorityScheduling(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	junitReporter := reporters.NewJUnitReporter(filepath.Join(configmanager.YuniKornTestConfig.LogDir, "priority_scheduling_junit.xml"))
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "TestPriorityScheduling", []ginkgo.Reporter{junitReporter})
}

var kubeClient k8s.KubeCtl

var preemptPolicyNever = v1.PreemptNever

var lowPriorityClass = schedulingv1.PriorityClass{
	ObjectMeta: metav1.ObjectMeta{
		Name: "yk-test-low",
	},
	Value:            -100,
	PreemptionPolicy: &preemptPolicyNever,
}

var highPriorityClass = schedulingv1.PriorityClass{
	ObjectMeta: metav1.ObjectMeta{
		Name: "yk-test-high",
	},
	Value:            100,
	PreemptionPolicy: &preemptPolicyNever,
}

var normalPriorityClass = schedulingv1.PriorityClass{
	ObjectMeta: metav1.ObjectMeta{
		Name: "yk-test-normal",
	},
	Value:            0,
	PreemptionPolicy: &preemptPolicyNever,
}

var _ = ginkgo.BeforeSuite(func() {
	var err error
	kubeClient = k8s.KubeCtl{}
	Expect(kubeClient.SetClient()).To(BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	By(fmt.Sprintf("Creating priority class %s", lowPriorityClass.Name))
	_, err = kubeClient.CreatePriorityClass(&lowPriorityClass)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Creating priority class %s", highPriorityClass.Name))
	_, err = kubeClient.CreatePriorityClass(&highPriorityClass)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Creating priority class %s", normalPriorityClass.Name))
	_, err = kubeClient.CreatePriorityClass(&normalPriorityClass)
	Ω(err).ShouldNot(HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {
	var err error
	kubeClient = k8s.KubeCtl{}
	Expect(kubeClient.SetClient()).To(BeNil())

	By(fmt.Sprintf("Removing priority class %s", normalPriorityClass.Name))
	err = kubeClient.DeletePriorityClass(normalPriorityClass.Name)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Removing priority class %s", highPriorityClass.Name))
	err = kubeClient.DeletePriorityClass(highPriorityClass.Name)
	Ω(err).ShouldNot(HaveOccurred())

	By(fmt.Sprintf("Removing priority class %s", lowPriorityClass.Name))
	err = kubeClient.DeletePriorityClass(lowPriorityClass.Name)
	Ω(err).ShouldNot(HaveOccurred())
})

var By = ginkgo.By

var Ω = gomega.Ω
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
var Expect = gomega.Expect
var Equal = gomega.Equal
