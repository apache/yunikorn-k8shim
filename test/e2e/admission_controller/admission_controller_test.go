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

package admission_controller_test

import (
    "fmt"

    "github.com/onsi/ginkgo"
    "github.com/onsi/gomega"
    v1 "k8s.io/api/core/v1"

    "github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
    "github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
    "github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = ginkgo.Describe("AdmissionController", func() {
    var kClient k8s.KubeCtl
    var ns = "admissionController_test"
    var oldConfigMap *v1.ConfigMap

    ginkgo.BeforeSuite(func() {
        // Initializing kubectl client and creating test namespace
        kClient = k8s.KubeCtl{}
        gomega.Ω(kClient.SetClient()).Should(gomega.BeNil())

        ginkgo.By(fmt.Sprintf("Creating test namepsace %s", ns))
        namespace, err := kClient.CreateNamespace(ns, nil)
        gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
        gomega.Ω(namespace.Status.Phase).Should(gomega.Equal(v1.NamespaceActive))

        // Get and copy default YuniKorn's configMap
        c, err := kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
            configmanager.DefaultYuniKornConfigMap)
        gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
        gomega.Ω(c).ShouldNot(gomega.BeNil())

        oldConfigMap = c.DeepCopy()
        gomega.Ω(c).Should(gomega.BeEquivalentTo(oldConfigMap))

        // Configure YuniKorn with invalid configMap so that pods can't be updated by YuniKorn
        // TODO

        // Create a Pod in the test namespace
        // TODO
    })

    ginkgo.Describe("To verify that the pod has not been mutated", func() {
        ginkgo.It("Scheduler name should be ...", func() {
            // TODO
        })

        ginkgo.It("Labels should be ...", func() {
            // TODO
        })
    })

    ginkgo.AfterEach(func() {
        // call the healthCheck api to check scheduler health
        ginkgo.By("Check YuniKorn's health")
        checks, err := yunikorn.GetFailedHealthChecks()
        gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
        gomega.Ω(checks).Should(gomega.Equal(""), checks)
    })

    ginkgo.AfterSuite(func() {
        ginkgo.By(fmt.Sprintf("Deleting test namepsace %s", ns))
        err := kClient.DeleteNamespace(ns)
        gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

        ginkgo.By("Restoring the old config maps")
        c, err := kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
            configmanager.DefaultYuniKornConfigMap)
        gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
        gomega.Ω(c).ShouldNot(gomega.BeNil())

        c.Data = oldConfigMap.Data
        e, err := kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
        gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
        gomega.Ω(e).ShouldNot(gomega.BeNil())
    })
})
