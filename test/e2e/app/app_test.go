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
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"

	"github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	crdclientset "github.com/apache/yunikorn-k8shim/pkg/client/clientset/versioned"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
)

var _ = ginkgo.Describe("App", func() {
	var kClient k8s.KubeCtl
	var appClient *crdclientset.Clientset
	// var appCRDDef string
	var appCRD *v1alpha1.Application
	var dev = "apptest"

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client and create test namespace
		kClient = k8s.KubeCtl{}
		gomega.Ω(kClient.SetClient()).To(gomega.BeNil())

		yunikorn.EnsureYuniKornConfigsPresent()

		ginkgo.By("Port-forward the scheduler pod")
		err := kClient.PortForwardYkSchedulerPod()
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("create apptest namespace")
		ns, err := kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))

		ginkgo.By("Deploy the example Application to the apptest namespace")
		appClient, err = yunikorn.NewApplicationClient()
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// error test case
		// error queue format
		appCRDDef, err := common.GetAbsPath("../testdata/app/application_err_queue.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).To(gomega.HaveOccurred())
		// error gang name format
		appCRDDef, err = common.GetAbsPath("../testdata/app/application_err_name.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).To(gomega.HaveOccurred())
		// error minMember format
		appCRDDef, err = common.GetAbsPath("../testdata/app/application_err_minmember.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).To(gomega.HaveOccurred())
		// error minResource format
		appCRDDef, err = common.GetAbsPath("../testdata/app/application_err_minresource.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).To(gomega.HaveOccurred())
		// error NodeSelector format
		appCRDDef, err = common.GetAbsPath("../testdata/app/application_err_nodeselector.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).To(gomega.HaveOccurred())
		// error tolerations format
		appCRDDef, err = common.GetAbsPath("../testdata/app/application_err_tolerations.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).To(gomega.HaveOccurred())

		// correct test case
		appCRDDef, err = common.GetAbsPath("../testdata/app/application.yaml")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		err = k8s.ApplyYamlWithKubectl(appCRDDef, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		appCRD, err = yunikorn.GetApplication(appClient, dev, "example")
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appCRD).NotTo(gomega.BeNil())
	})

	ginkgo.Context("Verifying the application CRD information", func() {
		ginkgo.It("Verify that the Application is created", func() {
			ginkgo.By("Verify that the Application is created")
			gomega.Ω(appCRD.Spec.Queue).To(gomega.Equal("root.default"))
			gomega.Ω(appCRD.ObjectMeta.Name).To(gomega.Equal("example"))
			gomega.Ω(appCRD.ObjectMeta.Namespace).To(gomega.Equal(dev))
			policy := appCRD.Spec.SchedulingPolicy
			gomega.Ω(policy.Type).To(gomega.Equal(v1alpha1.TryOnce))
			gomega.Ω(appCRD.Spec.TaskGroups[0].Name).To(gomega.Equal("test-task-0001"))
			gomega.Ω(appCRD.Spec.TaskGroups[0].MinMember).To(gomega.Equal(int32(1)))
			anscpu := resource.MustParse("300m")
			ansmem := resource.MustParse("128Mi")
			gomega.Ω(appCRD.Spec.TaskGroups[0].MinResource["cpu"]).To(gomega.Equal(anscpu))
			gomega.Ω(appCRD.Spec.TaskGroups[0].MinResource["memory"]).To(gomega.Equal(ansmem))
			gomega.Ω(appCRD.Spec.TaskGroups[0].NodeSelector["locate"]).To(gomega.Equal("west"))
			gomega.Ω(appCRD.Spec.TaskGroups[0].Tolerations[0].Key).To(gomega.Equal("key"))
			gomega.Ω(appCRD.Spec.TaskGroups[0].Tolerations[0].Operator).To(gomega.Equal(v1.TolerationOpEqual))
			gomega.Ω(appCRD.Spec.TaskGroups[0].Tolerations[0].Value).To(gomega.Equal("value"))
			gomega.Ω(appCRD.Spec.TaskGroups[0].Tolerations[0].Effect).To(gomega.Equal(v1.TaintEffectNoSchedule))
		})

		ginkgo.AfterEach(func() {
			// call the healthCheck api to check scheduler health
			ginkgo.By("Check Yunikorn's health")
			checks, err := yunikorn.GetFailedHealthChecks()
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
			gomega.Ω(checks).To(gomega.Equal(""), checks)
		})

		ginkgo.AfterSuite(func() {
			ginkgo.By("Deleting application CRD")
			err := yunikorn.DeleteApplication(appClient, dev, "example")
			gomega.Ω(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Deleting development namespaces")
			err = kClient.DeleteNamespace(dev)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		})
	})
})
