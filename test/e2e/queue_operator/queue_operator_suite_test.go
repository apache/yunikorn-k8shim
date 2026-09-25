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

package queue_operator

import (
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
)

var (
	kClient   k8s.KubeCtl
	apiClient client.Client
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestQueueOperator(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.ReportAfterSuite("TestQueueOperator", func(report ginkgo.Report) {
		gomega.Expect(common.CreateJUnitReportDir()).To(gomega.Succeed())
		gomega.Expect(reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-queue_operator_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)).To(gomega.Succeed())
	})
	ginkgo.RunSpecs(t, "TestQueueOperator", ginkgo.Label("TestQueueOperator"))
}

var _ = ginkgo.BeforeSuite(func() {
	kClient = k8s.KubeCtl{}
	gomega.Expect(kClient.SetClient()).To(gomega.Succeed())
	restConfig, err := kClient.GetKubeConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	scheme := runtime.NewScheme()
	gomega.Expect(clientgoscheme.AddToScheme(scheme)).To(gomega.Succeed())
	gomega.Expect(appsv1.AddToScheme(scheme)).To(gomega.Succeed())
	gomega.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
	gomega.Expect(admissionregistrationv1.AddToScheme(scheme)).To(gomega.Succeed())
	gomega.Expect(queuev1alpha1.AddToScheme(scheme)).To(gomega.Succeed())
	apiClient, err = client.New(restConfig, client.Options{Scheme: scheme})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})
