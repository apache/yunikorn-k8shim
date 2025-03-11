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

package spark_jobs_scheduling

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var _ = Describe("", func() {

	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var exErr error
	var sparkNS = "spark-" + common.RandSeq(10)
	var svcAcc = "svc-acc-" + common.RandSeq(10)
	var roleName = "spark-jobs-role-" + common.RandSeq(5)
	var clusterEditRole = "edit"
	var sparkImage = os.Getenv("SPARK_IMAGE")
	var sparkPyImage = os.Getenv("SPARK_PYTHON_IMAGE")
	var sparkExecutorCount = 3

	BeforeEach(func() {
		By(fmt.Sprintf("Spark image is: %s", sparkImage))
		Ω(sparkImage).NotTo(BeEmpty())
		By(fmt.Sprintf("Spark_py image is: %s", sparkPyImage))
		Ω(sparkPyImage).NotTo(BeEmpty())
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		Ω(exErr).NotTo(HaveOccurred())
		By(fmt.Sprintf("Creating namespace: %s for spark jobs", sparkNS))
		ns1, err := kClient.CreateNamespace(sparkNS, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		By(fmt.Sprintf("Creating service account: %s under namespace: %s", svcAcc, sparkNS))
		_, err = kClient.CreateServiceAccount(svcAcc, sparkNS)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Creating cluster role binding: %s for spark jobs", roleName))
		_, err = kClient.CreateClusterRoleBinding(roleName, clusterEditRole, sparkNS, svcAcc)
		Ω(err).NotTo(HaveOccurred())
	})

	It("Test_With_Spark_Jobs", func() {
		By("Submit the spark jobs")
		err := exec.Command(
			"bash",
			"../testdata/spark_jobs.sh",
			sparkImage,
			sparkPyImage,
			sparkNS,
			svcAcc,
			string(rune(sparkExecutorCount))).Run()
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("[%s] wait for root queue to appear\n", time.Now().Format("01-02-2006 15:04:05")))
		err = restClient.WaitforQueueToAppear(configmanager.DefaultPartition, configmanager.RootQueue, 120)
		Ω(err).NotTo(HaveOccurred())

		sparkQueueName := configmanager.RootQueue + "." + sparkNS
		By(fmt.Sprintf("[%s] wait for %s queue to appear\n", time.Now().Format("01-02-2006 15:04:05"),
			sparkQueueName))
		err = restClient.WaitforQueueToAppear(configmanager.DefaultPartition, sparkQueueName, 120)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Get apps from specific queue: %s", sparkQueueName))
		var appsFromQueue []*dao.ApplicationDAOInfo
		// Poll for apps to appear in the queue
		err = wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, time.Duration(120)*time.Second, false, func(context.Context) (done bool, err error) {
			appsFromQueue, err = restClient.GetApps(configmanager.DefaultPartition, sparkQueueName)
			if err != nil {
				return false, err
			}
			if len(appsFromQueue) == 4 {
				return true, nil
			}
			return false, err
		})
		Ω(err).NotTo(HaveOccurred())
		Ω(appsFromQueue).NotTo(BeEmpty())

		// Sort by submission time oldest first
		sort.SliceStable(appsFromQueue, func(i, j int) bool {
			l := appsFromQueue[i]
			r := appsFromQueue[j]
			return l.SubmissionTime < r.SubmissionTime
		})
		Ω(appsFromQueue).NotTo(BeEmpty())

		var appIds []string
		for _, each := range appsFromQueue {
			appIds = append(appIds, each.ApplicationID)
		}
		By(fmt.Sprintf("Apps submitted are: %s", appIds))

		// Verify that all the spark jobs are scheduled and are in running state.
		for _, id := range appIds[1:] {
			By(fmt.Sprintf("Verify driver pod for application %s has been created.", id))
			err = kClient.WaitForPodBySelector(sparkNS, fmt.Sprintf("spark-app-selector=%s, spark-role=driver", id), 180*time.Second)
			Ω(err).ShouldNot(HaveOccurred())

			By(fmt.Sprintf("Verify driver pod for application %s was completed.", id))
			err = kClient.WaitForPodBySelectorSucceeded(sparkNS, fmt.Sprintf("spark-app-selector=%s, spark-role=driver", id), 360*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{sparkNS})

		By("Killing all spark jobs")
		// delete the Spark pods one by one
		err := kClient.DeletePods(sparkNS)
		Ω(err).NotTo(HaveOccurred())

		By("Deleting cluster role bindings ")
		err = kClient.DeleteClusterRoleBindings(roleName)
		Ω(err).NotTo(HaveOccurred())

		By("Deleting service account")
		err = kClient.DeleteServiceAccount(svcAcc, sparkNS)
		Ω(err).NotTo(HaveOccurred())

		By("Deleting test namespaces")
		err = kClient.DeleteNamespace(sparkNS)
		Ω(err).NotTo(HaveOccurred())
	})
})
