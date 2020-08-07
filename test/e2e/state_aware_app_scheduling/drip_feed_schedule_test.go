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

package stateawareappscheduling_test

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func getSleepPodDef(namespace string, appName string, podName string) (*v1.Pod, error) {
	var sleepPodDef, err = common.GetAbsPath("../testdata/sleeppod_template.yaml")
	if err != nil {
		return nil, err
	}
	var sleepObj, err2 = k8s.GetPodObj(sleepPodDef)
	if err2 != nil {
		return nil, err
	}
	sleepObj.Namespace = namespace
	sleepObj.ObjectMeta.Labels["applicationId"] = appName
	sleepObj.Name = podName
	return sleepObj, nil
}

var _ = Describe("DripFeedSchedule:", func() {

	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var app1 = "app01-" + common.RandSeq(5)
	var app2 = "app02-" + common.RandSeq(5)
	var app3 = "app03-" + common.RandSeq(5)
	var NS = "sleep-" + common.RandSeq(10)
	var testTimeout float64 = 360
	var running = yunikorn.States().Application.Running
	var accepted = yunikorn.States().Application.Accepted
	var starting = yunikorn.States().Application.Starting

	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		By(fmt.Sprintf("Creating namespace: %s for sleep jobs", NS))
		var ns1, err1 = kClient.CreateNamespace(NS, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	PIt("Test_State_Aware_App_Sorting", func() {
		By("Submit 3 apps(app01, app02, app03) with one pod each")
		for _, each := range []string{app1, app2, app3} {
			var def, err1 = getSleepPodDef(NS, each, "pod1-"+common.RandSeq(5))
			Ω(err1).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(def, NS)
			Ω(err1).NotTo(HaveOccurred())
		}

		By(fmt.Sprintf("Get apps from specific queue: %s", NS))
		var appsFromQueue []map[string]interface{}
		//Poll for apps to appear in the queue
		err = wait.PollImmediate(time.Second, time.Duration(60)*time.Second, func() (done bool, err error) {
			appsFromQueue, err = restClient.GetAppsFromSpecificQueue("root." + NS)
			if len(appsFromQueue) == 3 {
				return true, nil
			}
			return false, err
		})
		Ω(err).NotTo(HaveOccurred())
		Ω(appsFromQueue).NotTo(BeEmpty())

		/*
			At this point, the apps state will be
			app01 - Starting
			app02 - Accepted
			app03 - Accepted
		*/
		By(fmt.Sprintf("Verify the app %s are in Starting state", app1))
		err = restClient.WaitForAppStateTransition(app1, starting, 60)
		Ω(err).NotTo(HaveOccurred())
		By(fmt.Sprintf("Verify the app %s are in Accepted state", app2))
		err = restClient.WaitForAppStateTransition(app2, accepted, 60)
		Ω(err).NotTo(HaveOccurred())
		By(fmt.Sprintf("Verify the app %s are in Accepted state", app3))
		err = restClient.WaitForAppStateTransition(app3, accepted, 60)
		Ω(err).NotTo(HaveOccurred())

		/*
			Add another pod for app01, and once this pod is allocated, verify app states:
				app01 - Running => pod1, pod2
				app02 - Starting => pod1
				app03 - Accepted => pod1

			Add another pod for app02, and once this pod is allocated, verify app states:
				app01 - Running => pod1, pod2
				app02 - Running => pod1, pod2
				app03 - starting => pod1

			Add another pod for app03, and once this pod is allocated, verify app states:
				app01 - Running => pod1, pod2
				app02 - Running => pod1, pod2
				app03 - Running => pod1, pod2
		*/
		var appStates = map[string][]string{
			app1: {running, starting, accepted},
			app2: {running, running, starting},
			app3: {running, running, running},
		}
		for _, each := range []string{app1, app2, app3} {
			By(fmt.Sprintf("Add one more pod to the app: %s", each))
			var podDef, err = getSleepPodDef(NS, each, "pod2-"+common.RandSeq(5))
			Ω(err).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(podDef, NS)
			Ω(err).NotTo(HaveOccurred())
			By(fmt.Sprintf("Verify that the app: %s is in running state", each))
			err = restClient.WaitForAppStateTransition(app1, appStates[each][0], 60)
			Ω(err).NotTo(HaveOccurred())
			err = restClient.WaitForAppStateTransition(app2, appStates[each][1], 60)
			Ω(err).NotTo(HaveOccurred())
			err = restClient.WaitForAppStateTransition(app3, appStates[each][2], 60)
			Ω(err).NotTo(HaveOccurred())
		}

	}, testTimeout)

	AfterEach(func() {
		By(fmt.Sprintf("Killing all jobs under %s namespace", NS))
		var pods, err = kClient.GetPodNamesFromNS(NS)
		Ω(err).NotTo(HaveOccurred())
		for _, each := range pods {
			err = kClient.DeletePod(each, NS)
			Ω(err).NotTo(HaveOccurred())
		}
		By(fmt.Sprintf("Deleting %s namespaces", NS))
		err = kClient.DeleteNamespace(NS)
		Ω(err).NotTo(HaveOccurred())
	})
})
