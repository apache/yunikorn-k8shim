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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amConf "github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const userInfoAnnotation = constants.DomainYuniKorn + "user.info"
const nonExistentNode = "non-existent-node"
const defaultPodTimeout = 10 * time.Second
const cronJobPodTimeout = 65 * time.Second

var _ = ginkgo.Describe("AdmissionController", func() {
	ginkgo.BeforeEach(func() {
		kubeClient = k8s.KubeCtl{}
		gomega.Expect(kubeClient.SetClient()).To(gomega.Succeed())
		ns = "ns-" + common.RandSeq(10)
		ginkgo.By(fmt.Sprintf("Creating namespace: %s for admission controller tests", ns))
		var ns1, err1 = kubeClient.CreateNamespace(ns, nil)
		gomega.Ω(err1).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.It("Verifying pod with preempt priority class", func() {
		ginkgo.By("has correct properties set")
		podCopy := testPod.DeepCopy()
		podCopy.Name = "preempt-pod"
		podCopy.Spec.PriorityClassName = testPreemptPriorityClass.Name
		podCopy.Spec.NodeName = nonExistentNode
		pod, err := kubeClient.CreatePod(podCopy, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		gomega.Ω(*pod.Spec.Priority).Should(gomega.Equal(testPreemptPriorityClass.Value))
		gomega.Ω(*pod.Spec.PreemptionPolicy).Should(gomega.Equal(*testPreemptPriorityClass.PreemptionPolicy))

		value, ok := pod.Annotations[constants.AnnotationAllowPreemption]
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(value).Should(gomega.Equal(constants.True))
	})

	ginkgo.It("Verifying pod with non-preempt priority class", func() {
		ginkgo.By("has correct properties set")
		podCopy := testPod.DeepCopy()
		podCopy.Name = nonExistentNode
		podCopy.Spec.PriorityClassName = testNonPreemptPriorityClass.Name
		podCopy.Spec.NodeName = nonExistentNode
		pod, err := kubeClient.CreatePod(podCopy, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		gomega.Ω(*pod.Spec.Priority).Should(gomega.Equal(testNonPreemptPriorityClass.Value))
		gomega.Ω(*pod.Spec.PreemptionPolicy).Should(gomega.Equal(*testNonPreemptPriorityClass.PreemptionPolicy))

		value, ok := pod.Annotations[constants.AnnotationAllowPreemption]
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(value).Should(gomega.Equal(constants.False))
	})

	ginkgo.It("Verifying pod with non-YK priority class", func() {
		ginkgo.By("has correct properties set")
		podCopy := testPod.DeepCopy()
		podCopy.Name = "non-yk-pod"
		podCopy.Spec.PriorityClassName = testNonYkPriorityClass.Name
		podCopy.Spec.NodeName = nonExistentNode
		pod, err := kubeClient.CreatePod(podCopy, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		gomega.Ω(*pod.Spec.Priority).Should(gomega.Equal(testNonYkPriorityClass.Value))
		gomega.Ω(*pod.Spec.PreemptionPolicy).Should(gomega.Equal(*testNonYkPriorityClass.PreemptionPolicy))

		value, ok := pod.Annotations[constants.AnnotationAllowPreemption]
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(value).Should(gomega.Equal(constants.True))
	})

	ginkgo.It("Verifying pod with no priority class", func() {
		ginkgo.By("has correct properties set")
		podCopy := testPod.DeepCopy()
		podCopy.Name = "no-priority"
		podCopy.Spec.NodeName = nonExistentNode
		pod, err := kubeClient.CreatePod(podCopy, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		value, ok := pod.Annotations[constants.AnnotationAllowPreemption]
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(value).Should(gomega.Equal(constants.True))
	})

	ginkgo.It("Verifying a pod is created in the test namespace", func() {
		ginkgo.By("has 1 running pod whose SchedulerName is yunikorn")
		pod, err := kubeClient.CreatePod(&testPod, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		// Wait for pod to move into running state
		err = kubeClient.WaitForPodBySelectorRunning(ns,
			fmt.Sprintf("app=%s", appName), 10)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(pod.Spec.SchedulerName).Should(gomega.BeEquivalentTo(constants.SchedulerName))
	})

	ginkgo.It("Verifying a pod is created in the bypass namespace", func() {
		ginkgo.By("Create a pod in the bypass namespace")
		pod, err := kubeClient.CreatePod(&testPod, bypassNs)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, bypassNs)

		err = kubeClient.WaitForPodBySelectorRunning(bypassNs,
			fmt.Sprintf("app=%s", appName), 10)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(pod.Spec.SchedulerName).ShouldNot(gomega.BeEquivalentTo(constants.SchedulerName))

	})

	ginkgo.It("Verifying the scheduler configuration is overridden", func() {
		invalidConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: make(map[string]string),
		}

		res, err := restClient.ValidateSchedulerConfig(invalidConfigMap)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(res.Allowed).Should(gomega.BeEquivalentTo(false))
	})

	ginkgo.It("Configure the scheduler with an empty configmap", func() {
		emptyConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: make(map[string]string),
		}
		cm, err := kubeClient.UpdateConfigMap(&emptyConfigMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(cm).ShouldNot(gomega.BeNil())
	})

	ginkgo.It("Configure the scheduler with invalid configmap", func() {
		invalidConfigMap := v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.ConfigMapName,
				Namespace: configmanager.YuniKornTestConfig.YkNamespace,
			},
			Data: map[string]string{"queues.yaml": "invalid"},
		}
		invalidCm, err := kubeClient.UpdateConfigMap(&invalidConfigMap, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).Should(gomega.HaveOccurred())
		gomega.Ω(invalidCm).ShouldNot(gomega.BeNil())
	})

	ginkgo.It("Check that annotation is added to a pod & cannot be modified", func() {
		ginkgo.By("Create a pod")
		pod, err := kubeClient.CreatePod(&testPod, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer deletePod(pod, ns)

		err = kubeClient.WaitForPodBySelector(ns,
			fmt.Sprintf("app=%s", appName), 10*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		pod, err = kubeClient.GetPod(pod.Name, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		userinfo := pod.Annotations[constants.DomainYuniKorn+"user.info"]
		gomega.Ω(userinfo).Should(gomega.Not(gomega.BeNil()))

		ginkgo.By("Attempt to update userinfo annotation")
		_, err = kubeClient.UpdatePodWithAnnotation(pod, ns, constants.DomainYuniKorn+"user.info", "shouldnotsucceed")
		gomega.Ω(err).Should(gomega.HaveOccurred())
	})

	ginkgo.It("Check that annotation is added to a deployment", func() {
		create := func() (string, error) {
			dep, err := kubeClient.CreateDeployment(&testDeployment, ns)
			name := ""
			if dep != nil {
				name = dep.Name
			}
			return name, err
		}

		runWorkloadTest(k8s.Deployment, create, defaultPodTimeout)
	})

	ginkgo.It("Check that annotation is added to a StatefulSet", func() {
		create := func() (string, error) {
			sfs, err := kubeClient.CreateStatefulSet(&testStatefulSet, ns)
			name := ""
			if sfs != nil {
				name = sfs.Name
			}
			return name, err
		}

		runWorkloadTest(k8s.StatefulSet, create, defaultPodTimeout)
	})

	ginkgo.It("Check that annotation is added to a DaemonSet", func() {
		create := func() (string, error) {
			ds, err := kubeClient.CreateDaemonSet(&testDaemonSet, ns)
			name := ""
			if ds != nil {
				name = ds.Name
			}
			return name, err
		}

		runWorkloadTest(k8s.DaemonSet, create, defaultPodTimeout)
	})

	ginkgo.It("Check that annotation is added to a ReplicaSet", func() {
		create := func() (string, error) {
			rs, err := kubeClient.CreateReplicaSet(&testReplicaSet, ns)
			name := ""
			if rs != nil {
				name = rs.Name
			}
			return name, err
		}

		runWorkloadTest(k8s.ReplicaSet, create, defaultPodTimeout)
	})

	ginkgo.It("Check that annotation is added to a Job", func() {
		create := func() (string, error) {
			job, err := kubeClient.CreateJob(&testJob, ns)
			name := ""
			if job != nil {
				name = job.Name
			}
			return name, err
		}

		runWorkloadTest(k8s.Job, create, defaultPodTimeout)
	})

	ginkgo.It("Check that annotation is added to a CronJob", func() {
		create := func() (string, error) {
			cj, err := kubeClient.CreateCronJob(&testCronJob, ns)
			name := ""
			if cj != nil {
				name = cj.Name
			}
			return name, err
		}

		runWorkloadTest(k8s.CronJob, create, cronJobPodTimeout)
	})

	ginkgo.It("Check that deployment is rejected when controller users are not trusted", func() {
		ginkgo.By("Retrieve existing configmap")
		configMap, err := kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data[amConf.AMAccessControlTrustControllers] = "false"
		ginkgo.By("Update configmap")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
			gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.By("Create a deployment")
		deployment, err2 := kubeClient.CreateDeployment(&testDeployment, ns)
		gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
		defer kubeClient.DeleteWorkloadAndPods(deployment.Name, k8s.Deployment, ns)

		// pod is not expected to appear
		ginkgo.By("Check for sleep pods (should time out)")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			10*time.Second)
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error: %v\n", err)
		gomega.Ω(err).Should(gomega.HaveOccurred())
		ginkgo.By("Check deployment status")
		deployment, err = kubeClient.GetDeployment(testDeployment.Name, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		fmt.Fprintf(ginkgo.GinkgoWriter, "Replicas: %d, AvailableReplicas: %d, ReadyReplicas: %d\n",
			deployment.Status.Replicas, deployment.Status.AvailableReplicas, deployment.Status.ReadyReplicas)
		gomega.Ω(deployment.Status.Replicas).To(gomega.Equal(int32(0)))
		gomega.Ω(deployment.Status.AvailableReplicas).To(gomega.Equal(int32(0)))
		gomega.Ω(deployment.Status.ReadyReplicas).To(gomega.Equal(int32(0)))

		// restore setting
		ginkgo.By("Restore trustController setting")
		configMap, err = kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		configMap.Data[amConf.AMAccessControlTrustControllers] = "true"
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
			gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		})

		// pod is expected to appear
		ginkgo.By("Check for sleep pod")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			60*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("Check that deployment is rejected when external user is not trusted", func() {
		ginkgo.By("Retrieve existing configmap")
		configMap, err := kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data[amConf.AMAccessControlExternalUsers] = ""
		ginkgo.By("Update configmap")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
			gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.By("Create a deployment")
		deployment := testDeployment.DeepCopy()
		deployment.Spec.Template.Annotations = make(map[string]string)
		deployment.Spec.Template.Annotations[userInfoAnnotation] = "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}"
		_, err = kubeClient.CreateDeployment(deployment, ns)
		fmt.Fprintf(ginkgo.GinkgoWriter, "Error received from API server: %v\n", err)
		gomega.Ω(err).Should(gomega.HaveOccurred())
		gomega.Ω(err).To(gomega.BeAssignableToTypeOf(&errors.StatusError{}))

		// modify setting
		ginkgo.By("Changing allowed externalUser setting")
		configMap, err = kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		configMap.Data[amConf.AMAccessControlExternalUsers] = "(^minikube-user$|^kubernetes-admin$)" // works with Minikube & KIND
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
			gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		})

		// submit deployment again
		ginkgo.By("Submit deployment again")
		_, err = kubeClient.CreateDeployment(deployment, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer kubeClient.DeleteWorkloadAndPods(deployment.Name, k8s.Deployment, ns)

		// pod is expected to appear
		ginkgo.By("Check for sleep pod")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			60*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("Check that replicaset is not modified if submitted by system user", func() {
		ginkgo.By("Retrieve existing configmap")
		configMap, err := kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}
		configMap.Data[amConf.AMAccessControlBypassAuth] = "true"
		ginkgo.By("Update configmap (bypassAuth -> true)")
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
			gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.By("Submit a deployment")
		deployment := testDeployment.DeepCopy()
		_, err = kubeClient.CreateDeployment(deployment, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		defer kubeClient.DeleteWorkloadAndPods(deployment.Name, k8s.Deployment, ns)
		ginkgo.By("Check for sleep pod")
		err = kubeClient.WaitForPodBySelector(ns, fmt.Sprintf("app=%s", deployment.ObjectMeta.Labels["app"]),
			60*time.Second)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Update configmap (bypassAuth -> false)")
		configMap, err = kubeClient.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		configMap.Data[amConf.AMAccessControlBypassAuth] = "false"
		// The wait wrapper still can't fully guarantee that the config in AdmissionController has been updated.
		yunikorn.WaitForAdmissionControllerRefreshConfAfterAction(func() {
			_, err = kubeClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
			gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.By("Update container image in deployment")
		deployment, err = kubeClient.GetDeployment(deployment.Name, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
		container := deployment.Spec.Template.Spec.Containers[0]
		container.Image = "nginx:1.16.1"
		deployment.Spec.Template.Spec.Containers[0] = container
		_, err = kubeClient.UpdateDeployment(deployment, ns)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Wait for sleep pod")
		err = kubeClient.WaitForPodBySelectorRunning(ns, fmt.Sprintf("app=%s", testDeployment.ObjectMeta.Labels["app"]),
			60)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

		ginkgo.By("Check for number of replicasets")
		replicaSetList, err2 := kubeClient.GetReplicaSets(ns)
		gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
		for _, rs := range replicaSetList.Items {
			fmt.Fprintf(ginkgo.GinkgoWriter, "%-20s\tReplicas: %d\tReady: %d\tAvailable: %d\n",
				rs.Name,
				rs.Status.Replicas,
				rs.Status.ReadyReplicas,
				rs.Status.AvailableReplicas)
		}
		gomega.Ω(replicaSetList.Items).To(gomega.HaveLen(2))
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{ns})

		ginkgo.By("Tear down namespace: " + ns)
		err := kubeClient.TearDownNamespace(ns)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// call the healthCheck api to check scheduler health
		ginkgo.By("Check YuniKorn's health")
		checks, err2 := yunikorn.GetFailedHealthChecks()
		gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
		gomega.Ω(checks).Should(gomega.Equal(""), checks)
	})
})

func runWorkloadTest(workloadType k8s.WorkloadType, create func() (string, error),
	podTimeout time.Duration) {
	ginkgo.By("Create a " + string(workloadType))
	name, err := create()
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	defer kubeClient.DeleteWorkloadAndPods(name, workloadType, ns)
	err = kubeClient.WaitForPodBySelector(ns, "app="+appName, podTimeout)
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

	ginkgo.By("Get at least one running pod")
	var pods *v1.PodList
	pods, err = kubeClient.GetPods(ns)
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	fmt.Fprintf(ginkgo.GinkgoWriter, "Running pod is %s\n", pods.Items[0].Name)
	pod, err2 := kubeClient.GetPod(pods.Items[0].Name, ns)
	gomega.Ω(err2).ShouldNot(gomega.HaveOccurred())
	userinfo := pod.Annotations[constants.DomainYuniKorn+"user.info"]
	gomega.Ω(userinfo).Should(gomega.Not(gomega.BeNil()))
}

func deletePod(pod *v1.Pod, namespace string) {
	if pod != nil {
		ginkgo.By("Delete pod " + pod.Name)
		err := kubeClient.DeletePod(pod.Name, namespace)
		gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
	}
}
