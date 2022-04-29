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
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/onsi/ginkgo"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

// variable populated in BeforeEach, never modified afterwards
var workerNodes []string

func getNodeThatCanRunPodWithoutToleration(k *k8s.KubeCtl, namespace string) string {
	By("Trying to launch a pod without a toleration to get a node which can launch it.")
	return runPodAndGetNodeName(k, k8s.SleepPodConfig{Name: "without-toleration", NS: namespace})
}

// GetNodeThatCanRunPod trying to launch a pod without a label to get a node which can launch it
func GetNodeThatCanRunPod(k *k8s.KubeCtl, namespace string) string {
	By("Trying to launch a pod without a label to get a node which can launch it.")
	return runPodAndGetNodeName(k, k8s.SleepPodConfig{Name: "without-toleration", NS: namespace})
}

func runPodAndGetNodeName(k *k8s.KubeCtl, conf k8s.SleepPodConfig) string {
	// launch a pod to find a node which can launch a pod. We intentionally do
	// not just take the node list and choose the first of them. Depending on the
	// cluster and the scheduler it might be that a "normal" pod cannot be
	// scheduled onto it.
	pod := runTestPod(k, conf)

	By("Explicitly delete pod here to free the resource it takes.")
	err := k.DeletePod(pod.Name, pod.Namespace)
	Ω(err).NotTo(HaveOccurred())
	return pod.Spec.NodeName
}

func runTestPod(k *k8s.KubeCtl, conf k8s.SleepPodConfig) *v1.Pod {
	initPod, podErr := k8s.InitSleepPod(conf)
	Ω(podErr).NotTo(HaveOccurred())
	pod, err := k.CreatePod(initPod, conf.NS)
	Ω(err).NotTo(HaveOccurred())
	Ω(k.WaitForPodRunning(pod.Namespace, pod.Name, time.Duration(60)*time.Second)).NotTo(HaveOccurred())
	pod1, err := k.GetPod(pod.Name, pod.Namespace)
	Ω(err).NotTo(HaveOccurred())
	return pod1
}

var _ = Describe("Predicates", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var err error
	var ns, anotherNS string
	var nodeList *v1.NodeList
	const LABELVALUE = "testing-label-value"

	BeforeEach(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}
		nodeList = &v1.NodeList{}
		nodeList, err = e2enode.GetReadySchedulableNodes(kClient.GetClient())
		Ω(err).NotTo(HaveOccurred(), fmt.Sprintf("Unexpected error occurred: %v", err))
		for _, n := range nodeList.Items {
			workerNodes = append(workerNodes, n.Name)
		}
		ns = "test-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace", ns))
		ns1, err1 := kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))

		anotherNS = "test-" + common.RandSeq(10)
		By(fmt.Sprintf("create %s namespace", anotherNS))
		ns2, err2 := kClient.CreateNamespace(anotherNS, nil)
		Ω(err2).NotTo(HaveOccurred())
		Ω(ns2.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	AfterEach(func() {
		By("Cleanup")
		for _, n := range []string{ns, anotherNS} {
			ginkgo.By("Tear down namespace: " + n)
			err = kClient.TearDownNamespace(n)
			Ω(err).NotTo(HaveOccurred())
		}
	})

	// Test Nodes does not have any label, hence it should be impossible to schedule Pod with
	// nonempty Selector set.
	/*
		Testname: Yunikorn Scheduler, node selector not matching
		Description:
			1. Create a Pod with a NodeSelector set to a value that does not match a node in the cluster.
			2. Since there are no nodes matching the criteria the Pod MUST not be scheduled.
	*/
	It("Verify_Non_Matching_NodeSelector_Respected", func() {
		By("Trying to schedule Pod with nonempty NodeSelector.")
		podName := "blocked-pod"

		conf := k8s.TestPodConfig{
			Name: podName,
			Labels: map[string]string{
				"name":          "blocked",
				"app":           "blocked-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			NodeSelector: map[string]string{
				"label": "nonempty",
			},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, podErr = kClient.CreatePod(initPod, ns)
		Ω(podErr).NotTo(HaveOccurred())

		By(fmt.Sprintf("Verify pod:%s is in pending state", podName))
		podErr = kClient.WaitForPodPending(ns, podName, time.Duration(60)*time.Second)
		Ω(podErr).NotTo(HaveOccurred())

		By("Verify the YuniKorn request failed scheduling")

		podErr = restClient.WaitForAllocationLog("default", "root."+ns, initPod.ObjectMeta.Labels["applicationId"], podName, 60)
		Ω(podErr).NotTo(HaveOccurred())
		log, podErr := restClient.GetAllocationLog("default", "root."+ns, initPod.ObjectMeta.Labels["applicationId"], podName)
		Ω(podErr).NotTo(HaveOccurred())
		Ω(log).NotTo(BeNil(), "Log can't be empty")
		logEntries := yunikorn.AllocLogToStrings(log)
		Ω(logEntries).To(ContainElement(MatchRegexp(".*didn't match Pod's node affinity")), "Log entry message mismatch")
	})

	/*
		Testname: Yunikorn Scheduler, node selector matching
		Description:
			1. Create a label on the node {key: value}.
			2. Then create a Pod with a NodeSelector set to {key: value}.
			3. Check to see if the Pod is scheduled on that node by YK scheduler.
	*/
	It("Verify_Matching_NodeSelector_Respected", func() {
		nodeName := GetNodeThatCanRunPod(&kClient, ns)
		By("Trying to apply a random label on the found node")
		key := fmt.Sprintf("kubernetes.io/e2e-%s", common.RandSeq(10))
		value := "101"
		framework.AddOrUpdateLabelOnNode(kClient.GetClient(), nodeName, key, value)
		framework.ExpectNodeHasLabel(kClient.GetClient(), nodeName, key, value)
		defer framework.RemoveLabelOffNode(kClient.GetClient(), nodeName, key)

		By("Trying to launch the pod, now with labels.")
		labelPodName := "with-labels"
		conf := k8s.TestPodConfig{
			Name:      labelPodName,
			Namespace: ns,
			Labels: map[string]string{
				"app":           "blocked-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			NodeSelector: map[string]string{
				key: value,
			},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(ns, labelPodName, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

		labelPod, err1 := kClient.GetPod(labelPodName, ns)
		Ω(err1).NotTo(HaveOccurred())
		Ω(labelPod.Spec.NodeName).Should(Equal(nodeName))
	})

	// Tests for Node Affinity
	It("Verify_Non_Matching_NodeAffinity_Respected", func() {
		By("Trying to schedule Pod with nonempty NodeAffinity.")
		podName := "blocked-pod"

		conf := k8s.TestPodConfig{
			Name: podName,
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "foo",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"bar", "value2"},
									},
								},
							}, {
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "diffkey",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"wrong", "value2"},
									},
								},
							},
						},
					},
				},
			},
			Labels: map[string]string{
				"name":          "blocked",
				"app":           "blocked-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, podErr = kClient.CreatePod(initPod, ns)
		Ω(podErr).NotTo(HaveOccurred())

		By(fmt.Sprintf("Verify pod:%s is in pending state", podName))
		podErr = kClient.WaitForPodPending(ns, podName, time.Duration(60)*time.Second)
		Ω(podErr).NotTo(HaveOccurred())

		By("Verify the YuniKorn request failed scheduling")

		podErr = restClient.WaitForAllocationLog("default", "root."+ns, initPod.ObjectMeta.Labels["applicationId"], podName, 60)
		Ω(podErr).NotTo(HaveOccurred())
		log, podErr := restClient.GetAllocationLog("default", "root."+ns, initPod.ObjectMeta.Labels["applicationId"], podName)
		Ω(podErr).NotTo(HaveOccurred())
		Ω(log).NotTo(BeNil(), "Log can't be empty")
		logEntries := yunikorn.AllocLogToStrings(log)
		Ω(logEntries).To(ContainElement(MatchRegexp(".*didn't match Pod's node affinity")), "Log entry message mismatch")
	})

	It("Verify_Matching_NodeAffinity_Respected", func() {
		nodeName := GetNodeThatCanRunPod(&kClient, ns)
		By("Trying to apply a random label on the found node")
		key := fmt.Sprintf("kubernetes.io/e2e-%s", common.RandSeq(10))
		value := "102"
		framework.AddOrUpdateLabelOnNode(kClient.GetClient(), nodeName, key, value)
		framework.ExpectNodeHasLabel(kClient.GetClient(), nodeName, key, value)
		defer framework.RemoveLabelOffNode(kClient.GetClient(), nodeName, key)

		By("Trying to launch the pod, now with labels.")
		labelPodName := "with-labels"
		conf := k8s.TestPodConfig{
			Name:      labelPodName,
			Namespace: ns,
			Labels: map[string]string{
				"app":           "blocked-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      key,
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{value},
									},
								},
							},
						},
					},
				},
			},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(ns, labelPodName, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

		labelPod, err1 := kClient.GetPod(labelPodName, ns)
		Ω(err1).NotTo(HaveOccurred())
		Ω(labelPod.Spec.NodeName).Should(Equal(nodeName))
	})

	// Tests for Taints & Tolerations
	It("Verify_Matching_Taint_Tolerations_Respected", func() {
		nodeName := getNodeThatCanRunPodWithoutToleration(&kClient, ns)
		By("Trying to apply a random taint on the found node.")
		testTaint := &v1.Taint{
			Key:    fmt.Sprintf("kubernetes.io/e2e-taint-key-%s", common.RandSeq(10)),
			Value:  "testing-taint-value",
			Effect: v1.TaintEffectNoSchedule,
		}
		err = controller.AddOrUpdateTaintOnNode(context.Background(), kClient.GetClient(), nodeName, testTaint)
		Ω(err).NotTo(HaveOccurred())
		framework.ExpectNodeHasTaint(kClient.GetClient(), nodeName, testTaint)
		defer func(c kubernetes.Interface, nodeName string, taint *v1.Taint) {
			err = controller.RemoveTaintOffNode(context.Background(), c, nodeName, nil, taint)
			Ω(err).NotTo(HaveOccurred())
		}(kClient.GetClient(), nodeName, testTaint)

		ginkgo.By("Trying to apply a random label on the found node.")
		labelKey := fmt.Sprintf("kubernetes.io/e2e-label-key-%s", common.RandSeq(10))
		labelValue := LABELVALUE
		framework.AddOrUpdateLabelOnNode(kClient.GetClient(), nodeName, labelKey, labelValue)
		framework.ExpectNodeHasLabel(kClient.GetClient(), nodeName, labelKey, labelValue)
		defer framework.RemoveLabelOffNode(kClient.GetClient(), nodeName, labelKey)

		ginkgo.By("Trying to relaunch the pod, now with tolerations.")
		tolerationPodName := "with-tolerations"
		conf := k8s.TestPodConfig{
			Name:      tolerationPodName,
			Namespace: ns,
			Labels: map[string]string{
				"app":           "tolerations-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			Tolerations:  []v1.Toleration{{Key: testTaint.Key, Value: testTaint.Value, Effect: testTaint.Effect}},
			NodeSelector: map[string]string{labelKey: labelValue},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(ns, tolerationPodName, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

		labelPod, err1 := kClient.GetPod(tolerationPodName, ns)
		Ω(err1).NotTo(HaveOccurred())
		Ω(labelPod.Spec.NodeName).Should(Equal(nodeName))
	})

	It("Verify_Not_Matching_Taint_Tolerations_Respected", func() {
		nodeName := getNodeThatCanRunPodWithoutToleration(&kClient, ns)
		By("Trying to apply a random taint on the found node.")
		testTaint := &v1.Taint{
			Key:    fmt.Sprintf("kubernetes.io/e2e-taint-key-%s", common.RandSeq(10)),
			Value:  "testing-taint-value",
			Effect: v1.TaintEffectNoSchedule,
		}
		err = controller.AddOrUpdateTaintOnNode(context.Background(), kClient.GetClient(), nodeName, testTaint)
		Ω(err).NotTo(HaveOccurred())

		framework.ExpectNodeHasTaint(kClient.GetClient(), nodeName, testTaint)
		defer func(c kubernetes.Interface, nodeName string, taint *v1.Taint) {
			err = controller.RemoveTaintOffNode(context.Background(), c, nodeName, nil, taint)
			Ω(err).NotTo(HaveOccurred())
		}(kClient.GetClient(), nodeName, testTaint)

		ginkgo.By("Trying to apply a random label on the found node.")
		labelKey := fmt.Sprintf("kubernetes.io/e2e-label-key-%s", common.RandSeq(10))
		labelValue := LABELVALUE
		framework.AddOrUpdateLabelOnNode(kClient.GetClient(), nodeName, labelKey, labelValue)
		framework.ExpectNodeHasLabel(kClient.GetClient(), nodeName, labelKey, labelValue)
		defer framework.RemoveLabelOffNode(kClient.GetClient(), nodeName, labelKey)

		ginkgo.By("Trying to relaunch the pod with no tolerations.")
		podNameNoTolerations := "with-no-tolerations"
		conf := k8s.TestPodConfig{
			Name:      podNameNoTolerations,
			Namespace: ns,
			Labels: map[string]string{
				"app":           "no-tolerations-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			NodeSelector: map[string]string{labelKey: labelValue},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err := kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Verify pod:%s is in pending state", podNameNoTolerations))
		err = kClient.WaitForPodPending(ns, podNameNoTolerations, time.Duration(60)*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Verify the YuniKorn request failed scheduling")

		err = restClient.WaitForAllocationLog("default", "root."+ns, initPod.ObjectMeta.Labels["applicationId"], podNameNoTolerations, 60)
		Ω(err).NotTo(HaveOccurred())
		log, err := restClient.GetAllocationLog("default", "root."+ns, initPod.ObjectMeta.Labels["applicationId"], podNameNoTolerations)
		Ω(err).NotTo(HaveOccurred())
		Ω(log).NotTo(BeNil(), "Log can't be empty")
		logEntries := yunikorn.AllocLogToStrings(log)
		Ω(logEntries).To(ContainElement(MatchRegexp(".*taint.*")), "Log entry message mismatch")

		// Remove taint off the node and verify the pod is scheduled on node.
		err = controller.RemoveTaintOffNode(context.Background(), kClient.GetClient(), nodeName, nil, testTaint)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(ns, podNameNoTolerations, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

		labelPod, err := kClient.GetPod(podNameNoTolerations, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(labelPod.Spec.NodeName).Should(Equal(nodeName))
	})

	// Tests for Pod Affinity & Anti Affinity
	Context("PodAffinity & AntiAffinity Tests", func() {
		type PodAffinityStruct struct {
			name string
			pod  *v1.Pod
			pods []*v1.Pod
			fits bool
		}

		podLabel := map[string]string{"service": "securityscan"}
		podLabel2 := map[string]string{"security": "S1"}
		var nodeName, labelKey string
		sleepContainer := v1.Container{Name: "test-container", Image: "alpine:latest", Command: []string{"sleep", "60"}}

		labelKey = "kubernetes.io/hostname"

		BeforeEach(func() {
			By("Finding a node that can fit the pods")
			nodeName = GetNodeThatCanRunPod(&kClient, ns)
		})

		DescribeTable("", func(t PodAffinityStruct) {
			By("Launching the pods with labels")
			for _, pod := range t.pods {
				pod.Labels["app"] = common.RandSeq(5)
				pod.Labels["applicationId"] = common.RandSeq(10)
				createdPod, err := kClient.CreatePod(pod, ns)
				Ω(err).NotTo(HaveOccurred())

				err = kClient.WaitForPodScheduled(createdPod.Namespace, createdPod.Name, time.Duration(60)*time.Second)
				Ω(err).NotTo(HaveOccurred())
			}

			By("Launching the pod with Pod (anti)-affinity")
			t.pod.Labels["app"] = common.RandSeq(5)
			t.pod.Labels["applicationId"] = common.RandSeq(10)
			testPod, err := kClient.CreatePod(t.pod, ns)
			Ω(err).NotTo(HaveOccurred())
			var err1 error
			if t.fits {
				err1 = kClient.WaitForPodScheduled(testPod.Namespace, testPod.Name, time.Duration(60)*time.Second)
			} else {
				err1 = kClient.WaitForPodUnschedulable(testPod, time.Duration(60)*time.Second)
			}
			Ω(err1).NotTo(HaveOccurred())
		},
			Entry("Verify_Not_Matching_Inter_Pod-Affinity_Respected",
				PodAffinityStruct{
					name: "Verify_Not_Matching_Inter_Pod-Affinity_Respected",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					fits: false,
				}),
			Entry("Verify_Matching_Inter_Pod-Affinity_Respected",
				PodAffinityStruct{
					name: "Verify_Matching_Inter-Pod-Affinity_Respected",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename2",
							Labels: podLabel,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							NodeName:      nodeName,
						},
					},
					},
					fits: true,
				}),
			Entry("Verify_Matching_Inter_Pod-Affinity_Respected_For_NotIn_Operator",
				PodAffinityStruct{
					name: "Verify_Matching_Inter-Pod-Affinity_Respected_For_NotIn_Operator",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpNotIn,
														Values:   []string{"securityscan3", "value3"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName},
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename2",
							Labels: podLabel}}},
					fits: true,
				}),
			Entry("Verify_Matching_Inter-Pod-Affinity_Under_Diff_NS",
				PodAffinityStruct{
					name: "validates that inter-pod-affinity is respected when pods have different Namespaces",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
											Namespaces:  []string{"diff-namespace"},
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "fakename2",
							Labels:    podLabel,
							Namespace: anotherNS}}},
					fits: false,
				}),
			Entry("Verify_Inter-Pod-Affinity_With_UnMatching_LabelSelector",
				PodAffinityStruct{
					name: "Doesn't satisfy the PodAffinity because of unmatching labelSelector with the existing pod",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"antivirusscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName}, ObjectMeta: metav1.ObjectMeta{
						Name:   "fakename2",
						Labels: podLabel}}},
					fits: false,
				}),
			/*If you specify multiple nodeSelectorTerms associated with nodeAffinity types,
			then the pod can be scheduled onto a node if one of the nodeSelectorTerms can be satisfied.

			If you specify multiple matchExpressions associated with nodeSelectorTerms,
			then the pod can be scheduled onto a node only if all matchExpressions is satisfied.
			*/
			Entry("Verify_Inter-Pod-Affinity_With_Multiple_Affinities",
				PodAffinityStruct{
					name: "validates that InterPodAffinity is respected if matching with multiple affinities in multiple RequiredDuringSchedulingIgnoredDuringExecution ",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpExists,
													}, {
														Key:      "wrongkey",
														Operator: metav1.LabelSelectorOpDoesNotExist,
													},
												},
											},
											TopologyKey: labelKey,
										}, {
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan"},
													}, {
														Key:      "service",
														Operator: metav1.LabelSelectorOpNotIn,
														Values:   []string{"WrongValue"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName}, ObjectMeta: metav1.ObjectMeta{
						Name:   "fakename2",
						Labels: podLabel}}},
					fits: true,
				}),
			Entry("Verify_Pod_Affinity_With_One_UnMatched_Expression",
				PodAffinityStruct{
					name: "The labelSelector requirements(items of matchExpressions) are ANDed, the pod cannot schedule onto the node because one of the matchExpression items doesn't match.",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Labels: podLabel2,
							Name:   "fakename",
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpExists,
													}, {
														Key:      "wrongkey",
														Operator: metav1.LabelSelectorOpDoesNotExist,
													},
												},
											},
											TopologyKey: labelKey,
										}, {
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan2"},
													}, {
														Key:      "service",
														Operator: metav1.LabelSelectorOpNotIn,
														Values:   []string{"WrongValue"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName}, ObjectMeta: metav1.ObjectMeta{
						Name:   "fakename2",
						Labels: podLabel}}},
					fits: false,
				}),
			Entry("Verfiy_Matched_Pod_Affinity_Anti_Affinity_Respected",
				PodAffinityStruct{
					name: "validates that InterPod Affinity and AntiAffinity is respected if matching",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
								PodAntiAffinity: &v1.PodAntiAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"antivirusscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName}, ObjectMeta: metav1.ObjectMeta{
						Name:   "fakename2",
						Labels: podLabel}}},
					fits: true,
				}),
			Entry("Verify_Matched_Pod_Affinity_Unmatched_Pod_Antiaffinity",
				PodAffinityStruct{
					name: "satisfies the PodAffinity but doesn't satisfies the PodAntiAffinity with the existing pod",
					pod: &v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "fakename",
							Labels: podLabel2,
						},
						Spec: v1.PodSpec{
							SchedulerName: configmanager.SchedulerName,
							Containers:    []v1.Container{sleepContainer},
							Affinity: &v1.Affinity{
								PodAffinity: &v1.PodAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
								PodAntiAffinity: &v1.PodAntiAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
										{
											LabelSelector: &metav1.LabelSelector{
												MatchExpressions: []metav1.LabelSelectorRequirement{
													{
														Key:      "service",
														Operator: metav1.LabelSelectorOpIn,
														Values:   []string{"securityscan", "value2"},
													},
												},
											},
											TopologyKey: labelKey,
										},
									},
								},
							},
						},
					},
					pods: []*v1.Pod{{Spec: v1.PodSpec{
						SchedulerName: configmanager.SchedulerName,
						Containers:    []v1.Container{sleepContainer},
						NodeName:      nodeName}, ObjectMeta: metav1.ObjectMeta{
						Name:   "fakename2",
						Labels: podLabel}}},
					fits: false,
				}),
		)
	})

	// Tests for PodFitsHost
	It("Verify_PodFitsHost_Respected", func() {
		nodeName := GetNodeThatCanRunPod(&kClient, ns)

		By("Trying to launch the pod, now with labels.")
		PodName := "test-pod"
		conf := k8s.TestPodConfig{
			Name:     PodName,
			NodeName: nodeName,
			Labels: map[string]string{
				"app":           "podFitsHost-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			Namespace: ns,
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err := kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(ns, PodName, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

		labelPod, err := kClient.GetPod(PodName, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(labelPod.Spec.NodeName).Should(Equal(nodeName))
	})

	// Tests for PodFitsHostPorts
	It("Verify_No_Conflict_With_Same_HostPort_Diff_IP_Protocol", func() {
		nodeName := GetNodeThatCanRunPod(&kClient, ns)

		By("Trying to apply a random label on the found node")
		key := fmt.Sprintf("kubernetes.io/e2e-hostport-%s", common.RandSeq(10))
		value := "103"
		framework.AddOrUpdateLabelOnNode(kClient.GetClient(), nodeName, key, value)
		framework.ExpectNodeHasLabel(kClient.GetClient(), nodeName, key, value)
		defer framework.RemoveLabelOffNode(kClient.GetClient(), nodeName, key)

		port := int32(54321)
		portMap := [][]interface{}{
			{"127.0.0.1", v1.ProtocolTCP},
			{"127.0.0.2", v1.ProtocolTCP},
			{"127.0.0.2", v1.ProtocolUDP},
		}
		for _, p := range portMap {
			s := reflect.ValueOf(p)
			Port := port
			Protocol, success := s.Index(1).Interface().(v1.Protocol)
			Ω(success).To(BeTrue())
			HostIP, success := s.Index(0).Interface().(string)
			Ω(success).To(BeTrue())
			labelPodName := "same-hostport-" + common.RandSeq(10)
			By(fmt.Sprintf("Trying to create a pod(%s) with hostport %v, protocol %v and hostIP %v and expect scheduled", labelPodName, Port, Protocol, HostIP))
			conf := k8s.TestPodConfig{
				Name:      labelPodName,
				Namespace: ns,
				Labels: map[string]string{
					"app":           "sameHostPort-app-" + common.RandSeq(5),
					"applicationId": common.RandSeq(10),
				},
				NodeSelector: map[string]string{
					key: value,
				},
				Ports: []v1.ContainerPort{
					{
						HostPort:      Port,
						ContainerPort: 80,
						Protocol:      Protocol,
						HostIP:        HostIP,
					},
				},
			}

			initPod, podErr := k8s.InitTestPod(conf)
			Ω(podErr).NotTo(HaveOccurred())
			_, err := kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			Ω(kClient.WaitForPodRunning(ns, labelPodName, time.Duration(60)*time.Second)).NotTo(HaveOccurred())
		}
	})

	It("Verify_Conflict_With_Same_HostPort_Same_IP_Protocol", func() {
		nodeName := GetNodeThatCanRunPod(&kClient, ns)

		By("Trying to apply a random label on the found node")
		key := fmt.Sprintf("kubernetes.io/e2e-hostport-%s", common.RandSeq(10))
		value := "104"
		framework.AddOrUpdateLabelOnNode(kClient.GetClient(), nodeName, key, value)
		framework.ExpectNodeHasLabel(kClient.GetClient(), nodeName, key, value)
		defer framework.RemoveLabelOffNode(kClient.GetClient(), nodeName, key)

		port := int32(54322)
		labelPodName := "same-hostport-" + common.RandSeq(10)
		By(fmt.Sprintf("Trying to create a pod(%s) with hostport %v, protocol %v and hostIP %v and expect scheduled", labelPodName, port, v1.ProtocolTCP, "0.0.0.0"))
		conf := k8s.TestPodConfig{
			Name:      labelPodName,
			Namespace: ns,
			Labels: map[string]string{
				"app":           "sameHostPort-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			NodeSelector: map[string]string{
				key: value,
			},
			Ports: []v1.ContainerPort{
				{
					HostPort:      port,
					ContainerPort: 80,
					Protocol:      v1.ProtocolTCP,
					HostIP:        "",
				},
			},
		}

		initPod, podErr := k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err := kClient.CreatePod(initPod, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPodRunning(ns, labelPodName, time.Duration(60)*time.Second)).NotTo(HaveOccurred())

		labelPodName2 := "same-hostport-" + common.RandSeq(10)
		By(fmt.Sprintf("Trying to create a pod(%s) with hostport %v, protocol %v and hostIP %v and expect not scheduled", labelPodName2, port, v1.ProtocolTCP, "127.0.0.1"))
		conf = k8s.TestPodConfig{
			Name:      labelPodName2,
			Namespace: anotherNS,
			Labels: map[string]string{
				"app":           "sameHostPort-app-" + common.RandSeq(5),
				"applicationId": common.RandSeq(10),
			},
			NodeSelector: map[string]string{
				key: value,
			},
			Ports: []v1.ContainerPort{
				{
					HostPort:      port,
					ContainerPort: 80,
					Protocol:      v1.ProtocolTCP,
					HostIP:        "127.0.0.1",
				},
			},
		}

		initPod, podErr = k8s.InitTestPod(conf)
		Ω(podErr).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(initPod, anotherNS)
		Ω(err).NotTo(HaveOccurred())

		By(fmt.Sprintf("Verify pod:%s is in pending state", labelPodName2))
		err = kClient.WaitForPodPending(anotherNS, labelPodName2, time.Duration(60)*time.Second)
		Ω(err).NotTo(HaveOccurred())

		By("Verify the YuniKorn request failed scheduling")

		err = restClient.WaitForAllocationLog("default", "root."+anotherNS, initPod.ObjectMeta.Labels["applicationId"], labelPodName2, 60)
		Ω(err).NotTo(HaveOccurred())
		log, err := restClient.GetAllocationLog("default", "root."+anotherNS, initPod.ObjectMeta.Labels["applicationId"], labelPodName2)
		Ω(err).NotTo(HaveOccurred())
		Ω(log).NotTo(BeNil(), "Log can't be empty")
		logEntries := yunikorn.AllocLogToStrings(log)
		Ω(logEntries).To(ContainElement(MatchRegexp(".*free ports.*")), "Log entry message mismatch")
	})

	AfterEach(func() {
		By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(HaveOccurred())
		Ω(checks).To(Equal(""), checks)
	})
})
