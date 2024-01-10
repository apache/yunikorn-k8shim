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

package bin_packing

import (
	"fmt"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
)

var _ = Describe("", func() {
	var ns string

	BeforeEach(func() {
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("Create namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	// Verify BinPacking Node Order Memory
	// 1. Sort nodes by increasing available memory: [nodeA, nodeB...]
	// 2. Increase nodeA by 20% of available memory. Add node=nodeA pod label.
	// 3. Increase nodeB by 10% of available memory
	// 4. Submit jobA of 3 pods. Verify all are allocated to nodeA
	// 5. Submit jobB of 3 pods with anti-affinity of node=nodeA. Verify all are allocated to nodeB
	It("Verify_BinPacking_Node_Order_Memory", func() {
		domRes := v1.ResourceMemory
		nodes, err := kClient.GetNodes()
		Ω(err).NotTo(HaveOccurred())

		// Sort nodes by available dominant resource in increasing order
		nodesAvailRes := kClient.GetNodesAvailRes(*nodes)
		sortedWorkerNodes := k8s.GetWorkerNodes(*nodes)
		sort.Slice(sortedWorkerNodes, func(i, j int) bool {
			node1, node2 := sortedWorkerNodes[i].Name, sortedWorkerNodes[j].Name
			q1, q2 := nodesAvailRes[node1][domRes], nodesAvailRes[node2][domRes]
			return q1.Cmp(q2) == -1
		})
		var nodeNames []string
		for _, node := range sortedWorkerNodes {
			nodeNames = append(nodeNames, node.Name)
		}
		By(fmt.Sprintf("Sorted nodes by available memory: %v", nodeNames))

		Ω(len(sortedWorkerNodes)).To(BeNumerically(">=", 2),
			"At least 2 nodes required")

		// Submit pods to nodeA/nodeB to stabilize node order
		padPct := []float64{0.2, 0.1}
		for i := 0; i < 2; i++ {
			// Determine memory padding amount
			nodeName := sortedWorkerNodes[i].Name
			nodeAvailMem := nodesAvailRes[nodeName][domRes]
			memPadding := int64(float64(nodeAvailMem.Value()) * padPct[i])
			podName := fmt.Sprintf("%s-padding", nodeName)

			// Create pod with nodeSelector
			podConf := k8s.TestPodConfig{
				Name:      podName,
				Namespace: ns,
				Labels: map[string]string{
					"node":          nodeName,
					"app":           "app-" + common.RandSeq(5),
					"applicationId": "appid-" + podName,
				},
				NodeSelector: map[string]string{"kubernetes.io/hostname": nodeName},
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("0m"),
						"memory": *resource.NewQuantity(memPadding, resource.BinarySI),
					},
				},
			}

			By(fmt.Sprintf("[%s] Submit pod %s", podConf.Labels["applicationId"], podName))
			initPod, err := k8s.InitTestPod(podConf)
			Ω(err).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(initPod, ns)
			Ω(err).NotTo(HaveOccurred())
			err = kClient.WaitForPodRunning(ns, podName, time.Duration(90)*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}

		// Sort nodes by available dominant resource in increasing order
		nodesAvailRes = kClient.GetNodesAvailRes(*nodes)
		sortedWorkerNodes = k8s.GetWorkerNodes(*nodes)
		sort.Slice(sortedWorkerNodes, func(i, j int) bool {
			node1, node2 := sortedWorkerNodes[i].Name, sortedWorkerNodes[j].Name
			q1, q2 := nodesAvailRes[node1][domRes], nodesAvailRes[node2][domRes]
			return q1.Cmp(q2) == -1
		})
		nodeNames = []string{}
		for _, node := range sortedWorkerNodes {
			nodeNames = append(nodeNames, node.Name)
		}
		By(fmt.Sprintf("Sorted nodes by available memory after padding: %v", nodeNames))

		// JobA Specs
		jobAPodSpec := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-joba-" + common.RandSeq(5),
			},
		}
		jobAConf := k8s.JobConfig{
			Name:        "joba-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(3),
			PodConfig:   jobAPodSpec,
		}

		// JobB Specs with anti-affinity to top node (highly utilised node). Should be scheduled to 2nd most-utilized node.
		jobBPodSpec := k8s.TestPodConfig{
			Labels: map[string]string{
				"app":           "sleep-" + common.RandSeq(5),
				"applicationId": "appid-jobb-" + common.RandSeq(5),
			},
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "node",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{sortedWorkerNodes[0].Name},
									},
								},
							},
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			},
		}
		jobBConf := k8s.JobConfig{
			Name:        "jobb-" + common.RandSeq(5),
			Namespace:   ns,
			Parallelism: int32(3),
			PodConfig:   jobBPodSpec,
		}

		for i, jobConf := range []k8s.JobConfig{jobAConf, jobBConf} {
			// Submit each job
			By(fmt.Sprintf("[%s] Create job %s with 3 pods", jobConf.PodConfig.Labels["applicationId"], jobConf.Name))
			job, jobErr := k8s.InitJobConfig(jobConf)
			Ω(jobErr).NotTo(HaveOccurred())
			_, jobErr = kClient.CreateJob(job, ns)
			Ω(jobErr).NotTo(HaveOccurred())
			createErr := kClient.WaitForJobPodsCreated(ns, jobConf.Name, int(jobConf.Parallelism), 30*time.Second)
			Ω(createErr).NotTo(HaveOccurred())
			timeoutErr := kClient.WaitForJobPods(ns, jobConf.Name, int(jobConf.Parallelism), 90*time.Second)
			Ω(timeoutErr).NotTo(HaveOccurred())

			// List job pods and verify running on expected node
			jobPods, lstErr := kClient.ListPods(ns, fmt.Sprintf("job-name=%s", jobConf.Name))
			Ω(lstErr).NotTo(HaveOccurred())
			Ω(jobPods).NotTo(BeNil())
			Ω(len(jobPods.Items)).Should(Equal(int(3)), "Pods count should be 3")
			for _, pod := range jobPods.Items {
				Ω(pod.Spec.NodeName).To(Equal(sortedWorkerNodes[i].Name),
					"job pods not scheduled to correct node")
			}
		}
	})

	AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{ns})
		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
