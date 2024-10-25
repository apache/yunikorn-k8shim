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

package foreign_pod

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const kubeSystem = "kube-system"

var _ = Describe("", func() {
	It("Verify foreign pod tracking", func() {
		By("Retrieving foreign pods from kube-system")
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		podList, err := kClient.GetPods(kubeSystem)
		Ω(err).NotTo(gomega.HaveOccurred())

		kubeUIDs := make(map[string]bool)
		kubeNodes := make(map[string]string)
		for _, pod := range podList.Items {
			kubeUIDs[string(pod.UID)] = true
			kubeNodes[string(pod.UID)] = pod.Spec.NodeName
			fmt.Fprintf(ginkgo.GinkgoWriter, "pod: %s, uid: %s, node: %s\n", pod.Name, pod.UID, pod.Spec.NodeName)
		}

		// retrieve foreign pod info
		By("Retrieving foreign allocations")
		var restClient yunikorn.RClient
		nodes, err := restClient.GetNodes("default")
		Ω(err).NotTo(gomega.HaveOccurred())
		foreignAllocs := make(map[string]bool)
		foreignNodes := make(map[string]string)
		for _, n := range *nodes {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Checking node %s\n", n.NodeID)
			if len(n.ForeignAllocations) > 0 {
				for _, falloc := range n.ForeignAllocations {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Found allocation %s on node %s\n", falloc.AllocationKey, falloc.NodeID)
					foreignAllocs[falloc.AllocationKey] = true
					foreignNodes[falloc.AllocationKey] = falloc.NodeID
				}
			}
		}

		// check that all UIDs from kube-system are tracked properly
		for uid := range kubeUIDs {
			Ω(foreignAllocs[uid]).To(Equal(true), "pod %s from kube-system is not tracked in Yunikorn", uid)
			Ω(foreignNodes[uid]).To(Equal(kubeNodes[uid]), "pod %s is tracked under incorrect node", uid)
		}
	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{kubeSystem})
	})
})
