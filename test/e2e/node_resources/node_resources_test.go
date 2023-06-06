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

package node_resources_test

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

var _ = Describe("", func() {
	var kClient k8s.KubeCtl //nolint
	var restClient yunikorn.RClient
	var ns string

	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("Creating namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}

		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})

	// To verify the node capacity
	// 1. Map node capacity to node name from kClient
	// 2. Pull Yunikorn node capacities through rest
	// 3. Validate values are equal
	It("Verify_Node_Capacity", func() {
		// Get kClient node capacities
		kClientNodes, getErr := kClient.GetNodes()
		Ω(getErr).NotTo(HaveOccurred())
		kClientNodeCapacities := kClient.GetNodesCapacity(*kClientNodes)

		// Get Yunikorn node capacities via rest.
		ykNodes, restErr := restClient.GetNodes("default")
		Ω(restErr).NotTo(HaveOccurred())

		// For each yk node capacity, compare to official Kubernetes value
		for _, ykNode := range *ykNodes {
			nodeName := ykNode.NodeID
			var ykCapacity yunikorn.ResourceUsage
			ykCapacity.ParseResourceUsage(ykNode.Capacity)

			kubeNodeCPU := kClientNodeCapacities[nodeName]["cpu"]
			kubeNodeMem := kClientNodeCapacities[nodeName][siCommon.Memory]
			kubeNodeMem.RoundUp(resource.Mega) // round to nearest megabyte for comparison

			// Compare memory to nearest megabyte
			roundedYKMem := ykCapacity.GetMemory()
			roundedYKMem.RoundUp(resource.Mega)
			cmpRes := kubeNodeMem.Cmp(roundedYKMem)
			Ω(cmpRes).To(Equal(0))

			// Compare cpu cores to nearest millicore
			cmpRes = kubeNodeCPU.Cmp(ykCapacity.GetCPU())
			Ω(cmpRes).To(Equal(0))
		}
	})
})
