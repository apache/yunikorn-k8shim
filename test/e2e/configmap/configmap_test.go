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

package configmap

import (
	"bytes"
	"compress/gzip"
	"io"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var oldConfigMap = new(v1.ConfigMap)

var _ = Describe("ConfigMap", func() {
	BeforeEach(func() {
		By("Get previous config")
		var err error
		oldConfigMap, err = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(err).NotTo(HaveOccurred())
		Ω(oldConfigMap).NotTo(BeNil())
	})

	It("Verify_ConfigMap_File", func() {
		configMap, err := k8s.GetConfigMapObj("../testdata/yunikorn-configs.yaml")
		Ω(err).NotTo(HaveOccurred())
		Ω(configMap).NotTo(BeNil())

		By("Updating the config map with BinaryData")
		configMap.Namespace = configmanager.YuniKornTestConfig.YkNamespace
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(HaveOccurred())

		queues := configMap.Data[configmanager.DefaultPolicyGroup]

		schedulerConfig, err := configs.LoadSchedulerConfigFromByteArray([]byte(queues))
		Ω(err).NotTo(HaveOccurred())
		Ω(len(schedulerConfig.Partitions)).To(Equal(1))
		Ω(len(schedulerConfig.Partitions[0].Queues)).To(Equal(1))

		ts := schedulerConfig.Partitions[0].Queues[0].Properties["timestamp"]
		err = yunikorn.WaitForQueueTS("root", ts, 30*time.Second)
		Ω(err).NotTo(HaveOccurred())

		checkSchedulerConfig(schedulerConfig)
	})

	It("Verify_Compressed_ConfigMap_File", func() {
		configMap, err := k8s.GetConfigMapObj("../testdata/compressed_yunikorn-configs.yaml")
		Ω(err).NotTo(HaveOccurred())
		Ω(configMap).NotTo(BeNil())

		By("Updating the config map with BinaryData")
		configMap.Namespace = configmanager.YuniKornTestConfig.YkNamespace
		_, err = kClient.UpdateConfigMap(configMap, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(HaveOccurred())

		queuesGz := configMap.BinaryData[configmanager.DefaultPolicyGroup+".gz"]
		Ω(len(queuesGz)).NotTo(Equal(0))
		gzReader, err := gzip.NewReader(bytes.NewReader(queuesGz))
		Ω(err).NotTo(HaveOccurred())
		decompressedBytes, err := io.ReadAll(gzReader)
		Ω(err).NotTo(HaveOccurred())
		err = gzReader.Close()
		Ω(err).NotTo(HaveOccurred())

		schedulerConfig, err := configs.LoadSchedulerConfigFromByteArray(decompressedBytes)
		Ω(err).NotTo(HaveOccurred())
		Ω(len(schedulerConfig.Partitions)).To(Equal(1))
		Ω(len(schedulerConfig.Partitions[0].Queues)).To(Equal(1))

		ts := schedulerConfig.Partitions[0].Queues[0].Properties["timestamp"]
		err = yunikorn.WaitForQueueTS("root", ts, 30*time.Second)
		Ω(err).NotTo(HaveOccurred())

		checkSchedulerConfig(schedulerConfig)
	})

	AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{"default"})
		yunikorn.RestoreConfigMapWrapper(oldConfigMap, "")
	})
})

func checkSchedulerConfig(schedulerConfig *configs.SchedulerConfig) {
	configDAOInfo, err := restClient.GetConfig()
	Ω(err).NotTo(HaveOccurred())
	Ω(configDAOInfo).NotTo(BeNil())
	Ω(configDAOInfo.Partitions).To(Equal(schedulerConfig.Partitions))
}
