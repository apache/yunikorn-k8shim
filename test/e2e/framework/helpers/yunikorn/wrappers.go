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

package yunikorn

import (
	"time"

	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"

	"github.com/apache/yunikorn-core/pkg/common/configs"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var k = k8s.KubeCtl{}

func EnsureYuniKornConfigsPresent() {
	Ω(k.SetClient()).To(BeNil())
	By("Create initial configMap if not exists")
	exists, err := k.ConfigMapExists(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err).NotTo(HaveOccurred())
	if !exists {
		// create default configmap
		cm := CreateDefaultConfigMap()
		_, err := k.CreateConfigMap(cm, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(HaveOccurred())
	} else {
		// update with explicit policy if not already present
		cm, err := k.GetConfigMap(constants.ConfigMapName, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err).NotTo(HaveOccurred())
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		if _, ok := cm.Data[configmanager.DefaultPolicyGroup]; !ok {
			cm.Data[configmanager.DefaultPolicyGroup] = configs.DefaultSchedulerConfig
			_, err = k.UpdateConfigMap(cm, configmanager.YuniKornTestConfig.YkNamespace)
			Ω(err).NotTo(HaveOccurred())
		}
	}
}

func UpdateConfigMapWrapper(oldConfigMap *v1.ConfigMap, schedPolicy string, annotation string) {
	UpdateCustomConfigMapWrapper(oldConfigMap, schedPolicy, annotation, func(sc *configs.SchedulerConfig) error {
		return nil
	})
}

func UpdateCustomConfigMapWrapper(oldConfigMap *v1.ConfigMap, schedPolicy string, annotation string, mutator func(sc *configs.SchedulerConfig) error) {
	UpdateCustomConfigMapWrapperWithMap(oldConfigMap, schedPolicy, annotation, nil, mutator)
}

func UpdateCustomConfigMapWrapperWithMap(oldConfigMap *v1.ConfigMap, schedPolicy string, annotation string, customMap map[string]string, mutator func(sc *configs.SchedulerConfig) error) {
	Ω(k.SetClient()).To(BeNil())
	By("Port-forward the scheduler pod")
	fwdErr := k.PortForwardYkSchedulerPod()
	Ω(fwdErr).NotTo(HaveOccurred())

	By("Enabling new scheduling config")

	// Save old configMap
	Ω(k.SetClient()).To(BeNil())
	var c, err = k.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.DefaultYuniKornConfigMap)
	Ω(err).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())
	c.DeepCopyInto(oldConfigMap)
	Ω(c).Should(BeEquivalentTo(oldConfigMap))

	// Create configMap with timestamp + schedPolicy
	sc := common.CreateBasicConfigMap()
	if schedPolicy != "" {
		err = common.SetSchedulingPolicy(sc, "default", "root", schedPolicy)
		Ω(err).NotTo(HaveOccurred())
	}
	// Wait for 1 second to set a new timestamp. If we don't wait for it, we may get a same timestamp.
	time.Sleep(1 * time.Second)
	ts, tsErr := common.SetQueueTimestamp(sc, "default", "root")
	Ω(tsErr).NotTo(HaveOccurred())

	// allow caller to customize further
	mutatorErr := mutator(sc)
	Ω(mutatorErr).NotTo(HaveOccurred())

	configStr, yamlErr := common.ToYAML(sc)
	Ω(yamlErr).NotTo(HaveOccurred())
	c.Data[configmanager.DefaultPolicyGroup] = configStr

	for k, v := range customMap {
		c.Data[k] = v
	}
	c.BinaryData = nil
	var d, err3 = k.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(d).NotTo(BeNil())

	err = WaitForQueueTS("root", ts, 2*time.Minute)
	Ω(err).NotTo(HaveOccurred())
}

func RestoreConfigMapWrapper(oldConfigMap *v1.ConfigMap, annotation string) {
	Ω(k.SetClient()).To(BeNil())
	By("Restoring the old config maps")
	var c, err = k.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
		configmanager.DefaultYuniKornConfigMap)
	Ω(err).NotTo(HaveOccurred())
	Ω(c).NotTo(BeNil())

	oldSC := new(configs.SchedulerConfig)
	err = yaml.Unmarshal([]byte(oldConfigMap.Data[configmanager.DefaultPolicyGroup]), oldSC)
	Ω(err).NotTo(HaveOccurred())
	ts, tsErr := common.SetQueueTimestamp(oldSC, "default", "root")
	Ω(tsErr).NotTo(HaveOccurred())
	c.Data = oldConfigMap.Data
	c.BinaryData = oldConfigMap.BinaryData
	c.Data[configmanager.DefaultPolicyGroup], err = common.ToYAML(oldSC)
	Ω(err).NotTo(HaveOccurred())

	var e, err3 = k.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err3).NotTo(HaveOccurred())
	Ω(e).NotTo(BeNil())

	err = WaitForQueueTS("root", ts, 2*time.Minute)
	Ω(err).NotTo(HaveOccurred())
}

// There is no available method to check whether the config in admission controller has been updated
// As a temporary solution, we are checking the update event using the informer, followed by a 1-second sleep.
// Please refer to YUNIKORN-1998 for more details
func WaitForAdmissionControllerRefreshConfAfterAction(action func()) {
	k8s.ObserveConfigMapInformerUpdateAfterAction(action)
	time.Sleep(1 * time.Second)
}

var Describe = ginkgo.Describe
var It = ginkgo.It
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach

var Equal = gomega.Equal
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
var BeEquivalentTo = gomega.BeEquivalentTo
