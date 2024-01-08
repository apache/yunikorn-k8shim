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
	"errors"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	res "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
)

type ResourceUsage struct {
	memory   res.Quantity
	vCPU     res.Quantity
	resource map[string]int64
}

func (r *ResourceUsage) ParseResourceUsage(resource map[string]int64) {
	r.resource = resource
	r.memory = res.MustParse(fmt.Sprintf("%d", resource["memory"]))
	r.vCPU = res.MustParse(fmt.Sprintf("%dm", resource["vcore"]))
}

func (r *ResourceUsage) GetResourceValue(resourceName string) int64 {
	return r.resource[resourceName]
}

func (r *ResourceUsage) GetMemory() res.Quantity {
	return r.memory
}

func (r *ResourceUsage) GetCPU() res.Quantity {
	return r.vCPU
}

func GetYKUrl() string {
	return fmt.Sprintf("%s://%s",
		configmanager.YuniKornTestConfig.YkScheme,
		GetYKHost(),
	)
}

func GetYKHost() string {
	return fmt.Sprintf("%s:%s",
		configmanager.YuniKornTestConfig.YkHost,
		configmanager.YuniKornTestConfig.YkPort,
	)
}

func GetYKScheme() string {
	return configmanager.YuniKornTestConfig.YkScheme
}

func CreateDefaultConfigMap() *v1.ConfigMap {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.ConfigMapName,
			Namespace: configmanager.YuniKornTestConfig.YkNamespace,
		},
		Data: make(map[string]string),
	}
	cm.Data[configmanager.DefaultPolicyGroup] = configs.DefaultSchedulerConfig
	return cm
}

func GetSchedulerPodName(kClient k8s.KubeCtl) (string, error) {
	ykNS := configmanager.YuniKornTestConfig.YkNamespace
	schedComponent := fmt.Sprintf("component=%s", configmanager.YKScheduler)

	// Get current scheduler pod name
	schedPodList, err := kClient.ListPods(ykNS, schedComponent)
	if err != nil {
		return "", err
	}
	if len(schedPodList.Items) != 1 {
		msg := fmt.Sprintf("Scheduler pod list contains %d pods: %v", len(schedPodList.Items), schedPodList.Items)
		return "", errors.New(msg)
	}

	schedPod := schedPodList.Items[0]
	return schedPod.Name, nil
}

func RestorePortForwarding(kClient *k8s.KubeCtl) {
	ginkgo.By("Port-forward scheduler pod after restart")
	// kill running kubectl port-forward process if it exists
	kClient.KillPortForwardProcess()
	// port-forward the scheduler pod
	err := kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())
}

func RestartYunikorn(kClient *k8s.KubeCtl) {
	RestartYunikornAndAddTolerations(kClient, false, nil)
}

func RestartYunikornAndAddTolerations(kClient *k8s.KubeCtl, addTolerations bool, newTolerations []v1.Toleration) {
	schedulerPodName, err := kClient.GetSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())
	err = kClient.DeletePod(schedulerPodName, configmanager.YuniKornTestConfig.YkNamespace)
	Ω(err).NotTo(gomega.HaveOccurred())
	err = kClient.WaitForPodBySelector(configmanager.YuniKornTestConfig.YkNamespace, fmt.Sprintf("component=%s", configmanager.YKScheduler), 30*time.Second)
	Ω(err).NotTo(gomega.HaveOccurred())

	if addTolerations {
		schedulerPodName, err = kClient.GetSchedulerPod()
		Ω(err).NotTo(gomega.HaveOccurred())
		ykPhase, err2 := kClient.WaitForPodStateStable(configmanager.YuniKornTestConfig.YkNamespace, schedulerPodName, 30*time.Second)
		Ω(err2).NotTo(gomega.HaveOccurred())
		if ykPhase == v1.PodPending {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Scheduler pod is in Pending state\n")
			// used if YK cannot be scheduled because resources are occupied
			pod, err2 := kClient.GetPod(schedulerPodName, configmanager.YuniKornTestConfig.YkNamespace)
			Ω(err2).NotTo(gomega.HaveOccurred(), "Could not retrieve scheduler pod")
			tolerations := pod.Spec.Tolerations
			tolerations = append(tolerations, newTolerations...)
			pod.Spec.Tolerations = tolerations
			_, err = kClient.UpdatePod(pod, configmanager.YuniKornTestConfig.YkNamespace)
			Ω(err).NotTo(gomega.HaveOccurred(), "Could not update scheduler pod")
		}
	}

	err = kClient.WaitForPodBySelectorRunning(configmanager.YuniKornTestConfig.YkNamespace, fmt.Sprintf("component=%s", configmanager.YKScheduler), 30)
	Ω(err).NotTo(gomega.HaveOccurred())
}
