package simple_preemptor_test

import (
	"fmt"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
)

const (
	gangSleepJobPrefix   = "gang-sleep-job"
	normalSleepJobPrefix = "normal-sleep-job"
	taskGroupA           = "groupa"
	taskGroupB           = "groupb"
	taskGroupAprefix     = "tg-" + taskGroupA + "-" + gangSleepJobPrefix
	taskGroupBprefix     = "tg-" + taskGroupB + "-" + gangSleepJobPrefix
	parallelism          = 3
)

var _ = ginkgo.Describe("SimplePreemptor", func() {
	var kClient k8s.KubeCtl
	var restClient yunikorn.RClient
	var ns1 *v1.Namespace
	var oldConfigMap *v1.ConfigMap
	var dev = "dev" + common.RandSeq(5)

	// Define sleepPod
	sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev, Mem: 5000, Time: 600, RequiredNode: "yk8s-worker"}
	sleepPod2Configs := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: 5000, Time: 600}
	sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, RequiredNode: "yk8s-worker2", Mem: 4000, Time: 600}

	ginkgo.BeforeSuite(func() {
		// Initializing kubectl client
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(gomega.BeNil())
		// Initializing rest client
		restClient = yunikorn.RClient{}

		ginkgo.By("Enable basic scheduling config over config maps")
		var c, err = kClient.GetConfigMaps(configmanager.YuniKornTestConfig.YkNamespace,
			configmanager.DefaultYuniKornConfigMap)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(c).NotTo(gomega.BeNil())

		oldConfigMap = c.DeepCopy()
		Ω(c).Should(gomega.BeEquivalentTo(oldConfigMap))

		// Define basic configMap
		sc := common.CreateBasicConfigMap()
		configStr, yamlErr := common.ToYAML(sc)
		Ω(yamlErr).NotTo(gomega.HaveOccurred())

		c.Data[configmanager.DefaultPolicyGroup] = configStr
		var d, err3 = kClient.UpdateConfigMap(c, configmanager.YuniKornTestConfig.YkNamespace)
		Ω(err3).NotTo(gomega.HaveOccurred())
		Ω(d).NotTo(gomega.BeNil())

		ginkgo.By("create development namespace")
		ns1, err = kClient.CreateNamespace(dev, nil)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(ns1.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
	})

	ginkgo.It("Verify_basic_simple_preemption. Use case: Only one pod is running and same pod has been selected as victim", func() {

		ginkgo.By("Deploy the sleep pod to the development namespace")
		sleepObj, podErr := k8s.InitSleepPod(sleepPodConfigs)

		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			60)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deploy 2nd sleep pod to the development namespace")
		sleepObj2, podErr := k8s.InitSleepPod(sleepPod2Configs)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod2, err := kClient.CreatePod(sleepObj2, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod2.ObjectMeta.Labels["app"]),
			180)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		appsInfo2, err := restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo2).NotTo(gomega.BeNil())
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo2["allocations"]).NotTo(gomega.BeNil())
		allocations2, ok := appsInfo2["allocations"].([]interface{})[0].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(allocations2["allocationKey"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations2["nodeId"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations2["partition"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations2["uuid"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations2["applicationId"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core2 := sleepRespPod2.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem2 := sleepRespPod2.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap2, ok := allocations2["resource"].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(int64(resMap2["memory"].(float64))).To(gomega.Equal(mem2))
		gomega.Ω(int64(resMap2["vcore"].(float64))).To(gomega.Equal(core2))

		ginkgo.By("Deploy 3rd sleep daemon set pod to the development namespace")
		sleepObj3, podErr := k8s.InitSleepPod(sleepPod3Configs)
		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod3, err := kClient.CreatePod(sleepObj3, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod3.ObjectMeta.Labels["app"]),
			180)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		appsInfo, err := restClient.GetAppInfo("default", "root."+dev, sleepRespPod.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo["allocations"]).NotTo(gomega.BeNil())
		allocations, ok := appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(allocations["allocationKey"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["nodeId"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["partition"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["uuid"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["applicationId"]).To(gomega.Equal(sleepRespPod.ObjectMeta.Labels["applicationId"]))
		core := sleepRespPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
		mem := sleepRespPod.Spec.Containers[0].Resources.Requests.Memory().Value()
		resMap, ok := allocations["resource"].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(int64(resMap["memory"].(float64))).To(gomega.Equal(mem))
		gomega.Ω(int64(resMap["vcore"].(float64))).To(gomega.Equal(core))

		// assert sleeppod2 is killed
		err = kClient.WaitForPodEvent(dev, "sleepjob2", "Killing", 120)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		appsInfo, err = restClient.GetAppInfo("default", "root."+dev, sleepRespPod3.ObjectMeta.Labels["applicationId"])
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
		gomega.Ω(appsInfo).NotTo(gomega.BeNil())
		ginkgo.By("Verify the pod allocation properties")
		gomega.Ω(appsInfo["allocations"]).NotTo(gomega.BeNil())
		allocations, ok = appsInfo["allocations"].([]interface{})[0].(map[string]interface{})
		gomega.Ω(ok).Should(gomega.BeTrue())
		gomega.Ω(allocations["allocationKey"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["nodeId"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["partition"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["uuid"]).NotTo(gomega.BeNil())
		gomega.Ω(allocations["applicationId"]).To(gomega.Equal(sleepRespPod3.ObjectMeta.Labels["applicationId"]))
	})

	ginkgo.It("Verify_simple_preemption. Use case: When 3 sleep pods (2 opted out, regular) are running, regular pod should be victim to free up resources for 4th sleep pod", func() {

		// Define sleepPod
		sleepPodConfigs := k8s.SleepPodConfig{Name: "sleepjob", NS: dev, Mem: 2000, Time: 600, RequiredNode: "yk8s-worker"}
		sleepPod2Configs := k8s.SleepPodConfig{Name: "sleepjob2", NS: dev, Mem: 2000, Time: 600, RequiredNode: "yk8s-worker"}
		sleepPod3Configs := k8s.SleepPodConfig{Name: "sleepjob3", NS: dev, Mem: 2000, Time: 600, RequiredNode: "yk8s-worker"}
		sleepPod4Configs := k8s.SleepPodConfig{Name: "sleepjob4", NS: dev, Mem: 2500, Time: 600, Optedout: true, UID: types.UID("ddd")}
		sleepPod5Configs := k8s.SleepPodConfig{Name: "sleepjob5", NS: dev, Mem: 2500, Time: 600, Optedout: false}
		sleepPod6Configs := k8s.SleepPodConfig{Name: "sleepjob6", NS: dev, Mem: 2500, Time: 600, Optedout: false}
		sleepPod7Configs := k8s.SleepPodConfig{Name: "sleepjob7", NS: dev, Mem: 2500, Time: 600, RequiredNode: "yk8s-worker2"}

		for _, config := range []k8s.SleepPodConfig{sleepPodConfigs, sleepPod2Configs,
			sleepPod3Configs, sleepPod4Configs, sleepPod5Configs, sleepPod6Configs} {
			ginkgo.By("Deploy the sleep pod " + config.Name + " to the development namespace")
			sleepObj, podErr := k8s.InitSleepPod(config)

			Ω(podErr).NotTo(gomega.HaveOccurred())
			sleepRespPod, err := kClient.CreatePod(sleepObj, dev)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
			// Wait for pod to move to running state
			err = kClient.WaitForPodBySelectorRunning(dev,
				fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
				240)
			gomega.Ω(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Deploy the sleep pod " + sleepPod7Configs.Name + " to the development namespace")
		sleepObj, podErr := k8s.InitSleepPod(sleepPod7Configs)

		Ω(podErr).NotTo(gomega.HaveOccurred())
		sleepRespPod, err := kClient.CreatePod(sleepObj, dev)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// Wait for pod to move to running state
		err = kClient.WaitForPodBySelectorRunning(dev,
			fmt.Sprintf("app=%s", sleepRespPod.ObjectMeta.Labels["app"]),
			240)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

		// assert sleeppod6 is killed
		err = kClient.WaitForPodEvent(dev, "sleepjob4", "Killing", 1200)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())

	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Delete all sleep pods")
		// Delete all sleep pods
		var pods, err = kClient.GetPodNamesFromNS(ns1.Name)
		for _, each := range pods {
			if strings.Contains(each, "sleep") {
				ginkgo.By("Deleting sleep pod: " + each)
				err = kClient.DeletePod(each, ns1.Name)
				if err != nil {
					if statusErr, ok := err.(*k8serrors.StatusError); ok {
						if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
							fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pod %s - reason is %s, it "+
								"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
							continue
						}
					}
				}
			}
		}
	})

	ginkgo.AfterSuite(func() {
		ginkgo.By("Check Yunikorn's health")
		checks, err := yunikorn.GetFailedHealthChecks()
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(checks).To(gomega.Equal(""), checks)

		ginkgo.By("Tearing down namespace: " + ns1.Name)
		err = kClient.TearDownNamespace(ns1.Name)
		Ω(err).NotTo(gomega.HaveOccurred())
	})
})
