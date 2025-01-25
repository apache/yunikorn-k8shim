package pod_resource_scaling

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"

	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

var kClient k8s.KubeCtl
var yClient yunikorn.RClient
var err error
var ns string
var oldConfigMap = new(v1.ConfigMap)

var _ = ginkgo.BeforeSuite(func() {
	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())

	// Initializing rest client
	yClient = yunikorn.RClient{}
	Ω(yClient).NotTo(gomega.BeNil())
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")
})

var _ = ginkgo.BeforeEach(func() {

	// Skip if K8s version < 1.27
	k8sVer, err := kClient.GetKubernetesVersion()
	Ω(err).NotTo(HaveOccurred())
	var minVersion *version.Info = &version.Info{
		Major: "1",
		Minor: "27",
	}
	if k8sVer.Major < minVersion.Major || (k8sVer.Major == minVersion.Major && k8sVer.Minor < minVersion.Minor) {
		ginkgo.Skip("InPlacePodVerticalScaling requires K8s 1.27+")
	}

	// Create test namespace
	ns = "test-" + common.RandSeq(10)
	_, err = kClient.CreateNamespace(ns, nil)
	Ω(err).NotTo(HaveOccurred())
})

// var _ = ginkgo.AfterEach(func() {
// 	err := kClient.DeleteNamespace(ns)
// 	Ω(err).NotTo(HaveOccurred())
// })

var _ = ginkgo.AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

var _ = ginkgo.Describe("InPlacePodVerticalScaling", func() {
	ginkgo.It("Should handle pod resource updates correctly", func() {
		// Create pod with initial resources
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
				Labels: map[string]string{
					"app": "test-pod",
				},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "nginx",
						Image: "nginx:latest",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("100m"),
								"memory": resource.MustParse("100Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("100m"),
								"memory": resource.MustParse("100Mi"),
							},
						},
						ResizePolicy: []v1.ContainerResizePolicy{
							{
								ResourceName:  "cpu",
								RestartPolicy: "NotRequired",
							},
							{
								ResourceName:  "memory",
								RestartPolicy: "NotRequired",
							},
						},
					},
				},
			},
		}

		// Create pod
		_, err = kClient.CreatePod(pod, ns)
		Ω(err).NotTo(HaveOccurred())

		// Wait for pod running
		err = kClient.WaitForPodRunning(ns, pod.Name, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		// Get initial queue usage
		queueInfo, err := yClient.GetQueue("default", "root", false)
		Ω(err).NotTo(HaveOccurred())
		var initialUsage yunikorn.ResourceUsage
		initialUsage.ParseResourceUsage(queueInfo.MaxResource)
		initialCPU := initialUsage.GetResourceValue(siCommon.CPU)

		// Get initial pod state
		initialPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialStartTime := initialPod.Status.StartTime
		initialRestartCount := initialPod.Status.ContainerStatuses[0].RestartCount

		// Update pod resources
		updatedPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())

		updatedPod.Spec.Containers[0].Resources = v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("200m"),
				"memory": resource.MustParse("200Mi"),
			},
			Limits: v1.ResourceList{
				"cpu":    resource.MustParse("200m"),
				"memory": resource.MustParse("200Mi"),
			},
		}

		patch := []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/cpu",
				"value": "200m",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/memory",
				"value": "200Mi",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/cpu",
				"value": "200m",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/memory",
				"value": "200Mi",
			},
		}
		_, err = kClient.PatchPod(updatedPod, ns, patch)
		Ω(err).NotTo(HaveOccurred())

		time.Sleep(5 * time.Second)
		finalPod, err := kClient.GetPod(ns, pod.Name)
		Ω(err).NotTo(HaveOccurred())
		Ω(finalPod.Status.StartTime).To(gomega.Equal(initialStartTime), "Pod should not have restarted")
		Ω(finalPod.Status.ContainerStatuses[0].RestartCount).To(gomega.Equal(initialRestartCount), "Container should not have restarted")
		Ω(finalPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()).To(gomega.Equal(int64(200)))
		Ω(finalPod.Spec.Containers[0].Resources.Requests.Memory().Value() / (1024 * 1024)).To(gomega.Equal(int64(200)))

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			queueInfo, err = yClient.GetQueue("default", "root", false)
			if err != nil {
				return false
			}
			var usedResource yunikorn.ResourceUsage
			usedResource.ParseResourceUsage(queueInfo.MaxResource)
			usedCPU := usedResource.GetResourceValue(siCommon.CPU)
			return usedCPU > initialCPU
		}, 30*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())
	})
})
