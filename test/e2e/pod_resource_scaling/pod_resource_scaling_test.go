package pod_resource_scaling

import (
	"fmt"
	"runtime"
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
)

var kClient k8s.KubeCtl
var restClient yunikorn.RClient
var err error
var ns string
var oldConfigMap = new(v1.ConfigMap)
var suiteName string
var podName = "test-pod"

var _ = ginkgo.BeforeSuite(func() {
	_, filename, _, _ := runtime.Caller(0)
	suiteName = common.GetSuiteName(filename)

	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())

	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "")
})

var _ = BeforeEach(func() {
	// Skip if K8s version < 1.32
	k8sVer, err := kClient.GetKubernetesVersion()
	Ω(err).NotTo(HaveOccurred())
	var minVersion *version.Info = &version.Info{
		Major: "1",
		Minor: "32",
	}
	if k8sVer.Major < minVersion.Major || (k8sVer.Major == minVersion.Major && k8sVer.Minor < minVersion.Minor) {
		ginkgo.Skip("InPlacePodVerticalScaling requires K8s 1.32+")
	}

	// Create test namespace
	ns = "test-" + common.RandSeq(10)
	_, err = kClient.CreateNamespace(ns, nil)
	Ω(err).NotTo(HaveOccurred())
})

var _ = ginkgo.AfterEach(func() {
	By("Killing all pods")
	err := kClient.DeletePods(ns)
	Ω(err).NotTo(HaveOccurred())
	err = kClient.DeleteNamespace(ns)
	Ω(err).NotTo(HaveOccurred())
})

var _ = ginkgo.AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap)
})

func verifyYunikornResourceUsage(pod *v1.Pod, resourceName string, value int64) {
	err = utils.WaitForCondition(func() bool {
		currentPod, err := kClient.GetPod(pod.Name, ns)
		if err != nil {
			return false
		}

		// Verify scheduler name is yunikorn
		if currentPod.Spec.SchedulerName != "yunikorn" {
			return false
		}

		appID := "yunikorn-" + ns + "-autogen"
		app, err := restClient.GetAppInfo("default", "root."+ns, appID)
		if err != nil || app == nil {
			return false
		}

		if app.Allocations == nil {
			return false
		}

		for _, alloc := range app.Allocations {
			resVal, exists := alloc.ResourcePerAlloc[resourceName]
			if !exists {
				return false
			}

			if resVal == value {
				return true
			}
		}

		return false
	}, 30*time.Second, 120*time.Second)
	Ω(err).NotTo(HaveOccurred(), fmt.Sprintf("Pod should be scheduled by YuniKorn with correct resource(%s) allocation", resourceName))
}

var _ = ginkgo.Describe("InPlacePodVerticalScaling", func() {
	ginkgo.It("Pod resources(cpu/memory) resize up", func() {
		// Create pod with initial resources
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
				Labels: map[string]string{
					"app": podName,
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

		// Check if pod is scheduled by YuniKorn and verify CPU allocation is 100m
		verifyYunikornResourceUsage(pod, "vcore", 100)

		// Get initial pod state
		initialPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialStartTime := initialPod.Status.StartTime
		initialRestartCount := initialPod.Status.ContainerStatuses[0].RestartCount

		patch := []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/cpu",
				"value": "200m",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/cpu",
				"value": "200m",
			},
		}
		_, err = kClient.PatchPod(initialPod, ns, patch, "resize")
		Ω(err).NotTo(HaveOccurred())

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}
			return currentPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue() == int64(200)
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		finalPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(finalPod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(finalPod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(finalPod, "vcore", 200)

		patch = []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/memory",
				"value": "200Mi",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/memory",
				"value": "200Mi",
			},
		}
		_, err = kClient.PatchPod(initialPod, ns, patch, "resize")
		Ω(err).NotTo(HaveOccurred())

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}
			// Memory is in bytes, 200Mi = 200 * 1024 * 1024 bytes
			return currentPod.Spec.Containers[0].Resources.Requests.Memory().Value() == int64(200*1024*1024)
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		finalPod, err = kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(finalPod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(finalPod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(finalPod, "memory", 200*1024*1024)
	})

	ginkgo.It("Pod resources(cpu/memory) resize down", func() {
		// Create pod with initial resources
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
				Labels: map[string]string{
					"app": podName,
				},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "nginx",
						Image: "nginx:latest",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("200m"),
								"memory": resource.MustParse("200Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("200m"),
								"memory": resource.MustParse("200Mi"),
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

		// Check if pod is scheduled by YuniKorn and verify CPU allocation is 100m
		verifyYunikornResourceUsage(pod, "vcore", 200)

		// Get initial pod state
		initialPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialStartTime := initialPod.Status.StartTime
		initialRestartCount := initialPod.Status.ContainerStatuses[0].RestartCount

		patch := []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/cpu",
				"value": "100m",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/cpu",
				"value": "100m",
			},
		}
		_, err = kClient.PatchPod(initialPod, ns, patch, "resize")
		Ω(err).NotTo(HaveOccurred())

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}
			return currentPod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue() == int64(100)
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		finalPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(finalPod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(finalPod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(finalPod, "vcore", 100)

		patch = []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/memory",
				"value": "100Mi",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/memory",
				"value": "100Mi",
			},
		}
		_, err = kClient.PatchPod(initialPod, ns, patch, "resize")
		Ω(err).NotTo(HaveOccurred()) // Expect an error as memory cannot be decreased

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}

			return currentPod.Spec.Containers[0].Resources.Requests.Memory().Value() == int64(100*1024*1024)
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		finalPod, err = kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(finalPod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(finalPod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")
		verifyYunikornResourceUsage(finalPod, "memory", 100*1024*1024)
	})

	ginkgo.It("Pod resources(cpu/memory) resize to excessive values should fail", func() {
		// Create pod with initial resources
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
				Labels: map[string]string{
					"app": podName,
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

		// Check if pod is scheduled by YuniKorn and verify CPU allocation is 100m
		verifyYunikornResourceUsage(pod, "vcore", 100)

		// Get initial pod state
		initialPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		initialStartTime := initialPod.Status.StartTime
		initialRestartCount := initialPod.Status.ContainerStatuses[0].RestartCount

		patch := []map[string]interface{}{
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/limits/cpu",
				"value": "100000",
			},
			{
				"op":    "replace",
				"path":  "/spec/containers/0/resources/requests/cpu",
				"value": "100000",
			},
		}
		// Patch CPU to an excessive value
		_, err = kClient.PatchPod(initialPod, ns, patch, "resize")
		Ω(err).NotTo(HaveOccurred())

		// Wait for resource update to be reflected
		err = utils.WaitForCondition(func() bool {
			currentPod, err := kClient.GetPod(pod.Name, ns)
			if err != nil {
				return false
			}
			return currentPod.Status.Resize == v1.PodResizeStatusInfeasible
		}, 10*time.Second, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		finalPod, err := kClient.GetPod(pod.Name, ns)
		Ω(err).NotTo(HaveOccurred())
		Ω(finalPod.Status.StartTime).To(Equal(initialStartTime), "Pod should not have restarted")
		Ω(finalPod.Status.ContainerStatuses[0].RestartCount).To(Equal(initialRestartCount), "Container should not have restarted")

		// Verify pod resource usage is unchanged after set an excessive value
		verifyYunikornResourceUsage(finalPod, "vcore", 100)
	})
})
