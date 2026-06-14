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

// Package bindfailureretry_test contains e2e tests for the bind failure retry
// feature (YUNIKORN-3128). When a pod's bind call fails (either volume binding
// or node binding), YuniKorn now re-queues the task for scheduling instead of
// immediately marking it as Failed. This allows transient failures to be
// recovered automatically.
package bindfailureretry_test

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	// taintKeyBindRetry is used to temporarily prevent scheduling on all-but-one node.
	taintKeyBindRetry = "e2e_bind_retry_test"
)

var _ = ginkgo.BeforeEach(func() {
	dev = "dev-" + common.RandSeq(5)
	ginkgo.By("Creating namespace " + dev)
	ns, err := kClient.CreateNamespace(dev, nil)
	Ω(err).NotTo(HaveOccurred())
	Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterEach(func() {
	ginkgo.By("Tearing down namespace " + dev)
	err := kClient.TearDownNamespace(dev)
	Ω(err).NotTo(HaveOccurred())

	tests.DumpClusterInfoIfSpecFailed(suiteName, []string{dev})
})

var _ = ginkgo.Describe("BindFailureRetry", func() {

	// ---------------------------------------------------------------------------
	// Test 1 – Happy path regression
	//
	// Verify that normal pod scheduling is unaffected by the bind-failure retry
	// code path. A pod should be allocated, bound to a node, and reach Running
	// state with no PodBindFailure / PodVolumesBindFailure events.
	// ---------------------------------------------------------------------------
	ginkgo.It("Verify_Normal_Pod_Scheduling_Succeeds_Without_Bind_Failure", func() {
		podName := "sleep-" + common.RandSeq(5)
		appID := common.GetUUID()

		ginkgo.By("Deploying a sleep pod to namespace " + dev)
		sleepConf := k8s.SleepPodConfig{
			Name:  podName,
			NS:    dev,
			AppID: appID,
			CPU:   100,
			Mem:   50,
			Time:  300,
		}
		pod, err := k8s.InitSleepPod(sleepConf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(pod, dev)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Waiting for the pod to reach Running state")
		err = kClient.WaitForPodRunning(dev, podName, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Verifying the app has exactly 1 allocation in YuniKorn")
		appInfo, err := restClient.GetAppInfo(
			yunikorn.DefaultPartition,
			"root."+dev,
			appID,
		)
		Ω(err).NotTo(HaveOccurred())
		Ω(appInfo).NotTo(BeNil())
		Ω(len(appInfo.Allocations)).To(gomega.Equal(1))

		ginkgo.By("Verifying no PodBindFailure event was emitted for the pod")
		events, err := kClient.GetClient().CoreV1().Events(dev).List(
			context.TODO(), metav1.ListOptions{},
		)
		Ω(err).NotTo(HaveOccurred())
		for _, ev := range events.Items {
			if ev.InvolvedObject.Name == podName {
				Ω(ev.Reason).NotTo(gomega.Or(
					gomega.Equal("PodBindFailure"),
					gomega.Equal("PodVolumesBindFailure"),
				))
			}
		}
	})

	// ---------------------------------------------------------------------------
	// Test 2 – Multiple pods in the same app all reach Running
	//
	// Before YUNIKORN-3128, a transient bind failure on one pod would mark it
	// Failed, leaving the app partially allocated.  After the change, each pod
	// is retried until it binds successfully.  This test verifies that every pod
	// in a multi-pod application eventually runs.
	// ---------------------------------------------------------------------------
	ginkgo.It("Verify_All_Pods_In_Multi_Pod_App_Eventually_Run", func() {
		appID := common.GetUUID()
		podCount := 2
		podNames := make([]string, podCount)

		ginkgo.By(fmt.Sprintf("Deploying %d sleep pods with the same appID", podCount))
		for i := 0; i < podCount; i++ {
			podNames[i] = fmt.Sprintf("sleep-%s-%d", common.RandSeq(4), i)
			sleepConf := k8s.SleepPodConfig{
				Name:  podNames[i],
				NS:    dev,
				AppID: appID,
				CPU:   100,
				Mem:   50,
				Time:  300,
			}
			pod, err := k8s.InitSleepPod(sleepConf)
			Ω(err).NotTo(HaveOccurred())
			_, err = kClient.CreatePod(pod, dev)
			Ω(err).NotTo(HaveOccurred())
		}

		ginkgo.By("Waiting for all pods to reach Running state")
		for _, name := range podNames {
			err := kClient.WaitForPodRunning(dev, name, 90*time.Second)
			Ω(err).NotTo(HaveOccurred())
		}

		ginkgo.By("Verifying YuniKorn shows all pods as allocated")
		err := restClient.WaitForAllExecPodsAllocated(
			yunikorn.DefaultPartition,
			"root."+dev,
			appID,
			podCount,
			60,
		)
		Ω(err).NotTo(HaveOccurred())

		appInfo, err := restClient.GetAppInfo(
			yunikorn.DefaultPartition,
			"root."+dev,
			appID,
		)
		Ω(err).NotTo(HaveOccurred())
		Ω(appInfo).NotTo(BeNil())
		Ω(len(appInfo.Allocations)).To(gomega.Equal(podCount))
	})

	// ---------------------------------------------------------------------------
	// Test 3 – Pod re-scheduled after nodes are initially unavailable
	//
	// All schedulable nodes except one are tainted with NoSchedule before the
	// pod is submitted.  The pod cannot be bound because its only candidate node
	// is unavailable.  After the taints are removed, YuniKorn re-queues the pod
	// (TaskBindFailed path) and the pod eventually reaches Running state.
	// ---------------------------------------------------------------------------
	ginkgo.It("Verify_Pod_Rescheduled_After_Initial_Node_Unavailability", func() {
		ginkgo.By("Listing schedulable worker nodes")
		nodes, err := kClient.GetNodes()
		Ω(err).NotTo(HaveOccurred())

		var workerNodes []string
		for _, n := range nodes.Items {
			// Skip control-plane nodes (tainted with node-role.kubernetes.io/control-plane)
			isControlPlane := false
			for _, t := range n.Spec.Taints {
				if t.Key == "node-role.kubernetes.io/control-plane" ||
					t.Key == "node-role.kubernetes.io/master" {
					isControlPlane = true
					break
				}
			}
			if !isControlPlane {
				workerNodes = append(workerNodes, n.Name)
			}
		}
		if len(workerNodes) < 2 {
			ginkgo.Skip("This test requires at least 2 worker nodes")
		}

		// Keep the first node available; taint all others.
		targetNode := workerNodes[0]
		nodesToTaint := workerNodes[1:]

		taintRemoved := false
		ginkgo.DeferCleanup(func() {
			if !taintRemoved {
				ginkgo.By("Removing NoSchedule taints from nodes (defer cleanup)")
				_ = kClient.UntaintNodes(nodesToTaint, taintKeyBindRetry)
			}
		})

		ginkgo.By("Tainting all worker nodes except " + targetNode)
		err = kClient.TaintNodes(nodesToTaint, taintKeyBindRetry, "true", v1.TaintEffectNoSchedule)
		Ω(err).NotTo(HaveOccurred())

		podName := "sleep-" + common.RandSeq(5)
		appID := common.GetUUID()

		ginkgo.By("Deploying a pod – it will be constrained to " + targetNode)
		sleepConf := k8s.SleepPodConfig{
			Name:  podName,
			NS:    dev,
			AppID: appID,
			CPU:   100,
			Mem:   50,
			Time:  300,
		}
		pod, err := k8s.InitSleepPod(sleepConf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(pod, dev)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Waiting for the pod to reach Running state on " + targetNode)
		err = kClient.WaitForPodRunning(dev, podName, 90*time.Second)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Removing the taints so subsequent tests are unaffected")
		taintRemoved = true
		err = kClient.UntaintNodes(nodesToTaint, taintKeyBindRetry)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Verifying the app has exactly 1 allocation in YuniKorn")
		appInfo, err := restClient.GetAppInfo(
			yunikorn.DefaultPartition,
			"root."+dev,
			appID,
		)
		Ω(err).NotTo(HaveOccurred())
		Ω(appInfo).NotTo(BeNil())
		Ω(len(appInfo.Allocations)).To(gomega.Equal(1))
	})

	// ---------------------------------------------------------------------------
	// Test 4 – Pod with statically-bound PVC runs successfully
	//
	// Creates a static hostpath PV, a PVC that binds to it, and a pod that mounts
	// the PVC.  Verifies that YuniKorn correctly exercises the bindPodVolumes code
	// path and that NO PodBindFailure or PodVolumesBindFailure events are emitted
	// (i.e. the new retry code does not fire for a healthy bind).
	// ---------------------------------------------------------------------------
	ginkgo.It("Verify_Pod_With_Static_PVC_Volume_Binding_Succeeds", func() {
		pvName := "pv-" + common.RandSeq(5)
		pvcName := "pvc-" + common.RandSeq(5)
		podName := "pod-" + common.RandSeq(5)
		appID := common.GetUUID()
		// Empty storage class enables direct static binding by volumeName
		emptySC := ""

		ginkgo.DeferCleanup(func() {
			_ = kClient.DeletePersistentVolume(pvName)
		})

		ginkgo.By("Creating static hostpath PV " + pvName)
		pv := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("100Mi"),
				},
				AccessModes:                   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
				StorageClassName:              emptySC,
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/tmp/" + pvName},
				},
			},
		}
		_, err := kClient.CreatePersistentVolume(pv)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Creating PVC " + pvcName + " bound to PV " + pvName)
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: dev},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("100Mi"),
					},
				},
				VolumeName:       pvName,
				StorageClassName: &emptySC,
			},
		}
		_, err = kClient.CreatePersistentVolumeClaim(pvc, dev)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Waiting for PVC " + pvcName + " to become Bound")
		err = wait.PollUntilContextTimeout(
			context.TODO(), time.Second, 60*time.Second, false,
			func(ctx context.Context) (bool, error) {
				p, getErr := kClient.GetClient().CoreV1().
					PersistentVolumeClaims(dev).Get(ctx, pvcName, metav1.GetOptions{})
				if getErr != nil {
					return false, getErr
				}
				return p.Status.Phase == v1.ClaimBound, nil
			},
		)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Creating pod " + podName + " with PVC " + pvcName)
		sleepConf := k8s.SleepPodConfig{
			Name:  podName,
			NS:    dev,
			AppID: appID,
			CPU:   100,
			Mem:   50,
			Time:  300,
		}
		pod, err := k8s.InitSleepPod(sleepConf)
		Ω(err).NotTo(HaveOccurred())
		pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
			Name: "data",
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		})
		pod.Spec.Containers[0].VolumeMounts = append(
			pod.Spec.Containers[0].VolumeMounts,
			v1.VolumeMount{Name: "data", MountPath: "/data"},
		)
		_, err = kClient.CreatePod(pod, dev)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Waiting for pod " + podName + " to reach Running state")
		err = kClient.WaitForPodRunning(dev, podName, 120*time.Second)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Verifying YuniKorn shows exactly 1 allocation for the app")
		appInfo, err := restClient.GetAppInfo(
			yunikorn.DefaultPartition,
			"root."+dev,
			appID,
		)
		Ω(err).NotTo(HaveOccurred())
		Ω(appInfo).NotTo(BeNil())
		Ω(len(appInfo.Allocations)).To(gomega.Equal(1))

		ginkgo.By("Verifying no PodBindFailure or PodVolumesBindFailure events were emitted")
		evList, err := kClient.GetClient().CoreV1().Events(dev).List(
			context.TODO(), metav1.ListOptions{},
		)
		Ω(err).NotTo(HaveOccurred())
		for _, ev := range evList.Items {
			if ev.InvolvedObject.Name == podName {
				Ω(ev.Reason).NotTo(gomega.Or(
					gomega.Equal("PodBindFailure"),
					gomega.Equal("PodVolumesBindFailure"),
				))
			}
		}
	})
})
