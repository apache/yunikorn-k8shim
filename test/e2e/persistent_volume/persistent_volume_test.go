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

package persistent_volume

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

var suiteName string
var kClient k8s.KubeCtl
var restClient yunikorn.RClient

const (
	localTypePv    = "Local"
	standardScName = "standard"

	saName     = "nfs-service-account"
	crName     = "nfs-cluster-role"
	crbName    = "nfs-cluster-role-binding" //nolint:gosec
	serverName = "nfs-provisioner"
	scName     = "nfs-sc"
)

var _ = ginkgo.BeforeEach(func() {
	// Create namespace
	dev = "dev-" + common.RandSeq(5)
	ginkgo.By("Create namespace " + dev)
	ns, err := kClient.CreateNamespace(dev, nil)
	Ω(err).NotTo(HaveOccurred())
	Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterEach(func() {
	ginkgo.By("Tearing down namespace: " + dev)
	err := kClient.TearDownNamespace(dev)
	Ω(err).NotTo(HaveOccurred())
})

var _ = ginkgo.Describe("PersistentVolume", func() {
	ginkgo.It("Verify_static_binding_of_local_pv", func() {
		pvName := "local-pv-" + common.RandSeq(5)
		conf := k8s.PvConfig{
			Name:         pvName,
			Capacity:     "1Gi",
			AccessModes:  []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Type:         localTypePv,
			Path:         "/tmp",
			StorageClass: standardScName,
		}

		ginkgo.By("Create local type pv " + pvName)
		pvObj, err := k8s.InitPersistentVolume(conf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePersistentVolume(pvObj)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPersistentVolumeAvailable(pvName, 60*time.Second)).NotTo(HaveOccurred())

		pvcName := "pvc-" + common.RandSeq(5)
		pvcConf := k8s.PvcConfig{
			Name:       pvcName,
			Capacity:   "1Gi",
			VolumeName: pvName,
		}

		ginkgo.By("Create pvc " + pvcName + ", which binds to " + pvName)
		pvcObj, err := k8s.InitPersistentVolumeClaim(pvcConf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePersistentVolumeClaim(pvcObj, dev)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPersistentVolumeClaimPresent(dev, pvcName, 60*time.Second)).NotTo(HaveOccurred())

		podName := "pod-" + common.RandSeq(5)
		podConf := k8s.TestPodConfig{
			Name:      podName,
			Namespace: dev,
			PvcName:   pvcName,
		}

		ginkgo.By("Create pod " + podName + ", which uses pvc " + pvcName)
		podObj, err := k8s.InitTestPod(podConf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(podObj, dev)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Check pod " + podName + " is successfully running")
		err = kClient.WaitForPodRunning(dev, podName, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())
	})

	ginkgo.It("Verify_dynamic_bindng_with_nfs_server", func() {
		ginkgo.By("Start creating nfs provisioner.")

		// Create nfs server and related rbac
		createNfsRbac(saName, crName, crbName)
		createNfsProvisioner(saName, serverName, scName)

		// Create pvc using storageclass
		pvcName := "pvc-" + common.RandSeq(5)
		pvcConf := k8s.PvcConfig{
			Name:             pvcName,
			Capacity:         "1Gi",
			StorageClassName: scName,
		}

		ginkgo.By("Create pvc " + pvcName + ", which uses storage class " + scName)
		pvcObj, err := k8s.InitPersistentVolumeClaim(pvcConf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePersistentVolumeClaim(pvcObj, dev)
		Ω(err).NotTo(HaveOccurred())
		Ω(kClient.WaitForPersistentVolumeClaimPresent(dev, pvcName, 60*time.Second)).NotTo(HaveOccurred())

		// Create pod
		podName := "pod-" + common.RandSeq(5)
		podConf := k8s.TestPodConfig{
			Name:      podName,
			Namespace: dev,
			PvcName:   pvcName,
		}

		ginkgo.By("Create pod " + podName + " with pvc " + pvcName)
		podObj, err := k8s.InitTestPod(podConf)
		Ω(err).NotTo(HaveOccurred())
		_, err = kClient.CreatePod(podObj, dev)
		Ω(err).NotTo(HaveOccurred())

		ginkgo.By("Check pod " + podName + " is successfully running")
		err = kClient.WaitForPodRunning(dev, podName, 60*time.Second)
		Ω(err).NotTo(HaveOccurred())

	})

	ginkgo.AfterEach(func() {
		tests.DumpClusterInfoIfSpecFailed(suiteName, []string{"default"})

		// Clean up nfs provisioner resources
		deleteNfsRelatedRoles(saName, crName, crbName)
		deleteNfsProvisioner(serverName, scName)
	})
})

func createNfsRbac(svaName string, crName string, crbName string) {
	// Create service account, cluster role and role binding
	ginkgo.By("Create service account " + svaName)
	_, err := kClient.CreateServiceAccount(svaName, dev)
	Ω(err).NotTo(HaveOccurred())

	nfsClusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: crName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"*"},
				Resources: []string{
					"nodes", "nodes/proxy",
					"namespaces", "services", "pods", "pods/exec",
					"deployments", "deployments/finalizers",
					"replicationcontrollers", "replicasets",
					"statefulsets", "daemonsets",
					"events", "endpoints", "configmaps", "secrets", "jobs", "cronjobs",
					"storageclasses", "persistentvolumeclaims", "persistentvolumes",
				},
				Verbs: []string{"*"},
			},
			{
				APIGroups: []string{"openebs.io"},
				Resources: []string{"*"},
				Verbs:     []string{"*"},
			},
		},
	}
	ginkgo.By("Create cluster role " + crName)
	_, err = kClient.CreateClusterRole(nfsClusterRole)
	Ω(err).NotTo(HaveOccurred())

	ginkgo.By("Create cluster role binding " + crbName)
	_, err = kClient.CreateClusterRoleBinding(crbName, crName, dev, svaName)
	Ω(err).NotTo(HaveOccurred())
}

func createNfsProvisioner(svaName string, serverName string, scName string) {
	// Create nfs provisioner
	nfsProvisioner := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverName,
			Namespace: dev,
			Labels: map[string]string{
				"name": serverName,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": serverName,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"name": serverName,
					},
				},
				Spec: v1.PodSpec{
					ServiceAccountName: svaName,
					Containers: []v1.Container{
						{
							Name:  "nfs-provisioner",
							Image: "openebs/provisioner-nfs:0.10.0",
							Env: []v1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name: "OPENEBS_NAMESPACE",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name: "OPENEBS_SERVICE_ACCOUNT",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.serviceAccountName",
										},
									},
								},
								{
									Name:  "OPENEBS_IO_ENABLE_ANALYTICS",
									Value: "true",
								},
								{
									Name:  "OPENEBS_IO_NFS_SERVER_USE_CLUSTERIP",
									Value: "true",
								},
								{
									Name:  "OPENEBS_IO_INSTALLER_TYPE",
									Value: "openebs-operator-nfs",
								},
								{
									Name:  "OPENEBS_IO_NFS_SERVER_IMG",
									Value: "openebs/nfs-server-alpine:0.10.0",
								},
							},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse("50m"),
									"memory": resource.MustParse("50M"),
								},
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse("200m"),
									"memory": resource.MustParse("200M"),
								},
							},
						},
					},
				},
			},
		},
	}

	ginkgo.By("Create nfs provisioner " + serverName)
	_, err := kClient.CreateDeployment(nfsProvisioner, dev)
	Ω(err).NotTo(HaveOccurred())

	// Create storage class
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
			Annotations: map[string]string{
				"openebs.io/cas-type":   "nfsrwx",
				"cas.openebs.io/config": "- name: NFSServerType\n  value: \"kernel\"\n- name: BackendStorageClass\n  value: \"standard\"\n",
			},
		},
		Provisioner: "openebs.io/nfsrwx",
	}

	ginkgo.By("Create storage class " + scName)
	_, err = kClient.CreateStorageClass(sc)
	Ω(err).NotTo(HaveOccurred())
}

func deleteNfsRelatedRoles(serviceAccount string, clusterRole string, clusterRoleBinding string) {
	ginkgo.By("Deleting NFS related roles and bindings")
	err := kClient.DeleteClusterRoleBindings(clusterRoleBinding)
	if err != nil && !k8serrors.IsNotFound(err) {
		Ω(err).NotTo(HaveOccurred())
	}

	err2 := kClient.DeleteClusterRole(clusterRole)
	if err2 != nil && !k8serrors.IsNotFound(err2) {
		Ω(err2).NotTo(HaveOccurred())
	}

	err3 := kClient.DeleteServiceAccount(serviceAccount, dev)
	if err3 != nil && !k8serrors.IsNotFound(err3) {
		Ω(err3).NotTo(HaveOccurred())
	}
}

func deleteNfsProvisioner(deployName string, scName string) {
	ginkgo.By("Deleting NFS deployment and storage class")
	err := kClient.DeleteDeployment(deployName, dev)
	if err != nil && !k8serrors.IsNotFound(err) {
		Ω(err).NotTo(HaveOccurred())
	}
	err2 := kClient.DeleteStorageClass(scName)
	if err2 != nil && !k8serrors.IsNotFound(err2) {
		Ω(err2).NotTo(HaveOccurred())
	}
}
