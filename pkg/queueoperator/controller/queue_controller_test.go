//go:build envtest

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

package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.yaml.in/yaml/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/queueconfig"
)

const testTargetNamespace = "default"

func newReconciler() *QueueReconciler {
	return &QueueReconciler{
		Client:          k8sClient,
		Scheme:          k8sClient.Scheme(),
		Recorder:        record.NewFakeRecorder(100),
		TargetNamespace: testTargetNamespace,
		PartitionName:   "default",
	}
}

func doReconcile(ctx context.Context, r *QueueReconciler) {
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "trigger", Namespace: "default"},
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

func getConfigMapYAML(ctx context.Context) queueconfig.SchedulerConfig {
	cm := &corev1.ConfigMap{}
	ExpectWithOffset(1, k8sClient.Get(ctx, types.NamespacedName{
		Name:      ConfigMapName,
		Namespace: testTargetNamespace,
	}, cm)).To(Succeed())

	var config queueconfig.SchedulerConfig
	ExpectWithOffset(1, yaml.Unmarshal([]byte(cm.Data[ConfigMapQueueKey]), &config)).To(Succeed())
	return config
}

func deleteAllQueues(ctx context.Context) {
	queueList := &queuev1alpha1.QueueList{}
	Expect(k8sClient.List(ctx, queueList)).To(Succeed())
	for i := range queueList.Items {
		_ = k8sClient.Delete(ctx, &queueList.Items[i])
	}
	Eventually(func() int {
		list := &queuev1alpha1.QueueList{}
		Expect(k8sClient.List(ctx, list)).To(Succeed())
		return len(list.Items)
	}, 5*time.Second, 200*time.Millisecond).Should(Equal(0))
}

func deleteConfigMap(ctx context.Context) {
	cm := &corev1.ConfigMap{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      ConfigMapName,
		Namespace: testTargetNamespace,
	}, cm)
	if err == nil {
		_ = k8sClient.Delete(ctx, cm)
	}
}

var _ = Describe("Queue Controller", func() {
	ctx := context.Background()

	AfterEach(func() {
		deleteAllQueues(ctx)
		deleteConfigMap(ctx)
	})

	Context("When reconciling a single Queue CR", func() {
		It("should create a ConfigMap with correct content", func() {
			By("creating a Queue CR")
			queue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{Name: "team-a"},
				},
			}
			Expect(k8sClient.Create(ctx, queue)).To(Succeed())

			By("reconciling")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying ConfigMap exists with correct structure")
			config := getConfigMapYAML(ctx)
			Expect(config.Partitions).To(HaveLen(1))
			Expect(config.Partitions[0].Name).To(Equal("default"))
			Expect(config.Partitions[0].Queues).To(HaveLen(1))

			root := config.Partitions[0].Queues[0]
			Expect(root.Name).To(Equal("root"))
			Expect(root.Parent).To(BeTrue())
			Expect(root.SubmitACL).To(Equal("*"))
			Expect(root.Queues).To(HaveLen(1))
			Expect(root.Queues[0].Name).To(Equal("team-a"))
		})
	})

	Context("When reconciling multiple Queue CRs", func() {
		It("should include all queues as children of root", func() {
			By("creating multiple Queue CRs")
			for _, name := range []string{"team-a", "team-b", "team-c"} {
				queue := &queuev1alpha1.Queue{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
					Spec: queuev1alpha1.QueueSpec{
						Queue: queuev1alpha1.QueueConfig{Name: name},
					},
				}
				Expect(k8sClient.Create(ctx, queue)).To(Succeed())
			}

			By("reconciling")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying all queues are under root")
			config := getConfigMapYAML(ctx)
			root := config.Partitions[0].Queues[0]
			Expect(root.Queues).To(HaveLen(3))
			names := make([]string, len(root.Queues))
			for i, q := range root.Queues {
				names[i] = q.Name
			}
			Expect(names).To(ContainElements("team-a", "team-b", "team-c"))
		})
	})

	Context("When duplicate queue names exist", func() {
		It("should keep the oldest and skip the duplicate", func() {
			By("creating the first Queue CR")
			queue1 := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a-first", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:      "team-a",
						SubmitACL: "first-acl",
					},
				},
			}
			Expect(k8sClient.Create(ctx, queue1)).To(Succeed())

			time.Sleep(50 * time.Millisecond)

			By("creating a second Queue CR with the same queue name")
			queue2 := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a-second", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:      "team-a",
						SubmitACL: "second-acl",
					},
				},
			}
			Expect(k8sClient.Create(ctx, queue2)).To(Succeed())

			By("reconciling")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying only the oldest queue is present")
			config := getConfigMapYAML(ctx)
			root := config.Partitions[0].Queues[0]
			Expect(root.Queues).To(HaveLen(1))
			Expect(root.Queues[0].Name).To(Equal("team-a"))
			Expect(root.Queues[0].SubmitACL).To(Equal("first-acl"))
		})
	})

	Context("When no Queue CRs exist", func() {
		It("should create a ConfigMap with empty root queue", func() {
			By("reconciling with no Queue CRs")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying ConfigMap has root with no children")
			config := getConfigMapYAML(ctx)
			root := config.Partitions[0].Queues[0]
			Expect(root.Name).To(Equal("root"))
			Expect(root.Queues).To(BeEmpty())
		})
	})

	Context("When a Queue CR is updated", func() {
		It("should update the ConfigMap on re-reconcile", func() {
			By("creating a Queue CR")
			queue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:      "team-a",
						SubmitACL: "original-acl",
					},
				},
			}
			Expect(k8sClient.Create(ctx, queue)).To(Succeed())

			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying original content")
			config := getConfigMapYAML(ctx)
			Expect(config.Partitions[0].Queues[0].Queues[0].SubmitACL).To(Equal("original-acl"))

			By("updating the Queue CR")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "team-a", Namespace: "default"}, queue)).To(Succeed())
			queue.Spec.Queue.SubmitACL = "updated-acl"
			Expect(k8sClient.Update(ctx, queue)).To(Succeed())

			By("re-reconciling")
			doReconcile(ctx, r)

			By("verifying updated content")
			config = getConfigMapYAML(ctx)
			Expect(config.Partitions[0].Queues[0].Queues[0].SubmitACL).To(Equal("updated-acl"))
		})
	})

	Context("When a Queue CR has a complex hierarchy", func() {
		It("should preserve the full nested structure in the ConfigMap", func() {
			By("creating a complex Queue CR")
			parentTrue := true
			maxApps := uint64(100)
			queue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "team-complex", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:   "team-complex",
						Parent: &parentTrue,
						Resources: &queuev1alpha1.Resources{
							Guaranteed: map[string]string{"memory": "4G", "vcore": "4"},
							Max:        map[string]string{"memory": "16G", "vcore": "16"},
						},
						Properties: map[string]string{
							"application.sort.policy": "fair",
						},
						Queues: []queuev1alpha1.QueueConfig{
							{
								Name:            "dev",
								MaxApplications: &maxApps,
								Parent:          &parentTrue,
								Queues: []queuev1alpha1.QueueConfig{
									// Child max-apps must be > 0 whenever the
									// parent's max-apps is set; YuniKorn's
									// checkQueueMaxApplications enforces this.
									{Name: "sandbox", MaxApplications: func() *uint64 { v := uint64(50); return &v }()},
								},
							},
							{
								Name:      "prod",
								SubmitACL: "prod-team",
							},
						},
						Limits: []queuev1alpha1.Limit{
							{
								Limit:        "user limit",
								Users:        []string{"*"},
								MaxResources: map[string]string{"memory": "2G"},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, queue)).To(Succeed())

			By("reconciling")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying the full hierarchy")
			config := getConfigMapYAML(ctx)
			root := config.Partitions[0].Queues[0]
			Expect(root.Queues).To(HaveLen(1))

			teamQ := root.Queues[0]
			Expect(teamQ.Name).To(Equal("team-complex"))
			Expect(teamQ.Parent).To(BeTrue())
			Expect(teamQ.Resources).NotTo(BeNil())
			Expect(teamQ.Resources.Guaranteed).To(HaveKeyWithValue("memory", "4G"))
			Expect(teamQ.Resources.Max).To(HaveKeyWithValue("vcore", "16"))
			Expect(teamQ.Properties).To(HaveKeyWithValue("application.sort.policy", "fair"))
			Expect(teamQ.Queues).To(HaveLen(2))

			devQ := teamQ.Queues[0]
			Expect(devQ.Name).To(Equal("dev"))
			Expect(devQ.MaxApplications).To(Equal(uint64(100)))
			Expect(devQ.Queues).To(HaveLen(1))
			Expect(devQ.Queues[0].Name).To(Equal("sandbox"))
			Expect(devQ.Queues[0].MaxApplications).To(Equal(uint64(50)))

			prodQ := teamQ.Queues[1]
			Expect(prodQ.Name).To(Equal("prod"))
			Expect(prodQ.SubmitACL).To(Equal("prod-team"))

			Expect(teamQ.Limits).To(HaveLen(1))
			Expect(teamQ.Limits[0].Limit).To(Equal("user limit"))
			Expect(teamQ.Limits[0].Users).To(Equal([]string{"*"}))
			Expect(teamQ.Limits[0].MaxResources).To(HaveKeyWithValue("memory", "2G"))
		})
	})

	Context("When a Queue CR is deleted", func() {
		It("should remove the queue from the ConfigMap on re-reconcile", func() {
			By("creating two Queue CRs")
			for _, name := range []string{"team-a", "team-b"} {
				queue := &queuev1alpha1.Queue{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
					Spec: queuev1alpha1.QueueSpec{
						Queue: queuev1alpha1.QueueConfig{Name: name},
					},
				}
				Expect(k8sClient.Create(ctx, queue)).To(Succeed())
			}

			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying both queues in ConfigMap")
			config := getConfigMapYAML(ctx)
			Expect(config.Partitions[0].Queues[0].Queues).To(HaveLen(2))

			By("deleting one Queue CR")
			toDelete := &queuev1alpha1.Queue{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "team-a", Namespace: "default"}, toDelete)).To(Succeed())
			Expect(k8sClient.Delete(ctx, toDelete)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: "team-a", Namespace: "default"}, &queuev1alpha1.Queue{}))
			}, 5*time.Second, 200*time.Millisecond).Should(BeTrue())

			By("re-reconciling")
			doReconcile(ctx, r)

			By("verifying only team-b remains")
			config = getConfigMapYAML(ctx)
			root := config.Partitions[0].Queues[0]
			Expect(root.Queues).To(HaveLen(1))
			Expect(root.Queues[0].Name).To(Equal("team-b"))
		})
	})

	// Defense-in-depth: even if a malformed CR slips past the webhook (e.g.
	// webhook disabled, failurePolicy=Ignore softfail, direct etcd write),
	// the reconciler MUST refuse to write the resulting ConfigMap so the
	// scheduler is never handed a config that crashloops it.
	Context("When the assembled YuniKorn config is invalid", func() {
		It("refuses to write the ConfigMap, marks the CR Degraded, and trips the webhook-missed counter", func() {
			By("creating a CR whose merged config violates a YuniKorn rule (parent-max-apps without child-max-apps)")
			parentTrue := true
			parentMax := uint64(100)
			queue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-team", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:            "bad-team",
						Parent:          &parentTrue,
						MaxApplications: &parentMax,
						// child has no max-apps -> violates checkQueueMaxApplications
						Queues: []queuev1alpha1.QueueConfig{{Name: "child"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, queue)).To(Succeed())

			// envtest does not install the validating webhook in this
			// suite, so this CR reaches etcd unvetted — the exact
			// "webhook was bypassed" scenario the new metric is built
			// to detect. Snapshot the counter so we can prove the
			// reconciler ticks it by exactly one.
			missedBefore := testutil.ToFloat64(webhookMissedInvalidTotal.WithLabelValues("validate"))

			By("reconciling")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying the ConfigMap was NOT written")
			cm := &corev1.ConfigMap{}
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name: ConfigMapName, Namespace: testTargetNamespace,
			}, cm)
			Expect(errors.IsNotFound(err)).To(BeTrue(),
				"ConfigMap must not exist when merged config is invalid; got err=%v", err)

			By("verifying the CR is marked Degraded with the validation reason")
			fresh := &queuev1alpha1.Queue{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "bad-team", Namespace: "default",
			}, fresh)).To(Succeed())
			var degraded *metav1.Condition
			for i := range fresh.Status.Conditions {
				if fresh.Status.Conditions[i].Type == queuev1alpha1.ConditionTypeDegraded {
					degraded = &fresh.Status.Conditions[i]
				}
			}
			Expect(degraded).NotTo(BeNil(), "Degraded condition must be set")
			Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
			Expect(degraded.Message).To(ContainSubstring("YuniKorn config"))

			By("verifying webhook_missed_invalid_total{stage=validate} incremented by exactly 1")
			missedAfter := testutil.ToFloat64(webhookMissedInvalidTotal.WithLabelValues("validate"))
			Expect(missedAfter-missedBefore).To(Equal(float64(1)),
				"reconciler must tick webhook_missed_invalid_total when it catches a config the webhook should have rejected")
		})

		// Per-CR attribution: a bad CR poisoning the merged config must NOT
		// mark unrelated valid CRs as Degraded. Only the CR that fails its
		// own single-CR YuniKorn validation should carry the Degraded
		// condition. The good CR's status is intentionally left untouched
		// (the previous last-known-good ConfigMap is still in place).
		It("only marks the offending CR Degraded; the unrelated good CR is untouched", func() {
			By("creating one good CR and one bad CR (parent-max-apps without child-max-apps)")
			goodQueue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "good-team", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{Name: "good-team"},
				},
			}
			Expect(k8sClient.Create(ctx, goodQueue)).To(Succeed())

			parentTrue := true
			parentMax := uint64(100)
			badQueue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-team", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:            "bad-team",
						Parent:          &parentTrue,
						MaxApplications: &parentMax,
						Queues:          []queuev1alpha1.QueueConfig{{Name: "child"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, badQueue)).To(Succeed())

			By("reconciling")
			r := newReconciler()
			doReconcile(ctx, r)

			By("verifying ConfigMap was NOT written (refuse-to-overwrite on invalid merge)")
			cm := &corev1.ConfigMap{}
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name: ConfigMapName, Namespace: testTargetNamespace,
			}, cm)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			By("verifying the bad CR is Degraded with reason=InvalidConfig")
			freshBad := &queuev1alpha1.Queue{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "bad-team", Namespace: "default",
			}, freshBad)).To(Succeed())
			var badDegraded *metav1.Condition
			for i := range freshBad.Status.Conditions {
				if freshBad.Status.Conditions[i].Type == queuev1alpha1.ConditionTypeDegraded {
					badDegraded = &freshBad.Status.Conditions[i]
				}
			}
			Expect(badDegraded).NotTo(BeNil(), "bad CR must have Degraded condition")
			Expect(badDegraded.Status).To(Equal(metav1.ConditionTrue))
			Expect(badDegraded.Reason).To(Equal(StatusReasonInvalidConfig))
			Expect(badDegraded.Message).To(ContainSubstring("YuniKorn config"))

			By("verifying the GOOD CR has NO Degraded=True condition")
			freshGood := &queuev1alpha1.Queue{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "good-team", Namespace: "default",
			}, freshGood)).To(Succeed())
			for i := range freshGood.Status.Conditions {
				c := freshGood.Status.Conditions[i]
				if c.Type == queuev1alpha1.ConditionTypeDegraded {
					Expect(c.Status).NotTo(Equal(metav1.ConditionTrue),
						"unrelated good CR must not be marked Degraded=True; got reason=%q msg=%q",
						c.Reason, c.Message)
				}
			}
		})

		It("recovers (writes the ConfigMap) once the CR is fixed", func() {
			By("creating a bad CR that will be rejected by the merged validator")
			parentTrue := true
			parentMax := uint64(100)
			queue := &queuev1alpha1.Queue{
				ObjectMeta: metav1.ObjectMeta{Name: "recovery-team", Namespace: "default"},
				Spec: queuev1alpha1.QueueSpec{
					Queue: queuev1alpha1.QueueConfig{
						Name:            "recovery-team",
						Parent:          &parentTrue,
						MaxApplications: &parentMax,
						Queues:          []queuev1alpha1.QueueConfig{{Name: "child"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, queue)).To(Succeed())

			r := newReconciler()
			doReconcile(ctx, r)

			By("confirming the ConfigMap was refused")
			cm := &corev1.ConfigMap{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{
				Name: ConfigMapName, Namespace: testTargetNamespace,
			}, cm))).To(BeTrue())

			By("fixing the CR (giving the child a non-zero max-apps)")
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "recovery-team", Namespace: "default",
			}, queue)).To(Succeed())
			childMax := uint64(50)
			queue.Spec.Queue.Queues[0].MaxApplications = &childMax
			Expect(k8sClient.Update(ctx, queue)).To(Succeed())

			doReconcile(ctx, r)

			By("verifying the ConfigMap is now written")
			config := getConfigMapYAML(ctx)
			Expect(config.Partitions[0].Queues[0].Queues).To(HaveLen(1))
			Expect(config.Partitions[0].Queues[0].Queues[0].Name).To(Equal("recovery-team"))
		})
	})
})
