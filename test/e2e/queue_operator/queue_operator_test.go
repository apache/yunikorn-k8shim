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

package queue_operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	queuev1alpha1 "github.com/apache/yunikorn-k8shim/pkg/queueoperator/api/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/controller"
	"github.com/apache/yunikorn-k8shim/pkg/queueoperator/webhookpki"
)

const (
	operatorNamespace = "yunikorn"
	testNamespace     = "queue-operator-e2e"
	operatorName      = "yunikorn-queue-operator"
	timeout           = 2 * time.Minute
	interval          = time.Second
)

var _ = ginkgo.Describe("Queue operator lifecycle", ginkgo.Ordered, ginkgo.Serial, func() {
	ctx := context.Background()

	ginkgo.BeforeAll(func() {
		gomega.Expect(apiClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}})).To(gomega.Succeed())
	})

	ginkgo.AfterAll(func() {
		gomega.Expect(apiClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}})).To(gomega.Succeed())
	})

	ginkgo.It("starts two trusted endpoints and rejects invalid queues", func() {
		gomega.Eventually(func(g gomega.Gomega) {
			deployment := &appsv1.Deployment{}
			g.Expect(apiClient.Get(ctx, types.NamespacedName{Name: operatorName, Namespace: operatorNamespace}, deployment)).To(gomega.Succeed())
			g.Expect(deployment.Status.ReadyReplicas).To(gomega.Equal(int32(2)))
		}, timeout, interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
			g.Expect(apiClient.Get(ctx, types.NamespacedName{Name: operatorName}, configuration)).To(gomega.Succeed())
			g.Expect(configuration.Webhooks).To(gomega.HaveLen(1))
			g.Expect(configuration.Webhooks[0].FailurePolicy).NotTo(gomega.BeNil())
			g.Expect(*configuration.Webhooks[0].FailurePolicy).To(gomega.Equal(admissionregistrationv1.Fail))
			g.Expect(configuration.Webhooks[0].ClientConfig.CABundle).NotTo(gomega.BeEmpty())
		}, timeout, interval).Should(gomega.Succeed())

		invalid := queue("metadata-name", "different-queue")
		err := apiClient.Create(ctx, invalid)
		gomega.Expect(apierrors.IsInvalid(err)).To(gomega.BeTrue(), fmt.Sprintf("expected webhook rejection, got %v", err))

		configuration := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		gomega.Expect(apiClient.Get(ctx, types.NamespacedName{Name: operatorName}, configuration)).To(gomega.Succeed())
		configuration.Webhooks[0].FailurePolicy = ptr.To(admissionregistrationv1.Ignore)
		gomega.Expect(apiClient.Update(ctx, configuration)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &admissionregistrationv1.ValidatingWebhookConfiguration{}
			g.Expect(apiClient.Get(ctx, types.NamespacedName{Name: operatorName}, fresh)).To(gomega.Succeed())
			g.Expect(fresh.Webhooks[0].FailurePolicy).NotTo(gomega.BeNil())
			g.Expect(*fresh.Webhooks[0].FailurePolicy).To(gomega.Equal(admissionregistrationv1.Fail))
		}, timeout, interval).Should(gomega.Succeed())
	})

	ginkgo.It("requires explicit ConfigMap adoption and preserves unrelated data", func() {
		queue := queue("team-a", "team-a")
		gomega.Expect(apiClient.Create(ctx, queue)).To(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &queuev1alpha1.Queue{}
			g.Expect(apiClient.Get(ctx, client.ObjectKeyFromObject(queue), fresh)).To(gomega.Succeed())
			condition := conditionByType(fresh.Status.Conditions, queuev1alpha1.ConditionTypeDegraded)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(condition.Reason).To(gomega.Equal(controller.StatusReasonConfigMapOwnershipConflict))
		}, timeout, interval).Should(gomega.Succeed())

		configMap := &corev1.ConfigMap{}
		gomega.Expect(apiClient.Get(ctx, types.NamespacedName{Name: controller.ConfigMapName, Namespace: operatorNamespace}, configMap)).To(gomega.Succeed())
		gomega.Expect(configMap.Data[controller.ConfigMapQueueKey]).To(gomega.Equal("unmanaged"))
		gomega.Expect(configMap.Data["unrelated"]).To(gomega.Equal("preserved"))
		if configMap.Annotations == nil {
			configMap.Annotations = make(map[string]string)
		}
		configMap.Annotations[controller.ConfigMapManagedAnnotation] = controller.ConfigMapManagedValue
		configMap.Labels = map[string]string{"preserved": "true"}
		configMap.BinaryData = map[string][]byte{"preserved": []byte("value")}
		gomega.Expect(apiClient.Update(ctx, configMap)).To(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			freshQueue := &queuev1alpha1.Queue{}
			g.Expect(apiClient.Get(ctx, client.ObjectKeyFromObject(queue), freshQueue)).To(gomega.Succeed())
			available := conditionByType(freshQueue.Status.Conditions, queuev1alpha1.ConditionTypeAvailable)
			g.Expect(available).NotTo(gomega.BeNil())
			g.Expect(available.Status).To(gomega.Equal(metav1.ConditionTrue))
			freshConfigMap := &corev1.ConfigMap{}
			g.Expect(apiClient.Get(ctx, types.NamespacedName{Name: controller.ConfigMapName, Namespace: operatorNamespace}, freshConfigMap)).To(gomega.Succeed())
			g.Expect(freshConfigMap.Data[controller.ConfigMapQueueKey]).To(gomega.ContainSubstring("team-a"))
			g.Expect(freshConfigMap.Data["unrelated"]).To(gomega.Equal("preserved"))
			g.Expect(freshConfigMap.Labels["preserved"]).To(gomega.Equal("true"))
			g.Expect(freshConfigMap.BinaryData["preserved"]).To(gomega.Equal([]byte("value")))
		}, timeout, interval).Should(gomega.Succeed())
	})

	ginkgo.It("applies overrides and repairs drift and deletion", func() {
		deployment := &appsv1.Deployment{}
		gomega.Expect(apiClient.Get(ctx, types.NamespacedName{Name: operatorName, Namespace: operatorNamespace}, deployment)).To(gomega.Succeed())
		setEnv(&deployment.Spec.Template.Spec.Containers[0], controller.PartitionNameEnvVar, "queue-operator-e2e")
		setEnv(&deployment.Spec.Template.Spec.Containers[0], controller.NodeSortPolicyEnvVar, "type: fair\nresourceweights:\n  memory: 2")
		setEnv(&deployment.Spec.Template.Spec.Containers[0], controller.RootPropertiesEnvVar, "application.sort.policy: fair")
		gomega.Expect(apiClient.Update(ctx, deployment)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &appsv1.Deployment{}
			g.Expect(apiClient.Get(ctx, client.ObjectKeyFromObject(deployment), fresh)).To(gomega.Succeed())
			g.Expect(fresh.Status.UpdatedReplicas).To(gomega.Equal(int32(2)))
			g.Expect(fresh.Status.ReadyReplicas).To(gomega.Equal(int32(2)))
			g.Expect(fresh.Status.ObservedGeneration).To(gomega.Equal(fresh.Generation))
		}, timeout, interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			config := renderedConfig(ctx, g)
			g.Expect(config.Partitions[0].Name).To(gomega.Equal("queue-operator-e2e"))
			g.Expect(config.Partitions[0].NodeSortPolicy.Type).To(gomega.Equal("fair"))
			g.Expect(config.Partitions[0].NodeSortPolicy.ResourceWeights["memory"]).To(gomega.Equal(2.0))
			g.Expect(config.Partitions[0].Queues[0].Properties["application.sort.policy"]).To(gomega.Equal("fair"))
		}, timeout, interval).Should(gomega.Succeed())

		configMap := &corev1.ConfigMap{}
		key := types.NamespacedName{Name: controller.ConfigMapName, Namespace: operatorNamespace}
		gomega.Expect(apiClient.Get(ctx, key, configMap)).To(gomega.Succeed())
		configMap.Data[controller.ConfigMapQueueKey] = "drifted"
		gomega.Expect(apiClient.Update(ctx, configMap)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &corev1.ConfigMap{}
			g.Expect(apiClient.Get(ctx, key, fresh)).To(gomega.Succeed())
			g.Expect(fresh.Data[controller.ConfigMapQueueKey]).NotTo(gomega.Equal("drifted"))
		}, timeout, interval).Should(gomega.Succeed())

		gomega.Expect(apiClient.Get(ctx, key, configMap)).To(gomega.Succeed())
		gomega.Expect(apiClient.Delete(ctx, configMap)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &corev1.ConfigMap{}
			g.Expect(apiClient.Get(ctx, key, fresh)).To(gomega.Succeed())
			g.Expect(fresh.Annotations[controller.ConfigMapManagedAnnotation]).To(gomega.Equal(controller.ConfigMapManagedValue))
			g.Expect(fresh.Data[controller.ConfigMapQueueKey]).To(gomega.ContainSubstring("team-a"))
		}, timeout, interval).Should(gomega.Succeed())
	})

	ginkgo.It("preserves CA state across both replica replacements", func() {
		secret := &corev1.Secret{}
		secretKey := types.NamespacedName{Name: webhookpki.DefaultSecretName, Namespace: operatorNamespace}
		gomega.Expect(apiClient.Get(ctx, secretKey, secret)).To(gomega.Succeed())
		fingerprint := secretFingerprint(secret.Data)

		pods := &corev1.PodList{}
		gomega.Expect(apiClient.List(ctx, pods, client.InNamespace(operatorNamespace), client.MatchingLabels{"component": operatorName})).To(gomega.Succeed())
		gomega.Expect(pods.Items).To(gomega.HaveLen(2))
		originalNames := []string{pods.Items[0].Name, pods.Items[1].Name}
		for _, podName := range originalNames {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: operatorNamespace}}
			gomega.Expect(apiClient.Delete(ctx, pod)).To(gomega.Succeed())
			gomega.Eventually(func(g gomega.Gomega) {
				freshPods := &corev1.PodList{}
				g.Expect(apiClient.List(ctx, freshPods, client.InNamespace(operatorNamespace), client.MatchingLabels{"component": operatorName})).To(gomega.Succeed())
				ready := 0
				for i := range freshPods.Items {
					if podReady(&freshPods.Items[i]) {
						ready++
					}
				}
				g.Expect(ready).To(gomega.Equal(2))
			}, timeout, interval).Should(gomega.Succeed())
		}

		freshSecret := &corev1.Secret{}
		gomega.Expect(apiClient.Get(ctx, secretKey, freshSecret)).To(gomega.Succeed())
		gomega.Expect(secretFingerprint(freshSecret.Data)).To(gomega.Equal(fingerprint))
		gomega.Eventually(func() error {
			invalid := queue("replacement-check", "different")
			err := apiClient.Create(ctx, invalid)
			if apierrors.IsInvalid(err) {
				return nil
			}
			if err == nil {
				deleteErr := apiClient.Delete(ctx, invalid)
				if deleteErr != nil {
					return gomega.StopTrying("invalid Queue was admitted after pod replacement and cleanup failed").Wrap(deleteErr)
				}
				return gomega.StopTrying("invalid Queue was admitted after pod replacement")
			}
			return fmt.Errorf("webhook admission was not yet available: %w", err)
		}, timeout, interval).Should(gomega.Succeed())
	})

	ginkgo.It("reconciles queue deletion to an empty root", func() {
		queue := &queuev1alpha1.Queue{ObjectMeta: metav1.ObjectMeta{Name: "team-a", Namespace: testNamespace}}
		gomega.Expect(apiClient.Delete(ctx, queue)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			config := renderedConfig(ctx, g)
			g.Expect(config.Partitions[0].Queues).To(gomega.HaveLen(1))
			g.Expect(config.Partitions[0].Queues[0].Queues).To(gomega.BeEmpty())
		}, timeout, interval).Should(gomega.Succeed())
	})
})

func queue(name, queueName string) *queuev1alpha1.Queue {
	return &queuev1alpha1.Queue{
		TypeMeta:   metav1.TypeMeta{APIVersion: queuev1alpha1.GroupVersion.String(), Kind: "Queue"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: queuev1alpha1.QueueSpec{Queue: queuev1alpha1.QueueConfig{
			Name: nameOr(queueName, name), Parent: ptr.To(true), SubmitACL: "*",
		}},
	}
}

func nameOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func conditionByType(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func renderedConfig(ctx context.Context, g gomega.Gomega) *configs.SchedulerConfig {
	configMap := &corev1.ConfigMap{}
	g.Expect(apiClient.Get(ctx, types.NamespacedName{Name: controller.ConfigMapName, Namespace: operatorNamespace}, configMap)).To(gomega.Succeed())
	config, err := configs.LoadSchedulerConfigFromByteArray([]byte(configMap.Data[controller.ConfigMapQueueKey]))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return config
}

func secretFingerprint(data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var result bytes.Buffer
	for _, key := range keys {
		result.WriteString(key)
		result.WriteByte(0)
		result.Write(data[key])
		result.WriteByte(0)
	}
	digest := sha256.Sum256(result.Bytes())
	return fmt.Sprintf("%x", digest)
}

func setEnv(container *corev1.Container, name, value string) {
	for i := range container.Env {
		if container.Env[i].Name == name {
			container.Env[i].Value = value
			container.Env[i].ValueFrom = nil
			return
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{Name: name, Value: value})
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
