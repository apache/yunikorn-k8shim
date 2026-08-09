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

package client

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type SchedulerKubeClient struct {
	clientSet *kubernetes.Clientset
	configs   *rest.Config
}

// Every client identifies the concern it serves in its user agent so that traffic can be
// attributed on the API server side. The concern comes first to allow prefix matching, the
// build version is appended by UserAgent().
const (
	UserAgentAdmissionController = "yunikorn-admission-controller"

	userAgentBootstrap = "yunikorn-bootstrap"
	userAgentWrites    = "yunikorn-scheduler/writes"
	userAgentInformers = "yunikorn-scheduler/informers"
	userAgentEvents    = "yunikorn-scheduler/events"
)

// UserAgent appends the build version to the concern, e.g. "yunikorn-scheduler/writes
// (1.7.0)". The version is only set in release builds, it is left out when empty.
func UserAgent(concern string) string {
	return userAgentWithVersion(concern, conf.GetBuildInfoMap()["buildVersion"])
}

func userAgentWithVersion(concern, version string) string {
	if version == "" {
		return concern
	}
	return fmt.Sprintf("%s (%s)", concern, version)
}

// rateLimitPolicy turns the configured qps and burst into the values to set on a REST config
// and a description of the resulting limit for logging.
// A qps <= 0 disables client side rate limiting: this is expressed as a negative QPS, which
// client-go treats as "create no rate limiter", while a QPS of 0 silently falls back to its
// own defaults of 5 QPS / 10 burst (see RESTClientFor and NewForConfigAndClient in
// client-go). A QPS set without a burst is rejected by client-go, so the burst defaults to
// the configured qps.
func rateLimitPolicy(concern string, qps, burst int) (effectiveQPS float32, effectiveBurst int, description string) {
	if qps <= 0 {
		if burst > 0 {
			log.Log(log.ShimClient).Warn("ignoring configured burst, client side rate limiting is disabled unless qps is set to a positive value",
				zap.String("concern", concern),
				zap.Int("qps", qps),
				zap.Int("burst", burst))
		}
		return -1, 0, "unlimited"
	}
	if burst <= 0 {
		log.Log(log.ShimClient).Warn("no burst configured, defaulting burst to the configured qps",
			zap.String("concern", concern),
			zap.Int("qps", qps))
		burst = qps
	}
	return float32(qps), burst, fmt.Sprintf("%d qps / %d burst", qps, burst)
}

// newRestConfig creates a REST config for a single purpose client, see rateLimitPolicy for
// the way the configured qps and burst are applied.
func newRestConfig(kc string, qps, burst int, concern string) *rest.Config {
	config := CreateRestConfigOrDie(kc)
	config.UserAgent = UserAgent(concern)

	var rateLimit string
	config.QPS, config.Burst, rateLimit = rateLimitPolicy(concern, qps, burst)

	log.Log(log.ShimClient).Info("creating Kubernetes client",
		zap.String("concern", concern),
		zap.String("rateLimit", rateLimit),
		zap.String("userAgent", config.UserAgent))
	return config
}

func newClientSetOrDie(config *rest.Config) *kubernetes.Clientset {
	configuredClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Log(log.ShimClient).Fatal("failed to get Clientset", zap.Error(err))
	}
	return configuredClient
}

// newSchedulerKubeClient creates the client used for all writes to the API server. Beside
// the pod writes it also backs the volume binder and the predicate framework handle, so the
// configured QPS and burst are an opt-in policy cap on those paths as well.
// The QPS and burst are read from the configuration singleton, like all other configuration
// access. In the shim this means the client must be created after UpdateConfigMaps has run
// for the values set by the operator to take effect. The admission controller never
// populates the scheduler configuration, so its clients always run on the defaults: that is
// intended, its traffic is low volume and it previously ran on a 1000/1000 limiter which was
// never engaged either.
func newSchedulerKubeClient(kc string, concern string) SchedulerKubeClient {
	schedulerConf := conf.GetSchedulerConf()

	config := newRestConfig(kc, schedulerConf.KubeQPS, schedulerConf.KubeBurst, concern)
	return SchedulerKubeClient{
		clientSet: newClientSetOrDie(config),
		configs:   config,
	}
}

// NewInformerClientSet creates the client backing the informer factories. Informer traffic
// is never rate limited on the client side: watches are long lived and a relist must not be
// throttled behind the write path.
func NewInformerClientSet(kc string) kubernetes.Interface {
	return newClientSetOrDie(newRestConfig(kc, 0, 0, userAgentInformers))
}

// NewEventsClientSet creates the client backing the event broadcaster sink. It is never rate
// limited: the configured event rate is enforced by the sink, which sheds the events above
// it, see NewRateLimitedEventSink. A limiter here as well would delay the events which are
// kept for as long as a storm lasts.
// This client retries a server side rejection like every other client. Only the client built
// by NewEventSink fails fast on it, that behaviour only makes sense paired with the sink
// which mutes the events for the window the server asked for.
func NewEventsClientSet(kc string) kubernetes.Interface {
	return newEventsClientSet(kc, nil, clock.RealClock{})
}

func CreateRestConfigOrDie(kc string) *rest.Config {
	config, err := CreateRestConfig(kc)
	if err != nil {
		log.Log(log.ShimClient).Fatal("unable to create REST config, aborting", zap.Error(err))
	}
	return config
}

func CreateRestConfig(kc string) (*rest.Config, error) {
	// attempt to use in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil && err != rest.ErrNotInCluster {
		log.Log(log.ShimClient).Error("failed to create REST config", zap.Error(err))
		return nil, err
	}
	if config != nil {
		return config, nil
	}

	// fall back to kubeconfig if present
	if kc == "" {
		kc = conf.GetDefaultKubeConfigPath()
	}
	log.Log(log.ShimClient).Info(fmt.Sprintf("Not running inside Kubernetes; using KUBECONFIG at %s", kc))
	config, err = clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		log.Log(log.ShimClient).Error("failed to create kubeClient configs", zap.Error(err))
		return config, err
	}
	return config, nil
}

func (nc SchedulerKubeClient) GetClientSet() kubernetes.Interface {
	return nc.clientSet
}

func (nc SchedulerKubeClient) GetConfigs() *rest.Config {
	return nc.configs
}

func (nc SchedulerKubeClient) Bind(pod *v1.Pod, hostID string) error {
	log.Log(log.ShimClient).Info("bind pod to node",
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)),
		zap.String("nodeID", hostID))

	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Bind(
		context.Background(),
		&v1.Binding{ObjectMeta: apis.ObjectMeta{
			Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: hostID,
			},
		},
		apis.CreateOptions{}); err != nil {
		log.Log(log.ShimClient).Error("failed to bind pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return err
	}
	return nil
}

func (nc SchedulerKubeClient) Create(pod *v1.Pod) (*v1.Pod, error) {
	return nc.clientSet.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, apis.CreateOptions{})
}

func (nc SchedulerKubeClient) Delete(pod *v1.Pod) error {
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, apis.DeleteOptions{}); err != nil {
		log.Log(log.ShimClient).Warn("failed to delete pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return err
	}
	return nil
}

func (nc SchedulerKubeClient) GetConfigMap(namespace string, name string) (*v1.ConfigMap, error) {
	configmap, err := nc.clientSet.CoreV1().ConfigMaps(namespace).Get(context.Background(), name, apis.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		log.Log(log.ShimClient).Warn("failed to get configmap",
			zap.String("namespace", namespace),
			zap.String("name", name),
			zap.Error(err))
		return nil, err
	}
	return configmap, nil
}

func (nc SchedulerKubeClient) Get(podNamespace string, podName string) (*v1.Pod, error) {
	pod, err := nc.clientSet.CoreV1().Pods(podNamespace).Get(context.Background(), podName, apis.GetOptions{})
	if err != nil {
		log.Log(log.ShimClient).Warn("failed to get pod",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(err))
		return nil, err
	}
	return pod, nil
}

func (nc SchedulerKubeClient) UpdatePod(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error) {
	var updatedPod *v1.Pod
	var updateErr error
	// Retrieve the latest version of Pod before attempting update
	// RetryOnConflict uses exponential backoff to avoid exhausting the API server
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestPod, getErr := nc.clientSet.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, apis.GetOptions{})
		if getErr != nil {
			log.Log(log.ShimClient).Warn("failed to get latest version of Pod",
				zap.Error(getErr))
		}
		// allow mutator to update pod
		podMutator(latestPod)
		if updatedPod, updateErr = nc.clientSet.CoreV1().Pods(pod.Namespace).Update(context.Background(), latestPod, apis.UpdateOptions{}); updateErr != nil {
			log.Log(log.ShimClient).Warn("failed to update pod",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.Error(updateErr))
			return updateErr
		}
		return nil
	})
	if retryErr != nil {
		log.Log(log.ShimClient).Error("Update pod failed",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(retryErr))
		return pod, retryErr
	}
	log.Log(log.ShimClient).Info("Successfully updated pod",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name))
	return updatedPod, nil
}

func (nc SchedulerKubeClient) UpdateStatus(pod *v1.Pod) (*v1.Pod, error) {
	var updatedPod *v1.Pod
	var updateErr error
	newPodStatus := pod.Status
	// In case of conflicts, retry using the logic in
	// https://github.com/kubernetes/client-go/blob/v0.21.1/examples/create-update-delete-deployment/main.go#L118-L121
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Retrieve the latest version of Pod before attempting status update
		// RetryOnConflict uses exponential backoff to avoid exhausting the API server
		latestPod, getErr := nc.clientSet.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, apis.GetOptions{})
		if getErr != nil {
			log.Log(log.ShimClient).Warn("failed to get latest version of Pod",
				zap.Error(getErr))
		}
		latestPod.Status = newPodStatus

		if updatedPod, updateErr = nc.clientSet.CoreV1().Pods(pod.Namespace).UpdateStatus(context.Background(), latestPod, apis.UpdateOptions{}); updateErr != nil {
			log.Log(log.ShimClient).Warn("failed to update pod status",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.Error(updateErr))
			return updateErr
		}
		return nil
	})
	if retryErr != nil {
		log.Log(log.ShimClient).Error("Update pod status failed",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.Error(retryErr))
		return pod, retryErr
	}
	log.Log(log.ShimClient).Info("Successfully updated pod status",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.Stringer("newStatus", &pod.Status))
	return updatedPod, nil
}
