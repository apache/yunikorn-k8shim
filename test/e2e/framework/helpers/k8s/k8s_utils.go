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

package k8s

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	podutil "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"

	"k8s.io/apimachinery/pkg/util/wait"

	v1 "k8s.io/api/core/v1"
	authv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

const portForwardPort = 9080

var fw *portforward.PortForwarder
var lock = &sync.Mutex{}

type KubeCtl struct {
	clientSet      *kubernetes.Clientset
	kubeConfigPath string
	kubeConfig     *rest.Config
}

type PortForwardAPodRequest struct {
	// Kube config
	RestConfig *rest.Config
	// Pod to port-forward traffic for
	Pod v1.Pod
	// Local port to expose traffic to pod's target port
	LocalPort int
	// Target port for the pod
	PodPort int
	// Streams to configure where to read/write input and output
	Streams genericclioptions.IOStreams
	// StopCh is the channel used to stop the port forward process
	StopCh <-chan struct{}
	// ReadyCh communicates when the tunnel is ready to receive traffic
	ReadyCh chan struct{}
}

// findKubeConfig finds path from env:KUBECONFIG or ~/.kube/config
func (k *KubeCtl) findKubeConfig() error {
	env := os.Getenv("KUBECONFIG")
	if env != "" {
		k.kubeConfigPath = env
		return nil
	}
	var err error
	k.kubeConfigPath, err = expand(configmanager.YuniKornTestConfig.KubeConfig)
	return err
}

func expand(path string) (string, error) {
	if len(path) == 0 {
		return path, nil
	}

	if path[0] != '~' {
		return path, nil
	}

	if len(path) > 1 && path[1] != '/' && path[1] != '\\' {
		return "", errors.New("cannot expand user-specific home dir")
	}

	dir, err1 := os.UserHomeDir()
	if err1 != nil {
		return "", err1
	}

	return filepath.Join(dir, path[1:]), nil
}

func (k *KubeCtl) SetClient() error {
	var err error
	if k.findKubeConfig() != nil {
		return err
	}
	k.kubeConfig, err = clientcmd.BuildConfigFromFlags("", k.kubeConfigPath)
	if err != nil {
		return err
	}
	k.clientSet, err = kubernetes.NewForConfig(k.kubeConfig)
	return err
}

func (k *KubeCtl) GetClient() *kubernetes.Clientset {
	return k.clientSet
}

func (k *KubeCtl) GetKubeConfig() (*rest.Config, error) {
	if k.kubeConfig != nil {
		return k.kubeConfig, nil
	}
	return nil, errors.New("kubeconfig is nil")
}

func (k *KubeCtl) GetPods(namespace string) (*v1.PodList, error) {
	return k.clientSet.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
}

func (k *KubeCtl) GetPod(name, namespace string) (*v1.Pod, error) {
	return k.clientSet.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) GetSchedulerPod() (string, error) {
	podNameList, err := k.GetPodNamesFromNS(configmanager.YuniKornTestConfig.YkNamespace)
	if err != nil {
		return "", err
	}
	for _, podName := range podNameList {
		if strings.Contains(podName, configmanager.YKScheduler) {
			return podName, nil
		}
	}
	return "", errors.New("YK scheduler pod not found")
}

func (k *KubeCtl) KillPortForwardProcess() {
	if fw != nil {
		fw.Close()
		fw = nil
	}
}

func (k *KubeCtl) UpdatePodWithAnnotation(pod *v1.Pod, namespace, annotationKey, annotationVal string) (*v1.Pod, error) {
	annotations := map[string]string{}
	if pod.Annotations != nil {
		annotations = pod.Annotations
	}
	annotations[annotationKey] = annotationVal
	pod.Annotations = annotations
	return k.clientSet.CoreV1().Pods(namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
}

func (k *KubeCtl) DeletePodAnnotation(pod *v1.Pod, namespace, annotation string) (*v1.Pod, error) {
	annotations := pod.Annotations
	delete(annotations, annotation)
	pod.Annotations = annotations
	return k.clientSet.CoreV1().Pods(namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
}

func (k *KubeCtl) GetPodNamesFromNS(namespace string) ([]string, error) {
	var s []string
	Pods, err := k.GetPods(namespace)
	if err != nil {
		return nil, err
	}
	for _, each := range Pods.Items {
		s = append(s, each.Name)
	}
	return s, nil
}

func SetPortForwarder(req PortForwardAPodRequest, dialer httpstream.Dialer, ports []string) error {
	lock.Lock()
	defer lock.Unlock()

	var err error
	if fw == nil {
		fw, err = portforward.New(dialer, []string{fmt.Sprintf("%d:%d", req.LocalPort, req.PodPort)}, req.StopCh, req.ReadyCh, req.Streams.Out, req.Streams.ErrOut)
	}
	return err
}

func (k *KubeCtl) PortForwardPod(req PortForwardAPodRequest) error {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward",
		req.Pod.Namespace, req.Pod.Name)
	hostIP := strings.TrimLeft(req.RestConfig.Host, "htps:/")

	transport, upgrader, err := spdy.RoundTripperFor(req.RestConfig)
	if err != nil {
		return err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, &url.URL{Scheme: "https", Path: path, Host: hostIP})
	err = SetPortForwarder(req, dialer, []string{fmt.Sprintf("%d:%d", req.LocalPort, req.PodPort)})
	if err != nil {
		return err
	}
	return fw.ForwardPorts()
}

func (k *KubeCtl) PortForwardYkSchedulerPod() error {
	if fw != nil {
		fmt.Printf("port-forward is already running")
		return nil
	}
	stopCh := make(chan struct{}, 1)
	readyCh := make(chan struct{})
	errCh := make(chan error)

	stream := genericclioptions.IOStreams{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}

	go func() {
		schedulerPodName, err := k.GetSchedulerPod()
		if err != nil {
			errCh <- fmt.Errorf("unable to get %s to begin port-forwarding", schedulerPodName)
			return
		}
		err = k.PortForwardPod(PortForwardAPodRequest{
			RestConfig: k.kubeConfig,
			Pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      schedulerPodName,
					Namespace: configmanager.YuniKornTestConfig.YkNamespace,
				},
			},
			LocalPort: portForwardPort,
			PodPort:   portForwardPort,
			Streams:   stream,
			StopCh:    stopCh,
			ReadyCh:   readyCh,
		})
		if err != nil {
			errCh <- fmt.Errorf("unable to port-forward %s", schedulerPodName)
		}
	}()

	select {
	case <-readyCh:
		fmt.Printf("Port-forwarding traffic for %s...", configmanager.YKScheduler)
	case err := <-errCh:
		return err
	}
	return nil
}

func (k *KubeCtl) GetService(serviceName string, namespace string) (*v1.Service, error) {
	return k.clientSet.CoreV1().Services(namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
}

// Func to create a namespace provided a name
func (k *KubeCtl) CreateNamespace(namespace string, annotations map[string]string) (*v1.Namespace, error) {
	return k.clientSet.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:        namespace,
			Labels:      map[string]string{"Name": namespace},
			Annotations: annotations,
		},
		Spec:   v1.NamespaceSpec{},
		Status: v1.NamespaceStatus{},
	}, metav1.CreateOptions{})
}

func (k *KubeCtl) DeleteNamespace(namespace string) error {
	return k.clientSet.CoreV1().Namespaces().Delete(context.TODO(), namespace, metav1.DeleteOptions{})
}

func (k *KubeCtl) TearDownNamespace(namespace string) error {
	// Delete all pods
	var pods, err = k.GetPodNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range pods {
		err = k.DeletePod(each, namespace)
		if err != nil {
			return err
		}
	}

	// Delete namespace
	return k.clientSet.CoreV1().Namespaces().Delete(context.TODO(), namespace, metav1.DeleteOptions{})
}

func GetConfigMapObj(yamlPath string) (*v1.ConfigMap, error) {
	c, err := common.Yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	return c.(*v1.ConfigMap), err
}

func (k *KubeCtl) CreateConfigMap(cMap *v1.ConfigMap, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Create(context.TODO(), cMap, metav1.CreateOptions{})
}

func (k *KubeCtl) GetConfigMap(name string, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) UpdateConfigMap(cMap *v1.ConfigMap, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Update(context.TODO(), cMap, metav1.UpdateOptions{})
}

func (k *KubeCtl) DeleteConfigMap(cName string, namespace string) error {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Delete(context.TODO(), cName, metav1.DeleteOptions{})
}

func GetPodObj(yamlPath string) (*v1.Pod, error) {
	o, err := common.Yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	return o.(*v1.Pod), err
}

func (k *KubeCtl) CreatePod(pod *v1.Pod, namespace string) (*v1.Pod, error) {
	return k.clientSet.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
}

func (k *KubeCtl) DeletePod(podName string, namespace string) error {
	err := k.clientSet.CoreV1().Pods(namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	err = k.WaitForPodTerminated(namespace, podName, 60*time.Second)
	return err
}

// return a condition function that indicates whether the given pod is
// currently in desired state
func (k *KubeCtl) isPodInDesiredState(podName string, namespace string, state v1.PodPhase) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := k.clientSet.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case state:
			return true, nil
		case v1.PodUnknown:
			return false, fmt.Errorf("pod is in unknown state")
		}
		return false, nil
	}
}

// Return a condition function that indicates if the pod is NOT in the given namespace
func (k *KubeCtl) isPodNotInNS(podName string, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		podNames, err := k.GetPodNamesFromNS(namespace)
		if err != nil {
			return false, err
		}
		for _, name := range podNames {
			if podName == name {
				return false, nil
			}
		}
		return true, nil
	}
}

func (k *KubeCtl) WaitForPodTerminated(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout*time.Second, k.isPodNotInNS(podName, namespace))
}

// Poll up to timeout seconds for pod to enter running state.
// Returns an error if the pod never enters the running state.
func (k *KubeCtl) WaitForPodRunning(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, k.isPodInDesiredState(podName, namespace, v1.PodRunning))
}

func (k *KubeCtl) WaitForPodPending(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, k.isPodInDesiredState(podName, namespace, v1.PodPending))
}

func (k *KubeCtl) WaitForPodSucceeded(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, k.isPodInDesiredState(podName, namespace, v1.PodSucceeded))
}

func (k *KubeCtl) WaitForPodFailed(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, k.isPodInDesiredState(podName, namespace, v1.PodFailed))
}

// Returns the list of currently scheduled or running pods in `namespace` with the given selector
func (k *KubeCtl) ListPods(namespace string, selector string) (*v1.PodList, error) {
	listOptions := metav1.ListOptions{LabelSelector: selector}
	podList, err := k.clientSet.CoreV1().Pods(namespace).List(context.TODO(), listOptions)

	if err != nil {
		return nil, err
	}
	return podList, nil
}

// Wait up to timeout seconds for all pods in 'namespace' with given 'selector' to enter running state.
// Returns an error if no pods are found or not all discovered pods enter running state.
func (k *KubeCtl) WaitForPodBySelectorRunning(namespace string, selector string, timeout int) error {
	podList, err := k.ListPods(namespace, selector)
	if err != nil {
		return err
	}
	if len(podList.Items) == 0 {
		return fmt.Errorf("no pods in %s with selector %s", namespace, selector)
	}

	for _, pod := range podList.Items {
		if err := k.WaitForPodRunning(namespace, pod.Name, time.Duration(timeout)*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func (k *KubeCtl) CreateSecret(secret *v1.Secret, namespace string) (*v1.Secret, error) {
	return k.clientSet.CoreV1().Secrets(namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
}

func GetSecretObj(yamlPath string) (*v1.Secret, error) {
	o, err := common.Yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	return o.(*v1.Secret), err
}

func (k *KubeCtl) CreateServiceAccount(accountName string, namespace string) (*v1.ServiceAccount, error) {
	return k.clientSet.CoreV1().ServiceAccounts(namespace).Create(context.TODO(), &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: accountName},
	}, metav1.CreateOptions{})
}

func (k *KubeCtl) DeleteServiceAccount(accountName string, namespace string) error {
	return k.clientSet.CoreV1().ServiceAccounts(namespace).Delete(context.TODO(), accountName, metav1.DeleteOptions{})
}

func (k *KubeCtl) CreateClusterRoleBinding(
	roleName string,
	role string,
	namespace string,
	serviceAccount string) (*authv1.ClusterRoleBinding, error) {
	return k.clientSet.RbacV1().ClusterRoleBindings().Create(context.TODO(), &authv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Subjects: []authv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccount,
				Namespace: namespace,
			},
		},
		RoleRef: authv1.RoleRef{Name: role, Kind: "ClusterRole"},
	}, metav1.CreateOptions{})
}

func (k *KubeCtl) DeleteClusterRoleBindings(roleName string) error {
	return k.clientSet.RbacV1().ClusterRoleBindings().Delete(context.TODO(), roleName, metav1.DeleteOptions{})
}

func (k *KubeCtl) GetConfigMaps(namespace string, cMapName string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), cMapName, metav1.GetOptions{})
}

func (k *KubeCtl) UpdateConfigMaps(namespace string, cMap *v1.ConfigMap) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Update(context.TODO(), cMap, metav1.UpdateOptions{})
}

func (k *KubeCtl) DeleteConfigMaps(namespace string, cMapName string) error {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Delete(context.TODO(), cMapName, metav1.DeleteOptions{})
}

func (k *KubeCtl) CreateConfigMaps(namespace string, cMap *v1.ConfigMap) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Create(context.TODO(), cMap, metav1.CreateOptions{})
}

func (k *KubeCtl) GetEvents(namespace string) (*v1.EventList, error) {
	return k.clientSet.CoreV1().Events(namespace).List(context.TODO(), metav1.ListOptions{})
}

// CreateTestPodAction returns a closure that creates a pause pod upon invocation.
func (k *KubeCtl) CreateTestPodAction(pod *v1.Pod, namespace string) Action {
	return func() error {
		_, err := k.CreatePod(pod, namespace)
		return err
	}
}

// WaitForSchedulerAfterAction performs the provided action and then waits for
// scheduler to act on the given pod.
func (k *KubeCtl) WaitForSchedulerAfterAction(action Action, ns, podName string, expectSuccess bool) (bool, error) {
	predicate := ScheduleFailureEvent(podName)
	if expectSuccess {
		predicate = ScheduleSuccessEvent(ns, podName, "" /* any node */)
	}
	return ObserveEventAfterAction(k.clientSet, ns, predicate, action)
}

// PodScheduled checks if the pod has been scheduled
func (k *KubeCtl) PodScheduled(podNamespace, podName string) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := k.GetPod(podName, podNamespace)
		if err != nil {
			// This could be a connection error so retry.
			return false, nil
		}
		if pod.Spec.NodeName == "" {
			return false, nil
		}
		return true, nil
	}
}

func (k *KubeCtl) WaitForPodScheduled(namespace string, podName string, timeout time.Duration) error {
	return wait.PollImmediate(time.Second, timeout, k.PodScheduled(namespace, podName))
}

// PodUnschedulable returns a condition function that returns true if the given pod
// gets unschedulable status.
func (k *KubeCtl) PodUnschedulable(podNamespace, podName string) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := k.GetPod(podName, podNamespace)
		if err != nil {
			// This could be a connection error so retry.
			return false, nil
		}
		_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
		return cond != nil && cond.Status == v1.ConditionFalse &&
			cond.Reason == v1.PodReasonUnschedulable, nil
	}
}

// WaitForPodUnschedulable waits for a pod to fail scheduling and returns
// an error if it does not become unschedulable within the given timeout.
func (k *KubeCtl) WaitForPodUnschedulable(pod *v1.Pod, timeout time.Duration) error {
	return wait.PollImmediate(100*time.Millisecond, timeout, k.PodUnschedulable(pod.Namespace, pod.Name))
}

func (k *KubeCtl) UpdateYunikornSchedulerPodAnnotation(annotation string) error {
	/*
		https://github.com/kubernetes/website/commit/7fc323d45109d4213fcbfa44be04deae9a61e668#diff-7b0047f23923203a747765c0a99eeaa2
		Force sync of the config map by updating any pod annotation.
	*/
	schedPodList, err := k.ListPods(configmanager.YuniKornTestConfig.YkNamespace, fmt.Sprintf("component=%s", configmanager.YKScheduler))
	if err != nil {
		return err
	}

	if len(schedPodList.Items) != 1 {
		return errors.New("yunikorn scheduler pod is missing")
	}
	schedPod := schedPodList.Items[0]
	_, err = k.UpdatePodWithAnnotation(&schedPod, configmanager.YuniKornTestConfig.YkNamespace, annotation, annotation)
	return err
}

func (k *KubeCtl) RemoveYunikornSchedulerPodAnnotation(annotation string) error {
	/*
		https://github.com/kubernetes/website/commit/7fc323d45109d4213fcbfa44be04deae9a61e668#diff-7b0047f23923203a747765c0a99eeaa2
		Force sync of the config map by updating any pod annotation.
	*/
	schedPodList, err := k.ListPods(configmanager.YuniKornTestConfig.YkNamespace, fmt.Sprintf("component=%s", configmanager.YKScheduler))
	if err != nil {
		return err
	}

	if len(schedPodList.Items) != 1 {
		return errors.New("yunikorn scheduler pod is missing")
	}
	schedPod := schedPodList.Items[0]
	_, err = k.DeletePodAnnotation(&schedPod, configmanager.YuniKornTestConfig.YkNamespace, annotation)
	return err
}

func ApplyYamlWithKubectl(path, namespace string) error {
	cmd := exec.Command("kubectl", "apply", "-f", path, "-n", namespace)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	// if err != nil, isn't represent yaml format error.
	// it only represent the cmd.Run() fail.
	err := cmd.Run()
	// if yaml format error, errStr will show the detail
	errStr := stderr.String()
	if err != nil && errStr != "" {
		return fmt.Errorf("apply fail with %s", errStr)
	}
	return nil
}
