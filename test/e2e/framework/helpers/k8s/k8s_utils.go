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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	authv1 "k8s.io/api/rbac/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/retry"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
)

const portForwardPort = 9080

type WorkloadType string

const (
	Deployment  = "Deployment"
	StatefulSet = "StatefulSet"
	DaemonSet   = "DaemonSet"
	ReplicaSet  = "ReplicaSet"
	Job         = "Job"
	CronJob     = "CronJob"
)

var fw *portforward.PortForwarder
var lock = &locking.Mutex{}

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

// GetKubernetesVersion returns the version info from the Kubernetes server
func (k *KubeCtl) GetKubernetesVersion() (*version.Info, error) {
	k8sVer, err := k.clientSet.Discovery().ServerVersion()
	if err != nil {
		return k8sVer, err
	}
	return k8sVer, nil
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

func (k *KubeCtl) GetPodsByOptions(options metav1.ListOptions) (*v1.PodList, error) {
	return k.clientSet.CoreV1().Pods("").List(context.TODO(), options)
}

func (k *KubeCtl) GetDeployments(namespace string) (*appsv1.DeploymentList, error) {
	return k.clientSet.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{})
}

func (k *KubeCtl) GetDaemonSets(namespace string) (*appsv1.DaemonSetList, error) {
	return k.clientSet.AppsV1().DaemonSets(namespace).List(context.TODO(), metav1.ListOptions{})
}

func (k *KubeCtl) GetStatefulSets(namespace string) (*appsv1.StatefulSetList, error) {
	return k.clientSet.AppsV1().StatefulSets(namespace).List(context.TODO(), metav1.ListOptions{})
}

func (k *KubeCtl) GetJobs(namespace string) (*batchv1.JobList, error) {
	return k.clientSet.BatchV1().Jobs(namespace).List(context.TODO(), metav1.ListOptions{})
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

func (k *KubeCtl) UpdatePod(pod *v1.Pod, namespace string) (*v1.Pod, error) {
	return k.clientSet.CoreV1().Pods(namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
}

func (k *KubeCtl) PatchPod(pod *v1.Pod, namespace string, patch []map[string]interface{}, subresources ...string) (*v1.Pod, error) {
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}
	return k.clientSet.CoreV1().Pods(namespace).Patch(
		context.TODO(),
		pod.Name,
		types.JSONPatchType,
		patchBytes,
		metav1.PatchOptions{},
		subresources...,
	)
}

func (k *KubeCtl) ModifyResourceUsage(pod *v1.Pod, namespace string, newVcore int64, newMemory int64) (*v1.Pod, error) {
	patch := []map[string]interface{}{
		{
			"op":    "replace",
			"path":  "/spec/containers/0/resources/requests/cpu",
			"value": strconv.FormatInt(newVcore, 10) + "m",
		},
		{
			"op":    "replace",
			"path":  "/spec/containers/0/resources/requests/memory",
			"value": strconv.FormatInt(newMemory, 10) + "Mi",
		},
	}
	return k.PatchPod(pod, namespace, patch, "resize")
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

func (k *KubeCtl) GetDeploymentNamesFromNS(namespace string) ([]string, error) {
	var s []string
	deployments, err := k.GetDeployments(namespace)
	if err != nil {
		return nil, err
	}
	for _, each := range deployments.Items {
		s = append(s, each.Name)
	}
	return s, nil
}

func (k *KubeCtl) GetDaemonSetNamesFromNS(namespace string) ([]string, error) {
	var s []string
	daemonSets, err := k.GetDaemonSets(namespace)
	if err != nil {
		return nil, err
	}
	for _, each := range daemonSets.Items {
		s = append(s, each.Name)
	}
	return s, nil
}

func (k *KubeCtl) GetReplicaSetNamesFromNS(namespace string) ([]string, error) {
	var s []string
	replicaSets, err := k.GetReplicaSets(namespace)
	if err != nil {
		return nil, err
	}
	for _, each := range replicaSets.Items {
		s = append(s, each.Name)
	}
	return s, nil
}

func (k *KubeCtl) GetStatefulSetNamesFromNS(namespace string) ([]string, error) {
	var s []string
	statefulSets, err := k.GetStatefulSets(namespace)
	if err != nil {
		return nil, err
	}
	for _, each := range statefulSets.Items {
		s = append(s, each.Name)
	}
	return s, nil
}

func (k *KubeCtl) GetJobNamesFromNS(namespace string) ([]string, error) {
	var s []string
	jobs, err := k.GetJobs(namespace)
	if err != nil {
		return nil, err
	}
	for _, each := range jobs.Items {
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

func (k *KubeCtl) CreateService(service *v1.Service, namespace string) (*v1.Service, error) {
	return k.clientSet.CoreV1().Services(namespace).Create(context.TODO(), service, metav1.CreateOptions{})
}

// Func to create a namespace provided a name
func (k *KubeCtl) CreateNamespace(namespace string, annotations map[string]string) (*v1.Namespace, error) {
	// create namespace
	ns, err := k.clientSet.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:        namespace,
			Labels:      map[string]string{"Name": namespace},
			Annotations: annotations,
		},
		Spec:   v1.NamespaceSpec{},
		Status: v1.NamespaceStatus{},
	}, metav1.CreateOptions{})
	if err != nil {
		return ns, err
	}

	// wait for default service account to be present
	if err = k.WaitForServiceAccountPresent(namespace, "default", 60*time.Second); err != nil {
		return nil, err
	}

	return ns, nil
}

// Func to update a namespace with annotations
func (k *KubeCtl) UpdateNamespace(namespace string, annotations map[string]string) (*v1.Namespace, error) {
	ns, err := k.clientSet.CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{})
	if err != nil {
		return ns, err
	}
	nsCopy := ns.DeepCopy()
	nsCopy.Annotations = annotations

	// update namespace
	ns, err = k.clientSet.CoreV1().Namespaces().Update(context.TODO(), nsCopy, metav1.UpdateOptions{})
	if err != nil {
		return ns, err
	}
	return ns, nil
}

func (k *KubeCtl) WaitForServiceAccountPresent(namespace string, svcAcctName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Second, timeout, false, k.isServiceAccountPresent(namespace, svcAcctName).WithContext())
}

func (k *KubeCtl) isServiceAccountPresent(namespace string, svcAcctName string) wait.ConditionFunc {
	return func() (bool, error) {
		_, err := k.clientSet.CoreV1().ServiceAccounts(namespace).Get(context.Background(), svcAcctName, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
}

func (k *KubeCtl) DeleteNamespace(namespace string) error {
	var secs int64 = 0
	return k.clientSet.CoreV1().Namespaces().Delete(context.TODO(), namespace, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
}

func (k *KubeCtl) TearDownNamespace(namespace string) error {
	var err error
	err = k.DeleteDeployments(namespace)
	if err != nil {
		return err
	}

	err = k.DeleteDaemonSets(namespace)
	if err != nil {
		return err
	}

	err = k.DeleteReplicaSets(namespace)
	if err != nil {
		return err
	}

	err = k.DeleteStatefulSets(namespace)
	if err != nil {
		return err
	}

	err = k.DeleteJobs(namespace)
	if err != nil {
		return err
	}

	err = k.DeletePods(namespace)
	if err != nil {
		return err
	}

	err = k.DeletePVCs(namespace)
	if err != nil {
		return err
	}

	// Delete namespace
	return k.clientSet.CoreV1().Namespaces().Delete(context.TODO(), namespace, metav1.DeleteOptions{})
}

func (k *KubeCtl) DeleteDeployments(namespace string) error {
	// Delete all Deployment in namespace
	var deployments, err = k.GetDeploymentNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range deployments {
		err = k.DeleteDeployment(each, namespace)
		if err != nil {
			if statusErr, ok := err.(*k8serrors.StatusError); ok {
				if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete Deployment %s - reason is %s, it "+
						"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeleteDaemonSets(namespace string) error {
	// Delete all DaemonSet in namespace
	var daemonSets, err = k.GetDaemonSetNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range daemonSets {
		err = k.DeleteDaemonSet(each, namespace)
		if err != nil {
			if statusErr, ok := err.(*k8serrors.StatusError); ok {
				if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete DaemonSet %s - reason is %s, it "+
						"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeleteReplicaSets(namespace string) error {
	// Delete all ReplicaSet in namespace
	var replicaSets, err = k.GetReplicaSetNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range replicaSets {
		err = k.DeleteReplicaSet(each, namespace)
		if err != nil {
			if statusErr, ok := err.(*k8serrors.StatusError); ok {
				if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete ReplicaSet %s - reason is %s, it "+
						"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeleteStatefulSets(namespace string) error {
	// Delete all StatefulSet in namespace
	var statefulSets, err = k.GetStatefulSetNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range statefulSets {
		err = k.DeleteStatefulSet(each, namespace)
		if err != nil {
			if statusErr, ok := err.(*k8serrors.StatusError); ok {
				if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete StatefulSet %s - reason is %s, it "+
						"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeleteJobs(namespace string) error {
	// Delete all jobs
	var jobs, err = k.GetJobNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range jobs {
		err = k.DeleteJob(each, namespace)
		if err != nil {
			if statusErr, ok := err.(*k8serrors.StatusError); ok {
				if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete job %s - reason is %s, it "+
						"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeletePods(namespace string) error {
	// Delete all pods
	var pods, err = k.GetPodNamesFromNS(namespace)
	if err != nil {
		return err
	}
	for _, each := range pods {
		err = k.DeletePod(each, namespace)
		if err != nil {
			if statusErr, ok := err.(*k8serrors.StatusError); ok {
				if statusErr.ErrStatus.Reason == metav1.StatusReasonNotFound {
					fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pod %s - reason is %s, it "+
						"has been deleted in the meantime\n", each, statusErr.ErrStatus.Reason)
					continue
				}
			}
			return err
		}
	}
	return nil
}

func GetConfigMapObj(yamlPath string) (*v1.ConfigMap, error) {
	c, err := common.Yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	configMap, ok := c.(*v1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("failed to convert object to ConfigMap")
	}
	return configMap, nil
}

func (k *KubeCtl) LogNamespaceInfo(file *os.File, ns string) error {
	fmt.Fprintf(file, "Log namespace info, ns: %s\n", ns)
	cmd := fmt.Sprintf("kubectl cluster-info dump --namespaces=%s", ns)
	out, runErr := common.RunShellCmdForeground(cmd)
	if runErr != nil {
		return runErr
	}
	_, err := fmt.Fprintln(file, out)
	return err
}

func (k *KubeCtl) LogPodsInfo(file *os.File) error {
	fmt.Fprintln(file, "Log pods info:")
	pods, err := k.GetPodsByOptions(metav1.ListOptions{})
	if err != nil {
		return err
	} else {
		fmt.Fprintf(file, "Pod count is %d\n", len(pods.Items))
		for _, pod := range pods.Items {
			fmt.Fprintf(file, "Pod name is %s\n", pod.Name)
			fmt.Fprintf(file, "Pod details: %s\n", pod.String())
		}
	}
	return nil
}

func (k *KubeCtl) LogNodesInfo(file *os.File) error {
	fmt.Fprintln(file, "Log nodes info:")
	nodes, err := k.GetNodes()
	if err != nil {
		return err
	}
	fmt.Fprintf(file, "Node count is %d\n", len(nodes.Items))
	for _, node := range nodes.Items {
		fmt.Fprintf(file, "Node: %s\n", node.Name)
		nodeInfo, err := k.DescribeNode(node)
		if err != nil {
			fmt.Fprintf(file, "Failed to describe node: %s, err: %v\n", node.Name, err)
		}
		fmt.Fprintln(file, nodeInfo)
	}
	return nil
}

func (k *KubeCtl) GetPodLogs(podName string, namespace string, containerName string) ([]byte, error) {
	options := &v1.PodLogOptions{
		Container: containerName,
	}
	logsReq := k.clientSet.CoreV1().Pods(namespace).GetLogs(podName, options)
	logsBytes, err := logsReq.DoRaw(context.TODO())
	if err != nil {
		return []byte{}, err
	}

	return logsBytes, nil
}

func (k *KubeCtl) CreateConfigMap(cMap *v1.ConfigMap, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Create(context.TODO(), cMap, metav1.CreateOptions{})
}

func (k *KubeCtl) ConfigMapExists(name string, namespace string) (bool, error) {
	_, err := k.clientSet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (k *KubeCtl) GetConfigMap(name string, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) UpdateConfigMap(cMap *v1.ConfigMap, namespace string) (*v1.ConfigMap, error) {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Update(context.TODO(), cMap, metav1.UpdateOptions{})
}

func (k *KubeCtl) StartConfigMapInformer(namespace string, stopChan <-chan struct{}, eventHandler cache.ResourceEventHandler) error {
	informerFactory := informers.NewSharedInformerFactoryWithOptions(k.clientSet, 0, informers.WithNamespace(namespace))
	informerFactory.Start(stopChan)
	configMapInformer := informerFactory.Core().V1().ConfigMaps()
	_, err := configMapInformer.Informer().AddEventHandler(eventHandler)
	if err != nil {
		log.Log(log.AdmissionConf).Error("Error adding event handler", zap.Error(err))
		return err
	}
	go configMapInformer.Informer().Run(stopChan)
	if err := utils.WaitForCondition(func() bool {
		return configMapInformer.Informer().HasSynced()
	}, time.Second, 30*time.Second); err != nil {
		return err
	}

	return nil
}

func (k *KubeCtl) DeleteConfigMap(cName string, namespace string) error {
	return k.clientSet.CoreV1().ConfigMaps(namespace).Delete(context.TODO(), cName, metav1.DeleteOptions{})
}

func GetPodObj(yamlPath string) (*v1.Pod, error) {
	o, err := common.Yaml2Obj(yamlPath)
	if err != nil {
		return nil, err
	}
	pod, ok := o.(*v1.Pod)
	if !ok {
		return nil, fmt.Errorf("failed to convert object to Pod")
	}
	return pod, nil
}

func (k *KubeCtl) CreateDeployment(deployment *appsv1.Deployment, namespace string) (*appsv1.Deployment, error) {
	return k.clientSet.AppsV1().Deployments(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
}

func (k *KubeCtl) CreateStatefulSet(stetafulSet *appsv1.StatefulSet, namespace string) (*appsv1.StatefulSet, error) {
	return k.clientSet.AppsV1().StatefulSets(namespace).Create(context.TODO(), stetafulSet, metav1.CreateOptions{})
}

func (k *KubeCtl) CreateReplicaSet(replicaSet *appsv1.ReplicaSet, namespace string) (*appsv1.ReplicaSet, error) {
	return k.clientSet.AppsV1().ReplicaSets(namespace).Create(context.TODO(), replicaSet, metav1.CreateOptions{})
}

func (k *KubeCtl) CreateDaemonSet(daemonSet *appsv1.DaemonSet, namespace string) (*appsv1.DaemonSet, error) {
	return k.clientSet.AppsV1().DaemonSets(namespace).Create(context.TODO(), daemonSet, metav1.CreateOptions{})
}

func (k *KubeCtl) CreateCronJob(cronJob *batchv1.CronJob, namespace string) (*batchv1.CronJob, error) {
	return k.clientSet.BatchV1().CronJobs(namespace).Create(context.TODO(), cronJob, metav1.CreateOptions{})
}

func (k *KubeCtl) DeleteStatefulSet(name, namespace string) error {
	var secs int64 = 0
	err := k.clientSet.AppsV1().StatefulSets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
	return err
}

func (k *KubeCtl) DeleteReplicaSet(name, namespace string) error {
	var secs int64 = 0
	err := k.clientSet.AppsV1().ReplicaSets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
	return err
}

func (k *KubeCtl) DeleteDeployment(name, namespace string) error {
	var secs int64 = 0
	err := k.clientSet.AppsV1().Deployments(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
	return err
}

func (k *KubeCtl) DeleteDaemonSet(name, namespace string) error {
	var secs int64 = 0
	err := k.clientSet.AppsV1().DaemonSets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
	return err
}

func (k *KubeCtl) DeleteCronJob(name, namespace string) error {
	var secs int64 = 0
	err := k.clientSet.BatchV1().CronJobs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
	return err
}

func (k *KubeCtl) GetDeployment(name, namespace string) (*appsv1.Deployment, error) {
	return k.clientSet.AppsV1().Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) GetDaemonSet(name, namespace string) (*appsv1.DaemonSet, error) {
	return k.clientSet.AppsV1().DaemonSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) GetReplicaSet(name, namespace string) (*appsv1.ReplicaSet, error) {
	return k.clientSet.AppsV1().ReplicaSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) GetStatefulSet(name, namespace string) (*appsv1.StatefulSet, error) {
	return k.clientSet.AppsV1().StatefulSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k *KubeCtl) CreatePod(pod *v1.Pod, namespace string) (*v1.Pod, error) {
	return k.clientSet.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
}

func (k *KubeCtl) DeletePod(podName string, namespace string) error {
	var secs int64 = 0
	err := k.clientSet.CoreV1().Pods(namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
	})
	if err != nil {
		return err
	}

	err = k.WaitForPodTerminated(namespace, podName, 60*time.Second)
	return err
}

func (k *KubeCtl) DeletePodGracefully(podName string, namespace string) error {
	err := k.clientSet.CoreV1().Pods(namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	err = k.WaitForPodTerminated(namespace, podName, 120*time.Second)
	return err
}

func (k *KubeCtl) DeleteJob(jobName string, namespace string) error {
	var secs int64 = 0
	var policy = metav1.DeletePropagationForeground
	err := k.clientSet.BatchV1().Jobs(namespace).Delete(context.TODO(), jobName, metav1.DeleteOptions{
		GracePeriodSeconds: &secs,
		PropagationPolicy:  &policy,
	})
	if err != nil {
		return err
	}
	err = k.WaitForJobTerminated(namespace, jobName, 120*time.Second)
	return err
}

func (k *KubeCtl) UpdateDeployment(deployment *appsv1.Deployment, namespace string) (*appsv1.Deployment, error) {
	return k.clientSet.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
}

func (k *KubeCtl) GetReplicaSets(namespace string) (*appsv1.ReplicaSetList, error) {
	return k.clientSet.AppsV1().ReplicaSets(namespace).List(context.TODO(), metav1.ListOptions{})
}

// return a condition function that indicates whether the given pod is
// currently in desired state
func (k *KubeCtl) isPodInDesiredState(podName string, namespace string, state v1.PodPhase) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := k.clientSet.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
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

func (k *KubeCtl) isPodSelectorInNs(selector string, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		podList, err := k.ListPods(namespace, selector)
		if err != nil {
			return false, err
		}
		if len(podList.Items) > 0 {
			return true, nil
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

// Return a condition function that indicates if the job is NOT in the given namespace
func (k *KubeCtl) isJobNotInNS(jobName string, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		jobNames, err := k.GetJobNamesFromNS(namespace)
		if err != nil {
			return false, err
		}
		for _, name := range jobNames {
			if jobName == name {
				return false, nil
			}
		}
		return true, nil
	}
}

// return a condition function that indicates whether the given pod has received the expected event
func (k *KubeCtl) isPodEventTriggered(namespace string, podName string, expectedReason string) wait.ConditionFunc {
	return func() (bool, error) {
		events, err := k.clientSet.CoreV1().Events(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		eventItems := events.Items
		for _, event := range eventItems {
			if event.InvolvedObject.Name == podName && strings.Contains(event.Reason, expectedReason) {
				return true, nil
			}
		}
		return false, nil
	}
}

func (k *KubeCtl) isNumPod(namespace string, wanted int) wait.ConditionFunc {
	return func() (bool, error) {
		podList, err := k.GetPods(namespace)
		if err != nil {
			return false, err
		}
		if len(podList.Items) == wanted {
			return true, nil
		}
		return true, nil
	}
}

func (k *KubeCtl) WaitForJobPods(namespace string, jobName string, numPods int, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isNumJobPodsInDesiredState(jobName, namespace, numPods, v1.PodRunning).WithContext())
}
func (k *KubeCtl) WaitForPodEvent(namespace string, podName string, expectedReason string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodEventTriggered(namespace, podName, expectedReason).WithContext())
}

func (k *KubeCtl) WaitForPodTerminated(namespace string, podName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodNotInNS(podName, namespace).WithContext())
}

func (k *KubeCtl) WaitForJobTerminated(namespace string, jobName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isJobNotInNS(jobName, namespace).WithContext())
}

// Poll up to timeout seconds for pod to enter running state.
// Returns an error if the pod never enters the running state.
func (k *KubeCtl) WaitForPodRunning(namespace string, podName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodInDesiredState(podName, namespace, v1.PodRunning).WithContext())
}

func (k *KubeCtl) WaitForPodPending(namespace string, podName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodInDesiredState(podName, namespace, v1.PodPending).WithContext())
}

func (k *KubeCtl) WaitForPodSucceeded(namespace string, podName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodInDesiredState(podName, namespace, v1.PodSucceeded).WithContext())
}

func (k *KubeCtl) WaitForPodFailed(namespace string, podName string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodInDesiredState(podName, namespace, v1.PodFailed).WithContext())
}

func (k *KubeCtl) WaitForPodCount(namespace string, wanted int, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isNumPod(namespace, wanted).WithContext())
}

func (k *KubeCtl) WaitForPodStateStable(namespace string, podName string, timeout time.Duration) (v1.PodPhase, error) {
	var lastPhase v1.PodPhase
	samePhases := 0

	err := wait.PollUntilContextTimeout(context.TODO(), time.Second, timeout, false, k.isPodStable(namespace, podName, &samePhases, 3, &lastPhase).WithContext())
	return lastPhase, err
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

// Returns the list of currently scheduled or running pods in `namespace` with the given selector
func (k *KubeCtl) ListPodsByFieldSelector(namespace string, selector string) (*v1.PodList, error) {
	listOptions := metav1.ListOptions{FieldSelector: selector}
	podList, err := k.clientSet.CoreV1().Pods(namespace).List(context.TODO(), listOptions)

	if err != nil {
		return nil, err
	}
	return podList, nil
}

// Returns the list of pods in `namespace` with the given label selector
func (k *KubeCtl) ListPodsByLabelSelector(namespace string, selector string) (*v1.PodList, error) {
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

// Wait for all pods in 'namespace' with given 'selector' to enter succeeded state.
// Returns an error if no pods are found or not all discovered pods enter succeeded state.
func (k *KubeCtl) WaitForPodBySelectorSucceeded(namespace string, selector string, timeout time.Duration) error {
	podList, err := k.ListPods(namespace, selector)
	if err != nil {
		return err
	}
	if len(podList.Items) == 0 {
		return fmt.Errorf("no pods in %s with selector %s", namespace, selector)
	}

	for _, pod := range podList.Items {
		if err := k.WaitForPodSucceeded(namespace, pod.Name, timeout); err != nil {
			return err
		}
	}
	return nil
}

// Wait up to timeout seconds for a pod in 'namespace' with given 'selector' to exist
func (k *KubeCtl) WaitForPodBySelector(namespace string, selector string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isPodSelectorInNs(selector, namespace).WithContext())
}

func (k *KubeCtl) CreateSecret(secret *v1.Secret, namespace string) (*v1.Secret, error) {
	return k.clientSet.CoreV1().Secrets(namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
}

func (k *KubeCtl) CreateServiceAccount(accountName string, namespace string) (*v1.ServiceAccount, error) {
	return k.clientSet.CoreV1().ServiceAccounts(namespace).Create(context.TODO(), &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: accountName},
	}, metav1.CreateOptions{})
}

func (k *KubeCtl) DeleteServiceAccount(accountName string, namespace string) error {
	return k.clientSet.CoreV1().ServiceAccounts(namespace).Delete(context.TODO(), accountName, metav1.DeleteOptions{})
}

func (k *KubeCtl) CreateClusterRole(clusterRole *rbacv1.ClusterRole) (*rbacv1.ClusterRole, error) {
	return k.clientSet.RbacV1().ClusterRoles().Create(context.TODO(), clusterRole, metav1.CreateOptions{})
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

func (k *KubeCtl) DeleteClusterRole(roleName string) error {
	return k.clientSet.RbacV1().ClusterRoles().Delete(context.TODO(), roleName, metav1.DeleteOptions{})
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
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.PodScheduled(namespace, podName).WithContext())
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
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.PodUnschedulable(pod.Namespace, pod.Name).WithContext())
}

func (k *KubeCtl) CreatePriorityClass(pc *schedulingv1.PriorityClass) (*schedulingv1.PriorityClass, error) {
	return k.clientSet.SchedulingV1().PriorityClasses().Create(context.Background(), pc, metav1.CreateOptions{})
}

func (k *KubeCtl) DeletePriorityClass(priorityClassName string) error {
	return k.clientSet.SchedulingV1().PriorityClasses().Delete(context.Background(), priorityClassName, metav1.DeleteOptions{})
}

func (k *KubeCtl) CreateJob(job *batchv1.Job, namespace string) (*batchv1.Job, error) {
	return k.clientSet.BatchV1().Jobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
}

func (k *KubeCtl) WaitForJobPodsCreated(namespace string, jobName string, numPods int, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isNumJobPodsCreated(jobName, namespace, numPods).WithContext())
}

func (k *KubeCtl) WaitForJobPodsRunning(namespace string, jobName string, numPods int, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isNumJobPodsInDesiredState(jobName, namespace, numPods, v1.PodRunning).WithContext())
}

func (k *KubeCtl) WaitForJobPodsSucceeded(namespace string, jobName string, numPods int, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isNumJobPodsInDesiredState(jobName, namespace, numPods, v1.PodSucceeded).WithContext())
}

func (k *KubeCtl) WaitForPlaceholders(namespace string, podPrefix string, numPods int, timeout time.Duration, podPhase *v1.PodPhase) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*100, timeout, false, k.isNumPlaceholdersRunning(namespace, podPrefix, numPods, podPhase).WithContext())
}

func (k *KubeCtl) ListPlaceholders(namespace string, podPrefix string) ([]v1.Pod, error) {
	pods := make([]v1.Pod, 0)
	podList, lstErr := k.ListPods(namespace, "")
	if lstErr != nil {
		return pods, lstErr
	}
	for _, pod := range podList.Items {
		if strings.HasPrefix(pod.Name, podPrefix) && pod.Annotations[constants.AnnotationPlaceholderFlag] == constants.True {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

// WaitForPlaceholdersStableState used when the expected state of the placeholders cannot be properly determined in advance or not needed.
// Returns when the phase of pods does not change three times in a row.
func (k *KubeCtl) WaitForPlaceholdersStableState(namespace string, podPrefix string, timeout time.Duration) error {
	samePhases := 0
	podPhases := make(map[string]v1.PodPhase)
	return wait.PollUntilContextTimeout(context.TODO(), time.Second, timeout, false, k.arePlaceholdersStable(namespace, podPrefix, &samePhases, 3, podPhases).WithContext())
}

func (k *KubeCtl) isNumPlaceholdersRunning(namespace string, podPrefix string, num int, podPhase *v1.PodPhase) wait.ConditionFunc {
	return func() (bool, error) {
		phPods, lstErr := k.ListPlaceholders(namespace, podPrefix)
		if lstErr != nil {
			return false, lstErr
		}

		var count int
		for _, pod := range phPods {
			if podPhase == nil || *podPhase == pod.Status.Phase {
				count++
			}
		}
		return count == num, nil
	}
}

// checks the phase of all pods, returns true if nothing changes for "maxAttempt" times
func (k *KubeCtl) arePlaceholdersStable(namespace string, podPrefix string, samePhases *int,
	maxAttempts int, phases map[string]v1.PodPhase) wait.ConditionFunc {
	return func() (bool, error) {
		jobPods, lstErr := k.ListPlaceholders(namespace, podPrefix)
		if lstErr != nil {
			return false, lstErr
		}

		needRetry := false
		for _, pod := range jobPods {
			currentPhase := pod.Status.Phase
			prevPhase, ok := phases[pod.Name]
			if !ok {
				phases[pod.Name] = currentPhase
				needRetry = true
				continue
			}
			if prevPhase != currentPhase {
				needRetry = true
			}
			phases[pod.Name] = currentPhase
		}

		if needRetry {
			*samePhases = 0
		} else {
			*samePhases++
		}

		return *samePhases == maxAttempts, nil
	}
}

// checks the phase of a pod, returns true if nothing changes for "maxAttempt" times
func (k *KubeCtl) isPodStable(namespace, podName string, samePhases *int,
	maxAttempts int, lastPhase *v1.PodPhase) wait.ConditionFunc {
	return func() (bool, error) {
		pod, lstErr := k.GetPod(podName, namespace)
		if lstErr != nil {
			return false, lstErr
		}

		needRetry := false
		currentPhase := pod.Status.Phase
		if *lastPhase != currentPhase {
			needRetry = true
		}
		*lastPhase = currentPhase

		if needRetry {
			*samePhases = 0
		} else {
			*samePhases++
		}

		return *samePhases == maxAttempts, nil
	}
}

// Returns ConditionFunc that checks if at least num pods of jobName are created
func (k *KubeCtl) isNumJobPodsCreated(jobName string, namespace string, num int) wait.ConditionFunc {
	return func() (bool, error) {
		jobPods, lstErr := k.ListPods(namespace, fmt.Sprintf("job-name=%s", jobName))
		if lstErr != nil {
			return false, lstErr
		}

		return len(jobPods.Items) >= num, nil
	}
}

// Returns ConditionFunc that checks if at least num pods of jobName are running
func (k *KubeCtl) isNumJobPodsInDesiredState(jobName string, namespace string, num int, podPhase v1.PodPhase) wait.ConditionFunc {
	return func() (bool, error) {
		jobPods, lstErr := k.ListPods(namespace, fmt.Sprintf("job-name=%s", jobName))
		if lstErr != nil {
			return false, lstErr
		}
		if len(jobPods.Items) < num {
			return false, nil
		}

		counter := 0
		for _, pod := range jobPods.Items {
			isRunning, err := k.isPodInDesiredState(pod.Name, namespace, podPhase)()
			if err != nil {
				return false, err
			}
			if isRunning {
				counter++
			}
		}

		return counter >= num, nil
	}
}

func (k *KubeCtl) GetNodes() (*v1.NodeList, error) {
	return k.clientSet.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
}

func (k *KubeCtl) GetNodesCapacity(nodes v1.NodeList) map[string]v1.ResourceList {
	nodeAvailRes := make(map[string]v1.ResourceList)
	for _, node := range nodes.Items {
		nodeAvailRes[node.Name] = node.Status.Allocatable.DeepCopy()
	}

	return nodeAvailRes
}

func GetWorkerNodes(nodes v1.NodeList) []v1.Node {
	var workerNodes []v1.Node
	for _, node := range nodes.Items {
		scheduleable := true
		for _, t := range node.Spec.Taints {
			if t.Effect == v1.TaintEffectNoSchedule {
				scheduleable = false
				break
			}
		}
		if scheduleable {
			workerNodes = append(workerNodes, node)
		}
	}
	return workerNodes
}

// Sums up current resource usage in a list of pods. Non-running pods are filtered out.
func GetPodsTotalRequests(podList *v1.PodList) (reqs v1.ResourceList) {
	reqs = make(v1.ResourceList)
	for i := range podList.Items {
		pod := podList.Items[i]
		podReqs := v1.ResourceList{}
		if pod.Status.Phase == v1.PodRunning {
			podReqs, _ = resourcehelper.PodRequestsAndLimits(&pod)
		}
		for podReqName, podReqValue := range podReqs {
			if value, ok := reqs[podReqName]; !ok {
				reqs[podReqName] = podReqValue.DeepCopy()
			} else {
				value.Add(podReqValue)
				reqs[podReqName] = value
			}
		}
	}
	return
}

// GetNodesAvailRes Returns map of nodeName to list of available resource (memory and cpu only) amounts.
func (k *KubeCtl) GetNodesAvailRes(nodes v1.NodeList) map[string]v1.ResourceList {
	// Find used resources per node
	nodeUsage := make(map[string]v1.ResourceList)
	for _, node := range nodes.Items {
		nodePods, err := k.GetPodsByOptions(metav1.ListOptions{
			FieldSelector: fmt.Sprintf("spec.nodeName=%s", node.Name),
		})
		if err != nil {
			panic(err)
		}
		nodeUsage[node.Name] = GetPodsTotalRequests(nodePods)
	}

	// Initialize available resources as capacity and subtract used resources
	nodeAvailRes := make(map[string]v1.ResourceList)
	for _, node := range nodes.Items {
		resList := node.Status.Allocatable.DeepCopy()
		nodeAvailRes[node.Name] = resList

		usage := nodeUsage[node.Name]
		mem := resList.Memory().DeepCopy()
		mem.Sub(usage.Memory().DeepCopy())
		cpu := resList.Cpu().DeepCopy()
		cpu.Sub(usage.Cpu().DeepCopy())
		newRes := v1.ResourceList{"memory": mem, "cpu": cpu}
		nodeAvailRes[node.Name] = newRes
	}
	return nodeAvailRes
}

func (k *KubeCtl) DescribeNode(node v1.Node) (string, error) {
	cmd := "kubectl describe node " + node.Name
	out, runErr := common.RunShellCmdForeground(cmd)
	if runErr != nil {
		return "", runErr
	}
	return out, nil
}

func (k *KubeCtl) SetNodeLabel(name, key, value string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := k.clientSet.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		node.Labels[key] = value
		_, err = k.clientSet.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	})
}

func (k *KubeCtl) IsNodeLabelExists(name, key string) (bool, error) {
	node, err := k.clientSet.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	_, ok := node.Labels[key]
	if ok {
		return true, nil
	} else {
		return false, nil
	}
}

func (k *KubeCtl) RemoveNodeLabel(name, key, value string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := k.clientSet.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		delete(node.Labels, key)
		_, err = k.clientSet.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		return nil
	})
}

func (k *KubeCtl) TaintNode(name, key, val string, effect v1.TaintEffect) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := k.clientSet.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		t := v1.Taint{
			Effect: effect,
			Key:    key,
			Value:  val,
		}
		taints := node.Spec.Taints
		taints = append(taints, t)
		node.Spec.Taints = taints

		_, err = k.clientSet.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
		return err
	})
}

func (k *KubeCtl) TaintNodes(names []string, key, val string, effect v1.TaintEffect) error {
	for _, name := range names {
		err := k.TaintNode(name, key, val, effect)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KubeCtl) UntaintNode(name, key string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := k.clientSet.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		newTaints := make([]v1.Taint, 0)
		for _, taint := range node.Spec.Taints {
			if taint.Key != key {
				newTaints = append(newTaints, taint)
			}
		}

		node.Spec.Taints = newTaints
		_, err = k.clientSet.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
		return err
	})
}

func (k *KubeCtl) UntaintNodes(names []string, key string) error {
	for _, name := range names {
		err := k.UntaintNode(name, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func IsMasterNode(node *v1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if _, ok := common.MasterTaints[taint.Key]; ok {
			return true
		}
	}
	return false
}

func IsComputeNode(node *v1.Node) bool {
	roleNodeLabelExists := false
	for labelKey, labelValue := range node.Labels {
		if labelKey == common.RoleNodeLabel {
			roleNodeLabelExists = true
			if _, ok := common.ComputeNodeLabels[labelValue]; ok {
				return true
			}
		}
	}
	return !roleNodeLabelExists
}

func (k *KubeCtl) DeleteWorkloadAndPods(objectName string, wlType WorkloadType, namespace string) {
	ginkgo.By("Delete " + objectName + ", type is " + string(wlType) + " in namespace " + namespace)
	var err error
	switch wlType {
	case Deployment:
		err = k.DeleteDeployment(objectName, namespace)
	case StatefulSet:
		err = k.DeleteStatefulSet(objectName, namespace)
	case ReplicaSet:
		err = k.DeleteReplicaSet(objectName, namespace)
	case DaemonSet:
		err = k.DeleteDaemonSet(objectName, namespace)
	case Job:
		err = k.DeleteJob(objectName, namespace)
	case CronJob:
		err = k.DeleteCronJob(objectName, namespace)
	default:
		fmt.Fprintf(ginkgo.GinkgoWriter, "Unknown type %s, just deleting pods\n", string(wlType))
	}
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred(), "Could not delete "+objectName)

	pods, err := k.GetPods(namespace)
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred(), "Could not get pods from namespace "+namespace)
	fmt.Fprintf(ginkgo.GinkgoWriter, "Forcibly deleting remaining pods in namespace %s\n", namespace)
	for _, pod := range pods.Items {
		fmt.Fprintf(ginkgo.GinkgoWriter, "Deleting %s\n", pod.Name)
		_ = k.DeletePod(pod.Name, namespace) //nolint:errcheck
	}

	err = k.WaitForPodCount(namespace, 0, 10*time.Second)
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())
}

func (k *KubeCtl) CreatePersistentVolume(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	return k.clientSet.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
}

func (k *KubeCtl) CreatePersistentVolumeClaim(pvc *v1.PersistentVolumeClaim, ns string) (*v1.PersistentVolumeClaim, error) {
	return k.clientSet.CoreV1().PersistentVolumeClaims(ns).Create(context.TODO(), pvc, metav1.CreateOptions{})
}

func (k *KubeCtl) CreateStorageClass(sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {
	return k.clientSet.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
}

func (k *KubeCtl) GetPersistentVolume(name string) (*v1.PersistentVolume, error) {
	pv, err := k.clientSet.CoreV1().PersistentVolumes().Get(context.TODO(), name, metav1.GetOptions{})
	return pv, err
}

func (k *KubeCtl) WaitForPersistentVolumeAvailable(name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*200, timeout, true, k.isPersistentVolumeAvailable(name))
}

func (k *KubeCtl) WaitForPersistentVolumeClaimPresent(namespace string, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*200, timeout, true, k.isPersistentVolumeClaimPresent(namespace, name))
}

func (k *KubeCtl) isPersistentVolumeAvailable(name string) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		pv, err := k.GetPersistentVolume(name)
		if err != nil {
			return false, err
		}
		if pv.Status.Phase == v1.VolumeAvailable {
			return true, nil
		}
		return false, nil
	}
}

func (k *KubeCtl) isPersistentVolumeClaimPresent(namespace string, name string) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		_, err := k.clientSet.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

func (k *KubeCtl) GetPvcNameListFromNs(namespace string) ([]string, error) {
	var arr []string
	pvcList, err := k.clientSet.CoreV1().PersistentVolumeClaims(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, item := range pvcList.Items {
		arr = append(arr, item.Name)
	}
	return arr, nil
}

func (k *KubeCtl) GetPvNameListFromNs(namespace string) ([]string, error) {
	var arr []string
	pvList, err := k.clientSet.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, item := range pvList.Items {
		if item.Spec.ClaimRef.Namespace == namespace {
			arr = append(arr, item.Name)
		}
	}
	return arr, nil
}

func (k *KubeCtl) DeletePersistentVolume(pvName string) error {
	err := k.clientSet.CoreV1().PersistentVolumes().Delete(context.TODO(), pvName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (k *KubeCtl) DeletePersistentVolumeClaim(pvcName string, namespace string) error {
	err := k.clientSet.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), pvcName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (k *KubeCtl) DeletePVCs(namespace string) error {
	// Delete all PVC by namespace
	var PvcList, err = k.GetPvcNameListFromNs(namespace)
	if err != nil {
		return err
	}

	for _, each := range PvcList {
		err = k.DeletePersistentVolumeClaim(each, namespace)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeletePVs(namespace string) error {
	// Delete all PV by namespace
	var PvcList, err = k.GetPvNameListFromNs(namespace)
	if err != nil {
		return err
	}

	for _, item := range PvcList {
		err = k.DeletePersistentVolume(item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *KubeCtl) DeleteStorageClass(scName string) error {
	err := k.clientSet.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (k *KubeCtl) GetSecrets(namespace string) (*v1.SecretList, error) {
	return k.clientSet.CoreV1().Secrets(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetSecretValue retrieves the value for a specific key from a Kubernetes secret.
func (k *KubeCtl) GetSecretValue(namespace, secretName, key string) (string, error) {
	err := k.WaitForSecret(namespace, secretName, 5*time.Second)
	if err != nil {
		return "", err
	}
	secret, err := k.GetSecret(namespace, secretName)
	if err != nil {
		return "", err
	}
	// Check if the key exists in the secret
	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %s not found in secret %s", key, secretName)
	}
	return string(value), nil
}

func (k *KubeCtl) GetSecret(namespace, secretName string) (*v1.Secret, error) {
	secret, err := k.clientSet.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret, nil
}

func (k *KubeCtl) WaitForSecret(namespace, secretName string, timeout time.Duration) error {
	var cond wait.ConditionFunc // nolint:gosimple
	cond = func() (done bool, err error) {
		secret, err := k.GetSecret(namespace, secretName)
		if err != nil {
			return false, err
		}
		if secret != nil {
			return true, nil
		}
		return false, nil
	}
	return wait.PollUntilContextTimeout(context.TODO(), time.Second, timeout, false, cond.WithContext())
}

func WriteConfigToFile(config *rest.Config, kubeconfigPath string) error {
	// Build the kubeconfig API object from the rest.Config
	kubeConfig := &clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"default-cluster": {
				Server:                   config.Host,
				CertificateAuthorityData: config.CAData,
				InsecureSkipTLSVerify:    config.Insecure,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"default-auth": {
				Token: config.BearerToken,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"default-context": {
				Cluster:  "default-cluster",
				AuthInfo: "default-auth",
			},
		},
		CurrentContext: "default-context",
	}

	// Ensure the directory where the file is being written exists
	err := os.MkdirAll(filepath.Dir(kubeconfigPath), os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory for kubeconfig file: %v", err)
	}

	// Write the kubeconfig to the specified file
	err = clientcmd.WriteToFile(*kubeConfig, kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to write kubeconfig to file: %v", err)
	}

	return nil
}
