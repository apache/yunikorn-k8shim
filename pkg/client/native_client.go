package client

import (
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type NativeClient struct {
	clientSet *kubernetes.Clientset
}

func newNativeClient(kc string) KubeClient{
	config, err := clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		panic(err.Error())
	}

	configuredClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return &NativeClient{
		clientSet: configuredClient,
	}
}

func (nc *NativeClient) Bind(pod *v1.Pod, node v1.Node) error {
	glog.V(3).Infof("bind pod %s(%s) to node %s(%s)", pod.Name, pod.UID, node.Name, node.UID)
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Bind(
		&v1.Binding{ObjectMeta: apis.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: node.Name,
			},
		}); err != nil {
		glog.V(2).Infof("Failed to bind pod <%v/%v>: %#v", pod.Namespace, pod.Name, err)
		return err
	}
	return nil

}

func (nc *NativeClient) Delete(pod *v1.Pod, node v1.Node) error {
	// TODO make this configurable for pods
	gracefulSeconds := int64(3)
	glog.V(3).Infof("delete pod %s(%s) from node %s(%s)", pod.Name, pod.UID, node.Name, node.UID)
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &apis.DeleteOptions{
		GracePeriodSeconds: &gracefulSeconds,
	}); err != nil {
		glog.Errorf("Failed to delete pod <%v/%v>: %#v", pod.Namespace, pod.Name, err)
		return err
	}
	return nil
}

