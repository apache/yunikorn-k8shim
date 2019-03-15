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

func newNativeClient(kc string) NativeClient{
	config, err := clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		panic(err.Error())
	}

	configuredClient := kubernetes.NewForConfigOrDie(config)
	return NativeClient{
		clientSet: configuredClient,
	}
}

func (nc NativeClient) GetClientSet() *kubernetes.Clientset {
	return nc.clientSet
}

func (nc NativeClient) Bind(pod *v1.Pod, hostId string) error {
	glog.V(3).Infof("bind pod %s(%s) to node %s", pod.Name, pod.UID, hostId)
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Bind(
		&v1.Binding{ObjectMeta: apis.ObjectMeta{
			Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: hostId,
		},
	}); err != nil {
		glog.V(2).Infof("Failed to bind pod <%v/%v>: %#v", pod.Namespace, pod.Name, err)
		return err
	}
	return nil

}

func (nc NativeClient) Delete(pod *v1.Pod) error {
	// TODO make this configurable for pods
	gracefulSeconds := int64(3)
	if err := nc.clientSet.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &apis.DeleteOptions{
		GracePeriodSeconds: &gracefulSeconds,
	}); err != nil {
		glog.Errorf("Failed to delete pod <%v/%v>: %#v", pod.Namespace, pod.Name, err)
		return err
	}
	return nil
}

