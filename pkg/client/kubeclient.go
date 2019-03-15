package client

import (
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type KubeClient interface {
	// bind a pod to a specific host
	Bind(pod *v1.Pod, hostId string) error

	// Delete a pod from a host
	Delete(pod *v1.Pod) error

	// minimal expose this, only informers factory needs it
	GetClientSet() *kubernetes.Clientset
}

func NewKubeClient(kc string) KubeClient {
	//return newRestClient("127.0.0.1:8001")
	return newNativeClient(kc)
}