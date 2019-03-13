package client

import (
	"k8s.io/api/core/v1"
)

type KubeClient interface {
	// bind a pod to a specific host
	Bind(pod *v1.Pod, node v1.Node) error

	// Delete a pod from a host
	Delete(pod *v1.Pod, node v1.Node) error
}

func New(kc string) KubeClient {
	//return newRestClient("127.0.0.1:8001")
	return newNativeClient(kc)
}