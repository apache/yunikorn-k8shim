package controller

import (
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type PodController struct {
	kubeClient *kubernetes.Clientset
}

func NewPodController(clientSet *kubernetes.Clientset) *PodController {
	return &PodController{
		kubeClient: clientSet,
	}
}

func (pc *PodController) Bind(p *v1.Pod, hostId string) error {
	glog.V(4).Infof("bind pod %s/%s to host %s", p.Namespace, p.Name, hostId)
	if err := pc.kubeClient.CoreV1().Pods(p.Namespace).Bind(
		&v1.Binding{ObjectMeta: apis.ObjectMeta{
			Namespace: p.Namespace, Name: p.Name, UID: p.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: hostId,
		},
	}); err != nil {
		glog.V(2).Infof("Failed to bind pod <%v/%v>: %#v", p.Namespace, p.Name, err)
		return err
	}
	glog.V(3).Infof("Successfully bound pod %s", p.Name)
	return nil
}
