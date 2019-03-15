package controller

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"k8s.io/api/core/v1"
)

type PodController struct {
	kubeClient client.KubeClient
}

func NewPodController(client client.KubeClient) *PodController {
	return &PodController{
		kubeClient: client,
	}
}

func (pc *PodController) Bind(p *v1.Pod, hostId string) error {
	if err := pc.kubeClient.Bind(p, hostId); err != nil {
		return err
	}
	glog.V(3).Infof("Successfully bound pod %s", p.Name)
	return nil
}

func (pc *PodController) DeletePod(pod *v1.Pod) {
	glog.V(3).Infof("Delete pod %s, because it is owned by a completed job", pod.Name)
	if err := pc.kubeClient.Delete(pod); err != nil {
		glog.V(1).Infof("Failed to delete pod %s, error: %#v", pod.Name, err)
	}
}