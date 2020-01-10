package client

import (
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	coreInformerV1 "k8s.io/client-go/informers/core/v1"
	storageInformerV1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"
)

// scheduler client set encapsulates a set of useful clients
// that can be shared by callers when talking to K8s api-server,
// or the scheduler core.
type SchedulerClientSet struct {
	// configs
	Conf *conf.SchedulerConf

	// client apis
	KubeClient   KubeClient
	SchedulerApi api.SchedulerApi

	// resource informers
	PodInformer       coreInformerV1.PodInformer
	NodeInformer      coreInformerV1.NodeInformer
	ConfigMapInformer coreInformerV1.ConfigMapInformer
	PVInformer        coreInformerV1.PersistentVolumeInformer
	PVCInformer       coreInformerV1.PersistentVolumeClaimInformer
	StorageInformer   storageInformerV1.StorageClassInformer

	// volume binder handles PV/PVC related operations
	VolumeBinder *volumebinder.VolumeBinder
}

func (c *SchedulerClientSet) Run(stopCh <-chan struct{}) {
	if c.NodeInformer != nil {
		go c.NodeInformer.Informer().Run(stopCh)
	}
	if c.PodInformer != nil {
		go c.PodInformer.Informer().Run(stopCh)
	}
	if c.PVInformer != nil {
		go c.PVInformer.Informer().Run(stopCh)
	}
	if c.PVCInformer != nil {
		go c.PVCInformer.Informer().Run(stopCh)
	}
	if c.StorageInformer != nil {
		go c.StorageInformer.Informer().Run(stopCh)
	}
	if c.ConfigMapInformer != nil {
		go c.ConfigMapInformer.Informer().Run(stopCh)
	}
}
