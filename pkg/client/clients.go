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
	"time"

	"go.uber.org/zap"

	"k8s.io/client-go/informers"
	coreInformerV1 "k8s.io/client-go/informers/core/v1"
	schedulingInformerV1 "k8s.io/client-go/informers/scheduling/v1"
	storageInformerV1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

var informerTypes = [...]string{"Pod", "Node", "ConfigMap", "Storage", "PV", "PVC", "PriorityClass", "Namespaces", "CSINode", "CSIDrivers", "CSIStorageCapacity"}

type InformerType int

func (t InformerType) String() string {
	return informerTypes[t]
}

const (
	PodInformerHandlers InformerType = iota
	NodeInformerHandlers
	ConfigMapInformerHandlers
	StorageInformerHandlers
	PVInformerHandlers
	PVCInformerHandlers
	PriorityClassInformerHandlers
	NamespacesInformerHandlers
	CSINodeInformerHandlers
	CSIDriversInformerHandlers
	CSIStorageCapacityInformerHandlers
)

type hasInformer interface {
	Informer() cache.SharedIndexInformer
}

// clients encapsulates a set of useful client APIs
// that can be shared by callers when talking to K8s api-server,
// or the scheduler core.
type Clients struct {
	// configs
	conf *conf.SchedulerConf

	// client apis
	KubeClient   KubeClient
	SchedulerAPI api.SchedulerAPI

	// informer factory
	InformerFactory informers.SharedInformerFactory

	// resource informers
	hasInformers          []hasInformer
	PodInformer           coreInformerV1.PodInformer
	NodeInformer          coreInformerV1.NodeInformer
	ConfigMapInformer     coreInformerV1.ConfigMapInformer
	PVInformer            coreInformerV1.PersistentVolumeInformer
	PVCInformer           coreInformerV1.PersistentVolumeClaimInformer
	StorageInformer       storageInformerV1.StorageClassInformer
	CSINodeInformer       storageInformerV1.CSINodeInformer
	NamespaceInformer     coreInformerV1.NamespaceInformer
	PriorityClassInformer schedulingInformerV1.PriorityClassInformer

	// volume binder handles PV/PVC related operations
	VolumeBinder volumebinding.SchedulerVolumeBinder
}

func NewClients(scheduler api.SchedulerAPI, informerFactory informers.SharedInformerFactory, configs *conf.SchedulerConf, kubeClient KubeClient) *Clients {
	// init informers
	// volume informers are also used to get the Listers for the predicates
	hasInformers := []hasInformer{}

	podInformer := save(informerFactory.Core().V1().Pods(), &hasInformers)
	nodeInformer := save(informerFactory.Core().V1().Nodes(), &hasInformers)
	configMapInformer := save(informerFactory.Core().V1().ConfigMaps(), &hasInformers)
	storageInformer := save(informerFactory.Storage().V1().StorageClasses(), &hasInformers)
	pvInformer := save(informerFactory.Core().V1().PersistentVolumes(), &hasInformers)
	pvcInformer := save(informerFactory.Core().V1().PersistentVolumeClaims(), &hasInformers)
	priorityClassInformer := save(informerFactory.Scheduling().V1().PriorityClasses(), &hasInformers)
	namespaceInformer := save(informerFactory.Core().V1().Namespaces(), &hasInformers)
	csiNodeInformer := save(informerFactory.Storage().V1().CSINodes(), &hasInformers)
	csiDriversInformer := save(informerFactory.Storage().V1().CSIDrivers(), &hasInformers)
	csiStorageCapacityInformer := save(informerFactory.Storage().V1().CSIStorageCapacities(), &hasInformers)

	var capacityCheck = volumebinding.CapacityCheck{
		CSIDriverInformer:          csiDriversInformer,
		CSIStorageCapacityInformer: csiStorageCapacityInformer,
	}

	// create a volume binder (needs the informers)
	volumeBinder := volumebinding.NewVolumeBinder(
		klog.NewKlogr(),
		kubeClient.GetClientSet(),
		podInformer,
		nodeInformer,
		csiNodeInformer,
		pvcInformer,
		pvInformer,
		storageInformer,
		capacityCheck,
		configs.VolumeBindTimeout,
	)

	return &Clients{
		conf:                  configs,
		KubeClient:            kubeClient,
		SchedulerAPI:          scheduler,
		InformerFactory:       informerFactory,
		hasInformers:          hasInformers,
		PodInformer:           podInformer,
		NodeInformer:          nodeInformer,
		ConfigMapInformer:     configMapInformer,
		PVInformer:            pvInformer,
		PVCInformer:           pvcInformer,
		NamespaceInformer:     namespaceInformer,
		StorageInformer:       storageInformer,
		CSINodeInformer:       csiNodeInformer,
		PriorityClassInformer: priorityClassInformer,
		VolumeBinder:          volumeBinder,
	}
}

func save[T hasInformer](n T, collector *[]hasInformer) T {
	*collector = append(*collector, n)
	return n
}

func (c *Clients) GetConf() *conf.SchedulerConf {
	return c.conf
}

func (c *Clients) getInformers() []cache.SharedIndexInformer {
	informers := make([]cache.SharedIndexInformer, len(c.hasInformers))
	for i, informer := range c.hasInformers {
		informers[i] = informer.Informer()
	}
	return informers
}

func (c *Clients) WaitForSync() {
	syncStartTime := time.Now()
	counter := 0

	for _, informer := range c.getInformers() {
		for !informer.HasSynced() {
			time.Sleep(time.Second)
			counter++
			if counter%10 == 0 {
				log.Log(log.ShimClient).Info("Waiting for informers to sync",
					zap.Duration("timeElapsed", time.Since(syncStartTime).Round(time.Second)))
			}
		}
	}
}

func (c *Clients) Run(stopCh <-chan struct{}) {
	for _, informer := range c.getInformers() {
		go informer.Run(stopCh)
	}
}
