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
	appsInformerV1 "k8s.io/client-go/informers/apps/v1"
	coreInformerV1 "k8s.io/client-go/informers/core/v1"
	schedulingInformerV1 "k8s.io/client-go/informers/scheduling/v1"
	storageInformerV1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

// clients encapsulates a set of useful client APIs
// that can be shared by callers when talking to K8s api-server,
// or the scheduler core.
type Clients struct {
	// client apis
	KubeClient   KubeClient
	SchedulerAPI api.SchedulerAPI

	// informer factory
	InformerFactory informers.SharedInformerFactory

	// resource informers
	ConfigMapInformer             coreInformerV1.ConfigMapInformer
	CSIDriverInformer             storageInformerV1.CSIDriverInformer
	CSINodeInformer               storageInformerV1.CSINodeInformer
	CSIStorageCapacityInformer    storageInformerV1.CSIStorageCapacityInformer
	NamespaceInformer             coreInformerV1.NamespaceInformer
	NodeInformer                  coreInformerV1.NodeInformer
	PodInformer                   coreInformerV1.PodInformer
	StorageClassInformer          storageInformerV1.StorageClassInformer
	PVCInformer                   coreInformerV1.PersistentVolumeClaimInformer
	PVInformer                    coreInformerV1.PersistentVolumeInformer
	ReplicaSetInformer            appsInformerV1.ReplicaSetInformer
	PriorityClassInformer         schedulingInformerV1.PriorityClassInformer
	ServiceInformer               coreInformerV1.ServiceInformer
	StatefulSetInformer           appsInformerV1.StatefulSetInformer
	ReplicationControllerInformer coreInformerV1.ReplicationControllerInformer
	VolumeAttachmentInformer      storageInformerV1.VolumeAttachmentInformer

	// volume binder handles PV/PVC related operations
	VolumeBinder volumebinding.SchedulerVolumeBinder
}

func (c *Clients) WaitForSync() {
	syncStartTime := time.Now()
	counter := 0
	for {
		if c.ConfigMapInformer.Informer().HasSynced() &&
			c.CSIDriverInformer.Informer().HasSynced() &&
			c.CSINodeInformer.Informer().HasSynced() &&
			c.CSIStorageCapacityInformer.Informer().HasSynced() &&
			c.NamespaceInformer.Informer().HasSynced() &&
			c.NodeInformer.Informer().HasSynced() &&
			c.PodInformer.Informer().HasSynced() &&
			c.PriorityClassInformer.Informer().HasSynced() &&
			c.PVCInformer.Informer().HasSynced() &&
			c.PVInformer.Informer().HasSynced() &&
			c.ReplicaSetInformer.Informer().HasSynced() &&
			c.ReplicationControllerInformer.Informer().HasSynced() &&
			c.ServiceInformer.Informer().HasSynced() &&
			c.StatefulSetInformer.Informer().HasSynced() &&
			c.StorageClassInformer.Informer().HasSynced() &&
			c.VolumeAttachmentInformer.Informer().HasSynced() {
			return
		}
		time.Sleep(time.Second)
		counter++
		if counter%10 == 0 {
			log.Log(log.ShimClient).Info("Waiting for informers to sync",
				zap.Duration("timeElapsed", time.Since(syncStartTime).Round(time.Second)))
		}
	}
}

func (c *Clients) Run(stopCh <-chan struct{}) {
	go c.ConfigMapInformer.Informer().Run(stopCh)
	go c.CSIDriverInformer.Informer().Run(stopCh)
	go c.CSINodeInformer.Informer().Run(stopCh)
	go c.CSIStorageCapacityInformer.Informer().Run(stopCh)
	go c.NamespaceInformer.Informer().Run(stopCh)
	go c.NodeInformer.Informer().Run(stopCh)
	go c.PodInformer.Informer().Run(stopCh)
	go c.PriorityClassInformer.Informer().Run(stopCh)
	go c.PVCInformer.Informer().Run(stopCh)
	go c.PVInformer.Informer().Run(stopCh)
	go c.ReplicaSetInformer.Informer().Run(stopCh)
	go c.ReplicationControllerInformer.Informer().Run(stopCh)
	go c.ServiceInformer.Informer().Run(stopCh)
	go c.StatefulSetInformer.Informer().Run(stopCh)
	go c.StorageClassInformer.Informer().Run(stopCh)
	go c.VolumeAttachmentInformer.Informer().Run(stopCh)
}
