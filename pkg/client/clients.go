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

	"k8s.io/client-go/informers"
	coreInformerV1 "k8s.io/client-go/informers/core/v1"
	storageInformerV1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
)

// clients encapsulates a set of useful client APIs
// that can be shared by callers when talking to K8s api-server,
// or the scheduler core.
type Clients struct {
	// configs
	Conf *conf.SchedulerConf

	// client apis
	KubeClient   KubeClient
	SchedulerAPI api.SchedulerAPI

	// informer factory
	InformerFactory informers.SharedInformerFactory

	// resource informers
	PodInformer       coreInformerV1.PodInformer
	NodeInformer      coreInformerV1.NodeInformer
	ConfigMapInformer coreInformerV1.ConfigMapInformer
	PVInformer        coreInformerV1.PersistentVolumeInformer
	PVCInformer       coreInformerV1.PersistentVolumeClaimInformer
	StorageInformer   storageInformerV1.StorageClassInformer
	NamespaceInformer coreInformerV1.NamespaceInformer

	// volume binder handles PV/PVC related operations
	VolumeBinder *volumebinder.VolumeBinder
}

func (c *Clients) WaitForSync(interval time.Duration, timeout time.Duration) error {
	return utils.WaitForCondition(func() bool {
		// cache is re-sync'd when all informers are sync'd
		return c.NodeInformer.Informer().HasSynced() &&
			c.PodInformer.Informer().HasSynced() &&
			c.PVCInformer.Informer().HasSynced() &&
			c.PVInformer.Informer().HasSynced() &&
			c.StorageInformer.Informer().HasSynced() &&
			c.ConfigMapInformer.Informer().HasSynced() &&
			c.NamespaceInformer.Informer().HasSynced()
	}, interval, timeout)
}

func (c *Clients) Run(stopCh <-chan struct{}) {
	go c.NodeInformer.Informer().Run(stopCh)
	go c.PodInformer.Informer().Run(stopCh)
	go c.PVInformer.Informer().Run(stopCh)
	go c.PVCInformer.Informer().Run(stopCh)
	go c.StorageInformer.Informer().Run(stopCh)
	go c.ConfigMapInformer.Informer().Run(stopCh)
	go c.NamespaceInformer.Informer().Run(stopCh)
}
