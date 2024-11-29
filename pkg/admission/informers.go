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

package admission

import (
	"time"

	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	schedulinginformersv1 "k8s.io/client-go/informers/scheduling/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type Informers struct {
	ConfigMap     informersv1.ConfigMapInformer
	PriorityClass schedulinginformersv1.PriorityClassInformer
	Namespace     informersv1.NamespaceInformer
	stopChan      chan struct{}
}

func NewInformers(kubeClient client.KubeClient, namespace string) *Informers {
	stopChan := make(chan struct{})

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		kubeClient.GetClientSet(),
		0,
		informers.WithNamespace(namespace),
	)
	informerFactory.Start(stopChan)

	return &Informers{
		ConfigMap:     informerFactory.Core().V1().ConfigMaps(),
		PriorityClass: informerFactory.Scheduling().V1().PriorityClasses(),
		Namespace:     informerFactory.Core().V1().Namespaces(),
		stopChan:      stopChan,
	}
}

func (i *Informers) Start() {
	go i.ConfigMap.Informer().Run(i.stopChan)
	go i.PriorityClass.Informer().Run(i.stopChan)
	go i.Namespace.Informer().Run(i.stopChan)
	i.waitForSync()
}

func (i *Informers) Stop() {
	if i.stopChan != nil {
		close(i.stopChan)
	}
}

func (i *Informers) waitForSync() {
	syncStartTime := time.Now()
	counter := 0
	for {
		if i.ConfigMap.Informer().HasSynced() &&
			i.PriorityClass.Informer().HasSynced() &&
			i.Namespace.Informer().HasSynced() {
			return
		}
		time.Sleep(time.Second)
		counter++
		if counter%10 == 0 {
			log.Log(log.AdmissionClient).Info("Waiting for informers to sync",
				zap.Duration("timeElapsed", time.Since(syncStartTime).Round(time.Second)))
		}
	}
}
