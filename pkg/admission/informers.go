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
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type Informers struct {
	ConfigMap     informersv1.ConfigMapInformer
	PriorityClass schedulinginformersv1.PriorityClassInformer
	stopChan      chan struct{}
}

func NewInformers(kubeClient client.KubeClient, namespace string) *Informers {
	stopChan := make(chan struct{})

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient.GetClientSet(), 0, informers.WithNamespace(namespace))
	informerFactory.Start(stopChan)

	result := &Informers{
		ConfigMap:     informerFactory.Core().V1().ConfigMaps(),
		PriorityClass: informerFactory.Scheduling().V1().PriorityClasses(),
		stopChan:      stopChan,
	}

	return result
}

func (i *Informers) Start() {
	go i.ConfigMap.Informer().Run(i.stopChan)
	go i.PriorityClass.Informer().Run(i.stopChan)
	if err := i.waitForSync(time.Second, 30*time.Second); err != nil {
		log.Logger().Warn("Failed to sync informers", zap.Error(err))
	}
}

func (i *Informers) Stop() {
	if i.stopChan != nil {
		close(i.stopChan)
	}
}

func (i *Informers) waitForSync(interval time.Duration, timeout time.Duration) error {
	return utils.WaitForCondition(func() bool {
		return i.ConfigMap.Informer().HasSynced() &&
			i.PriorityClass.Informer().HasSynced()
	}, interval, timeout)
}
