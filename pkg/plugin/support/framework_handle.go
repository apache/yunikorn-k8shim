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

package support

import (
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-core/pkg/log"
)

type frameworkHandle struct {
	sharedLister          framework.SharedLister
	sharedInformerFactory informers.SharedInformerFactory
	clientSet             kubernetes.Interface
}

func (p frameworkHandle) SnapshotSharedLister() framework.SharedLister {
	return p.sharedLister
}

func (p frameworkHandle) SharedInformerFactory() informers.SharedInformerFactory {
	return p.sharedInformerFactory
}

func (p frameworkHandle) ClientSet() kubernetes.Interface {
	return p.clientSet
}

// stubbed out to fulfill framework.Handle contract; these are all currently unused by upstream K8S predicates

func (p frameworkHandle) IterateOverWaitingPods(callback func(framework.WaitingPod)) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RejectWaitingPod(uid types.UID) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) EventRecorder() events.EventRecorder {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) PreemptHandle() framework.PreemptHandle {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

var _ framework.Handle = frameworkHandle{}

func NewFrameworkHandle(sharedLister framework.SharedLister, informerFactory informers.SharedInformerFactory, clientSet kubernetes.Interface) framework.Handle {
	return &frameworkHandle{
		sharedLister:          sharedLister,
		sharedInformerFactory: informerFactory,
		clientSet:             clientSet,
	}
}
