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
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"

	"github.com/apache/yunikorn-core/pkg/log"
)

type frameworkHandle struct {
	sharedLister          framework.SharedLister
	sharedInformerFactory informers.SharedInformerFactory
	clientSet             kubernetes.Interface
	parallelizer          parallelize.Parallelizer
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

func (p frameworkHandle) Parallelizer() parallelize.Parallelizer {
	return p.parallelizer
}

// PodNominator stubs

func (p frameworkHandle) AddNominatedPod(pod *framework.PodInfo, nominatingInfo *framework.NominatingInfo) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) DeleteNominatedPodIfExists(pod *v1.Pod) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) UpdateNominatedPod(oldPod *v1.Pod, newPodInfo *framework.PodInfo) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) NominatedPodsForNode(nodeName string) []*framework.PodInfo {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

// PluginsRunner stubs

func (p frameworkHandle) RunPreScorePlugins(context.Context, *framework.CycleState, *v1.Pod, []*v1.Node) *framework.Status {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunScorePlugins(context.Context, *framework.CycleState, *v1.Pod, []*v1.Node) (framework.PluginToNodeScores, *framework.Status) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil, nil
}

func (p frameworkHandle) RunFilterPlugins(context.Context, *framework.CycleState, *v1.Pod, *framework.NodeInfo) framework.PluginToStatus {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunPreFilterExtensionAddPod(ctx context.Context, state *framework.CycleState, podToSchedule *v1.Pod, podInfoToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunPreFilterExtensionRemovePod(ctx context.Context, state *framework.CycleState, podToSchedule *v1.Pod, podInfoToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

// stubbed out to fulfill framework.Handle contract; these are all currently unused by upstream K8S predicates

func (p frameworkHandle) IterateOverWaitingPods(callback func(framework.WaitingPod)) {
	log.Logger().Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RejectWaitingPod(uid types.UID) bool {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return false
}

func (p frameworkHandle) EventRecorder() events.EventRecorder {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) KubeConfig() *rest.Config {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunFilterPluginsWithNominatedPods(ctx context.Context, state *framework.CycleState, pod *v1.Pod, info *framework.NodeInfo) *framework.Status {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) Extenders() []framework.Extender {
	log.Logger().Fatal("BUG: Should not be used by plugins")
	return nil
}

var _ framework.Handle = frameworkHandle{}

func NewFrameworkHandle(sharedLister framework.SharedLister, informerFactory informers.SharedInformerFactory, clientSet kubernetes.Interface) framework.Handle {
	return &frameworkHandle{
		sharedLister:          sharedLister,
		sharedInformerFactory: informerFactory,
		clientSet:             clientSet,
		parallelizer:          parallelize.NewParallelizer(parallelize.DefaultParallelism),
	}
}
