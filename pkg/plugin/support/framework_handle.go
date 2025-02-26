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
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"

	"github.com/apache/yunikorn-k8shim/pkg/log"
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

func (p frameworkHandle) ResourceClaimCache() *assumecache.AssumeCache {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

// PodNominator stubs

func (p frameworkHandle) AddNominatedPod(logger klog.Logger, pod *framework.PodInfo, nominatingInfo *framework.NominatingInfo) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) DeleteNominatedPodIfExists(pod *v1.Pod) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) UpdateNominatedPod(logger klog.Logger, oldPod *v1.Pod, newPodInfo *framework.PodInfo) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) NominatedPodsForNode(nodeName string) []*framework.PodInfo {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

// PluginsRunner stubs

func (p frameworkHandle) RunPreScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, infos []*framework.NodeInfo) *framework.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, infos []*framework.NodeInfo) ([]framework.NodePluginScores, *framework.Status) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil, nil
}

func (p frameworkHandle) RunFilterPlugins(context.Context, *framework.CycleState, *v1.Pod, *framework.NodeInfo) *framework.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunPreFilterExtensionAddPod(ctx context.Context, state *framework.CycleState, podToSchedule *v1.Pod, podInfoToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunPreFilterExtensionRemovePod(ctx context.Context, state *framework.CycleState, podToSchedule *v1.Pod, podInfoToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

// stubbed out to fulfill framework.Handle contract; these are all currently unused by upstream K8S predicates

func (p frameworkHandle) IterateOverWaitingPods(callback func(framework.WaitingPod)) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RejectWaitingPod(uid types.UID) bool {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return false
}

func (p frameworkHandle) EventRecorder() events.EventRecorder {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) KubeConfig() *rest.Config {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunFilterPluginsWithNominatedPods(ctx context.Context, state *framework.CycleState, pod *v1.Pod, info *framework.NodeInfo) *framework.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) Extenders() []framework.Extender {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) Activate(logger klog.Logger, pods map[string]*v1.Pod) {
	// currently only used by Preemption plugin, so not needed
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) SharedDRAManager() framework.SharedDRAManager {
	// currently only used by DRA
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
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
