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
	fwk "k8s.io/kube-scheduler/framework"
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

func (p frameworkHandle) AddNominatedPod(_ klog.Logger, _ fwk.PodInfo, _ *framework.NominatingInfo) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) DeleteNominatedPodIfExists(_ *v1.Pod) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) UpdateNominatedPod(_ klog.Logger, _ *v1.Pod, _ fwk.PodInfo) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) NominatedPodsForNode(_ string) []fwk.PodInfo {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

// PluginsRunner stubs

func (p frameworkHandle) RunPreScorePlugins(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ []fwk.NodeInfo) *fwk.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunScorePlugins(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ []fwk.NodeInfo) ([]framework.NodePluginScores, *fwk.Status) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil, nil
}

func (p frameworkHandle) RunFilterPlugins(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunPreFilterExtensionAddPod(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.PodInfo, _ fwk.NodeInfo) *fwk.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RunPreFilterExtensionRemovePod(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.PodInfo, _ fwk.NodeInfo) *fwk.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

// stubbed out to fulfill framework.Handle contract; these are all currently unused by upstream K8S predicates

func (p frameworkHandle) IterateOverWaitingPods(_ func(framework.WaitingPod)) {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) GetWaitingPod(_ types.UID) framework.WaitingPod {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) RejectWaitingPod(_ types.UID) bool {
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

func (p frameworkHandle) RunFilterPluginsWithNominatedPods(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) Extenders() []framework.Extender {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) Activate(_ klog.Logger, _ map[string]*v1.Pod) {
	// currently only used by Preemption plugin, so not needed
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
}

func (p frameworkHandle) SharedDRAManager() framework.SharedDRAManager {
	// currently only used by DRA
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) APIDispatcher() fwk.APIDispatcher {
	log.Log(log.ShimFramework).Fatal("BUG: Should not be used by plugins")
	return nil
}

func (p frameworkHandle) APICacher() framework.APICacher {
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
