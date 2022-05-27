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

package cache

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func (ctx *Context) WaitForRecovery(recoverableAppManagers []interfaces.Recoverable, maxTimeout time.Duration) error {
	// Currently, disable recovery when testing in a mocked cluster,
	// because mock pod/node lister is not easy. We do have unit tests for
	// waitForAppRecovery/recover separately.
	if !ctx.apiProvider.IsTestingMode() {
		if err := ctx.recover(recoverableAppManagers, maxTimeout); err != nil {
			log.Logger().Error("nodes recovery failed", zap.Error(err))
			return err
		}
	}

	return nil
}

// for a given pod, return an allocation if found
func getExistingAllocation(recoverableAppManagers []interfaces.Recoverable, pod *corev1.Pod) *si.Allocation {
	for _, mgr := range recoverableAppManagers {
		// only collect pod that needs recovery
		if !utils.IsPodTerminated(pod) {
			if alloc := mgr.GetExistingAllocation(pod); alloc != nil {
				return alloc
			}
		}
	}
	return nil
}

// Recover nodes and the placed allocations on these nodes.
// In this process, shim sends all nodes along with existing allocations on these nodes to the
// scheduler core, scheduler-core recovers its state and accept a node only it is able to recover
// node state plus the allocations. If a node is recovered successfully, its state is marked as
// healthy. Only healthy nodes can be used for scheduling.
func (ctx *Context) recover(mgr []interfaces.Recoverable, due time.Duration) error {
	allNodes, err := waitAndListNodes(ctx.apiProvider)
	if err != nil {
		return err
	}

	// add all known nodes to cache, waiting for recover
	for _, node := range allNodes {
		ctx.nodes.addAndReportNode(node, false)
	}

	// current, disable getting pods for a node during test,
	// because in the tests, we don't really send existing allocations
	// we simply simulate to accept or reject nodes on conditions.
	if !ctx.apiProvider.IsTestingMode() {
		var podList *corev1.PodList
		podList, err = ctx.apiProvider.GetAPIs().KubeClient.GetClientSet().
			CoreV1().Pods("").
			List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return err
		}

		nodeOccupiedResources := make(map[string]*si.Resource)
		for i := 0; i < len(podList.Items); i++ {
			pod := podList.Items[i]
			// only handle assigned pods
			if !utils.IsAssignedPod(&pod) {
				log.Logger().Info("Skipping unassigned pod",
					zap.String("podUID", string(pod.UID)),
					zap.String("podName", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)))
				continue
			}
			// yunikorn scheduled pods add to existing allocations
			_, err = utils.GetApplicationIDFromPod(&pod)
			ykPod := utils.GeneralPodFilter(&pod) && err == nil
			switch {
			case ykPod:
				if existingAlloc := getExistingAllocation(mgr, &pod); existingAlloc != nil {
					log.Logger().Debug("Adding resources for existing pod",
						zap.String("appID", existingAlloc.ApplicationID),
						zap.String("podUID", string(pod.UID)),
						zap.String("podName", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
						zap.String("nodeName", existingAlloc.NodeID),
						zap.Stringer("resources", common.GetPodResource(&pod)))
					existingAlloc.AllocationTags = common.CreateTagsForTask(&pod)
					if err = ctx.nodes.addExistingAllocation(existingAlloc); err != nil {
						log.Logger().Warn("Failed to add existing allocation", zap.Error(err))
					}
				} else {
					log.Logger().Warn("No allocation found for existing pod",
						zap.String("podUID", string(pod.UID)),
						zap.String("podName", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
						zap.String("nodeName", pod.Spec.NodeName),
						zap.Stringer("resources", common.GetPodResource(&pod)))
				}
			case !utils.IsPodTerminated(&pod):
				// pod is not terminated (succeed or failed) state,
				// and it has a node assigned, that means the scheduler
				// has already allocated the pod onto a node
				// we should report this occupied resource to scheduler-core
				occupiedResource := nodeOccupiedResources[pod.Spec.NodeName]
				if occupiedResource == nil {
					occupiedResource = common.NewResourceBuilder().Build()
				}
				podResource := common.GetPodResource(&pod)
				log.Logger().Debug("Adding resources for occupied pod",
					zap.String("podUID", string(pod.UID)),
					zap.String("podName", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
					zap.String("nodeName", pod.Spec.NodeName),
					zap.Stringer("resources", podResource))
				occupiedResource = common.Add(occupiedResource, podResource)
				nodeOccupiedResources[pod.Spec.NodeName] = occupiedResource
				ctx.nodes.cache.AddPod(&pod)
			default:
				log.Logger().Debug("Skipping terminated pod",
					zap.String("podUID", string(pod.UID)),
					zap.String("podName", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)))
			}
		}

		// why we need to calculate the occupied resources here? why not add an event-handler
		// in node_coordinator#addPod?
		// this is because the occupied resources must be calculated and counted before the
		// scheduling started. If we do both updating existing occupied resources along with
		// new pods scheduling, due to the fact that we cannot predicate the ordering of K8s
		// events, it could be dangerous because we might schedule pods onto some node that
		// doesn't have enough capacity (occupied resources not yet reported).
		for nodeName, occupiedResource := range nodeOccupiedResources {
			if cachedNode := ctx.nodes.getNode(nodeName); cachedNode != nil {
				cachedNode.updateOccupiedResource(occupiedResource, AddOccupiedResource)
			}
		}
	}

	// start new nodes
	for _, node := range ctx.nodes.nodesMap {
		log.Logger().Info("node state",
			zap.String("nodeName", node.name),
			zap.String("nodeState", node.getNodeState()))
		if node.getNodeState() == SchedulerNodeStates().New {
			dispatcher.Dispatch(CachedSchedulerNodeEvent{
				NodeID: node.name,
				Event:  RecoverNode,
			})
		}
	}

	// wait for nodes to be recovered
	if err = utils.WaitForCondition(func() bool {
		nodesRecovered := 0
		for _, node := range ctx.nodes.nodesMap {
			log.Logger().Info("node state",
				zap.String("nodeName", node.name),
				zap.String("nodeState", node.getNodeState()))
			switch node.getNodeState() {
			case SchedulerNodeStates().Healthy:
				nodesRecovered++
			case SchedulerNodeStates().Draining:
				nodesRecovered++
			case SchedulerNodeStates().Rejected:
				nodesRecovered++
			}
		}

		if nodesRecovered == len(allNodes) {
			log.Logger().Info("nodes recovery is successful",
				zap.Int("recoveredNodes", nodesRecovered))
			return true
		}
		log.Logger().Info("still waiting for recovering nodes",
			zap.Int("totalNodes", len(allNodes)),
			zap.Int("recoveredNodes", nodesRecovered))
		return false
	}, time.Second, due); err != nil {
		return fmt.Errorf("timeout waiting for app recovery in %s", due.String())
	}

	return nil
}

func waitAndListNodes(apiProvider client.APIProvider) ([]*corev1.Node, error) {
	var allNodes []*corev1.Node
	var listErr error

	// need to wait for sync
	// because the shared indexer doesn't sync its cache periodically
	if err := apiProvider.WaitForSync(); err != nil {
		return allNodes, err
	}

	// list all nodes in the cluster,
	// retry for sometime if there is some errors
	err := utils.WaitForCondition(func() bool {
		allNodes, listErr = apiProvider.GetAPIs().
			NodeInformer.Lister().List(labels.Everything())
		return listErr == nil
	}, time.Second, time.Minute)
	if err != nil {
		return nil, err
	}

	return allNodes, nil
}
