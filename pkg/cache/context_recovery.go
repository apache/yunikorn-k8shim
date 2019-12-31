/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	v1 "k8s.io/client-go/listers/core/v1"

	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/utils"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
)

func (ctx *Context) WaitForRecovery(maxTimeout time.Duration) error {
	// Currently, disable recovery when testing in a mocked cluster,
	// because mock pod/node lister is not easy. We do have unit tests for
	// waitForAppRecovery/waitForNodeRecovery separately.
	if !ctx.testMode {
		// step 1: recover apps
		if err := ctx.waitForAppRecovery(ctx.podInformer.Lister(), maxTimeout); err != nil {
			log.Logger.Error("app recovery failed", zap.Error(err))
			return err
		}

		// step 2: recover nodes
		if err := ctx.waitForNodeRecovery(ctx.nodeInformer.Lister(), maxTimeout); err != nil {
			log.Logger.Error("nodes recovery failed", zap.Error(err))
			return err
		}
	}

	return nil
}

// Wait until all previous scheduled applications are recovered, or fail as timeout.
// During this process, shim submits all applications again to the scheduler-core and verifies app
// state to ensure they are accepted, this must be done before recovering app allocations.
func (ctx *Context) waitForAppRecovery(lister v1.PodLister, maxTimeout time.Duration) error {
	// give informers sometime to warm up...
	allPods, err := waitAndListPods(lister)
	if err != nil {
		return err
	}

	// scan all pods and discover apps, for apps already scheduled before,
	// trigger app recovering
	toRecoverApps := make(map[string]*Application, 0)
	for _, pod := range allPods {
		// pod from a existing app must have been assigned to a node,
		// this means the app was scheduled and needs to be recovered
		if utils.IsAssignedPod(pod) && utils.IsSchedulablePod(pod) {
			app := ctx.getOrCreateApplication(pod)
			ctx.AddApplication(app)
			if app.GetApplicationState() == string(events.States().Application.New) {
				log.Logger.Info("start to recover the app",
					zap.String("appId", app.applicationId))
				dispatcher.Dispatch(NewSimpleApplicationEvent(app.applicationId, events.RecoverApplication))
				toRecoverApps[app.applicationId] = app
			}
		}
	}

	if len(toRecoverApps) > 0 {
		// check app states periodically, ensure all apps exit from recovering state
		if err := utils.WaitForCondition(func() bool {
			for _, app := range toRecoverApps {
				log.Logger.Info("appInfo",
					zap.String("appId", app.applicationId),
					zap.String("state", app.GetApplicationState()))
				if app.GetApplicationState() == string(events.States().Application.Accepted) {
					delete(toRecoverApps, app.applicationId)
				}
			}

			if len(toRecoverApps) == 0 {
				log.Logger.Info("app recovery is successful")
				return true
			}

			return false
		}, 1*time.Second, maxTimeout); err != nil {
			return fmt.Errorf("timeout waiting for app recovery in %s", maxTimeout.String())
		}
	}

	return nil
}

// Recover nodes and the placed allocations on these nodes.
// In this process, shim sends all nodes along with existing allocations on these nodes to the
// scheduler core, scheduler-core recovers its state and accept a node only it is able to recover
// node state plus the allocations. If a node is recovered successfully, its state is marked as
// healthy. Only healthy nodes can be used for scheduling.
func (ctx *Context) waitForNodeRecovery(nodeLister v1.NodeLister, maxTimeout time.Duration) error {
	allNodes, err := waitAndListNodes(nodeLister)
	if err != nil {
		return err
	}

	// add all known nodes to cache, waiting for recover
	for _, node := range allNodes {
		ctx.nodes.addAndReportNode(node, false)
		// current, disable getting pods for a node during test,
		// because in the tests, we don't really send existing allocations
		// we simply simulate to accept or reject nodes on conditions.
		if !ctx.testMode {
			if podList, err := ctx.kubeClient.GetClientSet().
				CoreV1().Pods("").
				List(metav1.ListOptions{
					FieldSelector: fmt.Sprintf("spec.nodeName=%s", node.Name),
				}); err != nil {
				return err
			} else {
				for _, pod := range podList.Items {
					if utils.IsSchedulablePod(&pod) && utils.IsAssignedPod(&pod) {
						log.Logger.Debug("existing pods",
							zap.String("podName", pod.Name),
							zap.String("podUID", string(pod.UID)),
							zap.String("podNodeName", pod.Spec.NodeName))
						if err := ctx.nodes.addExistingAllocation(&pod); err != nil {
							log.Logger.Warn("add existing allocation failed", zap.Error(err))
						}
					}
				}
			}
		}
	}

	if err := utils.WaitForCondition(func() bool {
		nodesRecovered := 0
		for _, node := range ctx.nodes.nodesMap {
			log.Logger.Info("node state",
				zap.String("nodeName", node.name),
				zap.String("nodeState", node.getNodeState()))
			switch node.getNodeState() {
			case string(events.States().Node.New):
				log.Logger.Info("node recovering",
					zap.String("nodeId", node.name))
				dispatcher.Dispatch(CachedSchedulerNodeEvent{
					NodeId: node.name,
					Event:  events.RecoverNode,
				})
			case string(events.States().Node.Healthy):
				nodesRecovered++
			case string(events.States().Node.Draining):
				nodesRecovered++
			}
		}

		if nodesRecovered == len(allNodes) {
			log.Logger.Info("nodes recovery is successful",
				zap.Int("recoveredNodes", nodesRecovered))
			return true
		} else {
			log.Logger.Info("still waiting for recovering nodes",
				zap.Int("totalNodes", len(allNodes)),
				zap.Int("recoveredNodes", nodesRecovered))
			return false
		}
	}, time.Second, maxTimeout); err != nil {
		return fmt.Errorf("timeout waiting for app recovery in %s", maxTimeout.String())
	}

	return nil
}

func waitAndListPods(lister v1.PodLister) (pods []*corev1.Pod, err error) {
	var allPods []*corev1.Pod
	if err := utils.WaitForCondition(func() bool {
		if allPods, _ = lister.List(labels.Everything()); allPods != nil {
			if len(allPods) > 0 {
				return true
			}
		}
		return false
	}, time.Second, time.Minute); err != nil {
		return nil, err
	}

	return allPods, nil
}

func waitAndListNodes(lister v1.NodeLister) (nodes []*corev1.Node, err error) {
	var allNodes []*corev1.Node
	if err := utils.WaitForCondition(func() bool {
		if allNodes, _ = lister.List(labels.Everything()); allNodes != nil {
			if len(allNodes) > 0 {
				return true
			}
		}
		return false
	}, time.Second, time.Minute); err != nil {
		return nil, err
	}

	return allNodes, nil
}
