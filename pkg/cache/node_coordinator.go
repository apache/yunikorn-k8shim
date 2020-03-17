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
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	k8sCache "k8s.io/client-go/tools/cache"
)

// nodeCoordinator looks at the resources that not allocated by yunikorn,
// and refresh scheduler cache to keep nodes' capacity in-sync
type nodeCoordinator struct {
	nodes *schedulerNodes
}

func newNodeCoordinator(nodes *schedulerNodes) *nodeCoordinator {
	return &nodeCoordinator{nodes}
}

// filter pods that not scheduled by us
func (c *nodeCoordinator) filterPods(obj interface{}) bool {
	switch obj.(type) {
	case *v1.Pod:
		pod := obj.(*v1.Pod)
		return !utils.GeneralPodFilter(pod)
	default:
		return false
	}
}

func (c *nodeCoordinator) updatePod(old, new interface{}) {
	oldPod, err := utils.Convert2Pod(old)
	if err != nil {
		log.Logger.Error("expecting a pod object", zap.Error(err))
		return
	}

	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Logger.Error("expecting a pod object", zap.Error(err))
		return
	}

	log.Logger.Info("### UPDATE",
		zap.String("old", string(oldPod.Status.Phase)),
		zap.String("newPod", string(newPod.Status.Phase)))

	// triggered when pod status phase changes
	if oldPod.Status.Phase != newPod.Status.Phase {
		// if pod is running but not scheduled by us,
		// we need to notify scheduler-core to re-sync the node resource
		log.Logger.Info("### UPDATE",
			zap.String("old", string(oldPod.Status.Phase)),
			zap.String("newPod", string(newPod.Status.Phase)),
			zap.Bool("equal", newPod.Status.Phase == v1.PodRunning))

		if newPod.Status.Phase == v1.PodRunning {
			log.Logger.Info("pod is allocated by another scheduler",
				zap.String("namespace", newPod.Namespace),
				zap.String("podName", newPod.Name),
				zap.String("podStatus", string(newPod.Status.Phase)))

			if nodeName := newPod.Spec.NodeName; nodeName != "" {
				podResource := common.GetPodResource(newPod)
				c.nodes.updateNodeCapacity(nodeName, podResource, SubCapacity)
				if err := c.nodes.cache.AddPod(newPod); err != nil {
					log.Logger.Warn("failed to update scheduler-cache",
						zap.Error(err))
				}
			}
		} else if newPod.Status.Phase == v1.PodSucceeded ||
			newPod.Status.Phase == v1.PodFailed {
			// this means pod is terminated
			// we need to add the capacity back and re-sync with the scheduler-core
			if nodeName := newPod.Spec.NodeName; nodeName != "" {
				podResource := common.GetPodResource(newPod)
				c.nodes.updateNodeCapacity(nodeName, podResource, AddCapacity)
				if err := c.nodes.cache.RemovePod(newPod); err != nil {
					log.Logger.Warn("failed to update scheduler-cache",
						zap.Error(err))
				}
			} else {
				log.Logger.Warn("node is not found in pod's spec",
					zap.String("namespace", newPod.Namespace),
					zap.String("podName", newPod.Name))
			}
		}
	}
}

func (c *nodeCoordinator) deletePod(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case k8sCache.DeletedFinalStateUnknown:
		var err error
		pod, err = utils.Convert2Pod(t.Obj)
		if err != nil {
			log.Logger.Error(err.Error())
			return
		}
	default:
		log.Logger.Error("cannot convert to pod")
		return
	}

	// if pod is already terminated
	if pod.Status.Phase == v1.PodSucceeded ||
		pod.Status.Phase == v1.PodFailed {
		log.Logger.Debug("pod is already terminated, node capacity should be already updated")
		return
	}

	log.Logger.Info("delete pod that scheduled by other schedulers",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name))

	podResource := common.GetPodResource(pod)
	c.nodes.updateNodeCapacity(pod.Spec.NodeName, podResource, AddCapacity)
	if err := c.nodes.cache.RemovePod(pod); err != nil {
		log.Logger.Warn("failed to update scheduler-cache",
			zap.Error(err))
	}
}