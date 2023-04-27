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
	"sync"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sCache "k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// nodeResourceCoordinator is used to track resources for Pods which are scheduled outside yunikorn and keep node occupied resources in sync
type nodeResourceCoordinator struct {
	nodes     *schedulerNodes
	boundPods map[types.UID]string
	lock      sync.RWMutex
}

// newNodeResourceCoordinator creates a new resource coordinator object
func newNodeResourceCoordinator(nodes *schedulerNodes) *nodeResourceCoordinator {
	return &nodeResourceCoordinator{
		nodes:     nodes,
		boundPods: make(map[types.UID]string),
	}
}

// filterPods selects only Pods not scheduled by yunikorn
func (c *nodeResourceCoordinator) filterPods(obj interface{}) bool {
	switch obj := obj.(type) {
	case *v1.Pod:
		return utils.GetApplicationIDFromPod(obj) == ""
	default:
		return false
	}
}

// addPod handles Pod addition events
func (c *nodeResourceCoordinator) addPod(new interface{}) {
	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Logger().Error("expecting a pod object", zap.Error(err))
		return
	}
	c.updatePodInternal(newPod)
}

// updatePod handles Pod update events
func (c *nodeResourceCoordinator) updatePod(_, new interface{}) {
	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Logger().Error("expecting a pod object", zap.Error(err))
		return
	}
	c.updatePodInternal(newPod)
}

// deletePod handles Pod deletion events
func (c *nodeResourceCoordinator) deletePod(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case k8sCache.DeletedFinalStateUnknown:
		var err error
		pod, err = utils.Convert2Pod(t.Obj)
		if err != nil {
			log.Logger().Error(err.Error())
			return
		}
	default:
		log.Logger().Error("cannot convert to pod")
		return
	}
	c.deletePodInternal(pod)
}

// updatePodInternal is used internally to update pod status
func (c *nodeResourceCoordinator) updatePodInternal(pod *v1.Pod) {
	c.lock.Lock()
	defer c.lock.Unlock()

	oldNode := c.getOldAssignedNode(pod)
	newNode := c.getNewAssignedNode(pod)

	// check for node assignment
	if oldNode == "" && newNode != "" {
		// bind pod
		c.bindPod(pod, newNode)
		return
	}

	// check for node removal
	if oldNode != "" && newNode == "" {
		// unbind pod
		c.unbindPod(pod, oldNode)
		return
	}

	// check for node changing - should not normally happen, but Pod spec is mutable
	if oldNode != newNode && oldNode != "" && newNode != "" {
		// unbind and rebind
		c.unbindPod(pod, oldNode)
		c.bindPod(pod, newNode)
	}
}

// deletePodInternal is used internally to remove pod
func (c *nodeResourceCoordinator) deletePodInternal(pod *v1.Pod) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// unbind the pod if it was previously bound
	if nodeName := c.getOldAssignedNode(pod); nodeName != "" {
		c.unbindPod(pod, nodeName)
	}
}

// getOldAssignedNode returns the node that a pod was previously assigned to
func (c *nodeResourceCoordinator) getOldAssignedNode(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	nodeName, ok := c.boundPods[pod.UID]
	if !ok {
		return ""
	}
	return nodeName
}

// getNewAssignedNode returns the node that a pod will be assigned to (assuming it is not terminated)
func (c *nodeResourceCoordinator) getNewAssignedNode(pod *v1.Pod) string {
	if pod != nil && utils.IsAssignedPod(pod) && !utils.IsPodTerminated(pod) {
		return pod.Spec.NodeName
	}
	return ""
}

// bindPod is used to bind a pod to a node and add occupied resources
func (c *nodeResourceCoordinator) bindPod(pod *v1.Pod, nodeName string) {
	podResource := common.GetPodResource(pod)
	log.Logger().Info("assigning non-yunikorn pod to node",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.String("nodeName", nodeName),
		zap.Any("resources", podResource.Resources))
	c.nodes.updateNodeOccupiedResources(nodeName, podResource, AddOccupiedResource)
	c.nodes.cache.AddPod(pod)
	c.boundPods[pod.UID] = nodeName
}

// unbindPod is used to unbind a pod from a node and subtract occupied resources
func (c *nodeResourceCoordinator) unbindPod(pod *v1.Pod, nodeName string) {
	podResource := common.GetPodResource(pod)
	log.Logger().Info("removing non-yunikorn pod from node",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.String("nodeName", nodeName),
		zap.Any("resources", podResource.Resources))
	c.nodes.updateNodeOccupiedResources(nodeName, podResource, SubOccupiedResource)
	c.nodes.cache.RemovePod(pod)
	delete(c.boundPods, pod.UID)
}
