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
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	k8sCache "k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// nodeResourceCoordinator looks at the resources that are not allocated by yunikorn,
// and refresh scheduler cache to keep nodes' capacity in-sync.
// this coordinator only looks after the pods that are not scheduled by yunikorn,
// and it registers update/delete handler to the pod informer. It ensures that the
// following operations are done
//  1) when a pod is becoming Running, add occupied node resource
//  2) when a pod is terminated, sub the occupied node resource
//  3) when a pod is deleted, sub the occupied node resource
// each of these updates will trigger a node UPDATE action to update the occupied
// resource in the scheduler-core.
type nodeResourceCoordinator struct {
	nodes *schedulerNodes
}

func newNodeResourceCoordinator(nodes *schedulerNodes) *nodeResourceCoordinator {
	return &nodeResourceCoordinator{nodes}
}

// filter pods that not scheduled by us
func (c *nodeResourceCoordinator) filterPods(obj interface{}) bool {
	switch obj := obj.(type) {
	case *v1.Pod:
		if utils.GeneralPodFilter(obj) {
			_, err := utils.GetApplicationIDFromPod(obj)
			return err != nil
		}
		return true
	default:
		return false
	}
}

func (c *nodeResourceCoordinator) updatePod(old, new interface{}) {
	oldPod, err := utils.Convert2Pod(old)
	if err != nil {
		log.Logger().Error("expecting a pod object", zap.Error(err))
		return
	}

	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Logger().Error("expecting a pod object", zap.Error(err))
		return
	}

	// this handles the allocate and release of a pod that not scheduled by yunikorn
	// the check is triggered when a pod status changes
	// conditions for allocate:
	//   1. pod got assigned to a node
	//   2. pod is not in terminated state
	if !utils.IsAssignedPod(oldPod) && utils.IsAssignedPod(newPod) && !utils.IsPodTerminated(newPod) {
		log.Logger().Debug("pod is assigned to a node, trigger occupied resource update",
			zap.String("namespace", newPod.Namespace),
			zap.String("podName", newPod.Name),
			zap.String("podStatusBefore", string(oldPod.Status.Phase)),
			zap.String("podStatusCurrent", string(newPod.Status.Phase)))
		// if pod is running but not scheduled by us,
		// we need to notify scheduler-core to re-sync the node resource
		podResource := common.GetPodResource(newPod)
		c.nodes.updateNodeOccupiedResources(newPod.Spec.NodeName, podResource, AddOccupiedResource)
		c.nodes.cache.AddPod(newPod)
		return
	}

	// conditions for release:
	//   1. pod is already assigned to a node
	//   2. pod status changes from non-terminated to terminated state
	if utils.IsAssignedPod(newPod) && oldPod.Status.Phase != newPod.Status.Phase && utils.IsPodTerminated(newPod) {
		log.Logger().Debug("pod terminated, trigger occupied resource update",
			zap.String("namespace", newPod.Namespace),
			zap.String("podName", newPod.Name),
			zap.String("podStatusBefore", string(oldPod.Status.Phase)),
			zap.String("podStatusCurrent", string(newPod.Status.Phase)))
		// this means pod is terminated
		// we need sub the occupied resource and re-sync with the scheduler-core
		podResource := common.GetPodResource(newPod)
		c.nodes.updateNodeOccupiedResources(newPod.Spec.NodeName, podResource, SubOccupiedResource)
		c.nodes.cache.RemovePod(newPod)
		return
	}
}

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

	// if pod is already terminated, that means the updates have already done
	if utils.IsPodTerminated(pod) {
		log.Logger().Debug("pod is already terminated, occupied resource updated should have already been done")
		return
	}

	log.Logger().Info("deleting pod that scheduled by other schedulers",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name))

	podResource := common.GetPodResource(pod)
	c.nodes.updateNodeOccupiedResources(pod.Spec.NodeName, podResource, SubOccupiedResource)
	c.nodes.cache.RemovePod(pod)
}
