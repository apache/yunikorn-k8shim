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
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type updateType int

const (
	AddOccupiedResource updateType = iota
	SubOccupiedResource
)

// scheduler nodes maintain cluster nodes and their status for the scheduler
type schedulerNodes struct {
	proxy    api.SchedulerAPI
	nodesMap map[string]*SchedulerNode
	cache    *external.SchedulerCache
	lock     *sync.RWMutex
}

func newSchedulerNodes(schedulerAPI api.SchedulerAPI, cache *external.SchedulerCache) *schedulerNodes {
	return &schedulerNodes{
		proxy:    schedulerAPI,
		nodesMap: make(map[string]*SchedulerNode),
		cache:    cache,
		lock:     &sync.RWMutex{},
	}
}

func (nc *schedulerNodes) getNode(name string) *SchedulerNode {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if node, ok := nc.nodesMap[name]; ok {
		return node
	}
	return nil
}

func convertToNode(obj interface{}) (*v1.Node, error) {
	if node, ok := obj.(*v1.Node); ok {
		return node, nil
	}
	return nil, fmt.Errorf("cannot convert to *v1.Node: %v", obj)
}

func equals(n1 *v1.Node, n2 *v1.Node) bool {
	n1Resource := common.GetNodeResource(&n1.Status)
	n2Resource := common.GetNodeResource(&n2.Status)
	return common.Equals(n1Resource, n2Resource)
}

func (nc *schedulerNodes) addExistingAllocation(allocation *si.Allocation) error {
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if schedulerNode, ok := nc.nodesMap[allocation.NodeID]; ok {
		schedulerNode.addExistingAllocation(allocation)
		return nil
	}
	return fmt.Errorf("orphan allocation %v", allocation)
}

func (nc *schedulerNodes) addNode(node *v1.Node) {
	nc.addAndReportNode(node, true)
}

func (nc *schedulerNodes) addAndReportNode(node *v1.Node, reportNode bool) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// add node to nodes map
	if _, ok := nc.nodesMap[node.Name]; !ok {
		var nodeLabels []byte
		nodeLabels, err := json.Marshal(node.Labels) // A nil pointer encodes as the "null" JSON value.
		if err != nil {
			log.Logger().Error("failed to marshall node labels to json", zap.Error(err))
			nodeLabels = make([]byte, 0)
		}

		log.Logger().Info("adding node to context",
			zap.String("nodeName", node.Name),
			zap.String("nodeLabels", string(nodeLabels)),
			zap.Bool("schedulable", !node.Spec.Unschedulable))

		ready := hasReadyCondition(node)
		newNode := newSchedulerNode(node.Name, string(node.UID), string(nodeLabels),
			common.GetNodeResource(&node.Status), nc.proxy, !node.Spec.Unschedulable, ready)
		nc.nodesMap[node.Name] = newNode
	}

	// once node is added to scheduler, first thing is to recover its state
	// node might already be in healthy state, previously recovered during recovery process,
	// do not trigger recover again in this case.
	if reportNode {
		if node, ok := nc.nodesMap[node.Name]; ok {
			triggerEvent(node, SchedulerNodeStates().New, RecoverNode)
		}
	}
}

func (nc *schedulerNodes) updateNodeOccupiedResources(name string, resource *si.Resource, opt updateType) {
	if common.IsZero(resource) {
		return
	}

	if schedulerNode := nc.getNode(name); schedulerNode != nil {
		capacity, occupied, ready := schedulerNode.updateOccupiedResource(resource, opt)
		request := common.CreateUpdateRequestForUpdatedNode(name, capacity, occupied, ready)
		log.Logger().Info("report occupied resources updates",
			zap.String("node", schedulerNode.name),
			zap.Any("request", request))
		if err := nc.proxy.UpdateNode(&request); err != nil {
			log.Logger().Info("hitting error while handling UpdateNode", zap.Error(err))
		}
	}
}

func (nc *schedulerNodes) updateNode(oldNode, newNode *v1.Node) {
	// before updating a node, check if it exists in the cache or not
	// if we receive a update node event but the node doesn't exist,
	// we need to add it instead of updating it.
	cachedNode := nc.getNode(newNode.Name)
	if cachedNode == nil {
		nc.addNode(newNode)
		return
	}

	nc.lock.Lock()
	defer nc.lock.Unlock()

	// cordon or restore node
	if (!oldNode.Spec.Unschedulable) && newNode.Spec.Unschedulable {
		triggerEvent(cachedNode, SchedulerNodeStates().Healthy, DrainNode)
	} else if oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable {
		triggerEvent(cachedNode, SchedulerNodeStates().Draining, RestoreNode)
	}

	ready := hasReadyCondition(newNode)
	capacityUpdated := equals(oldNode, newNode)
	readyUpdated := cachedNode.ready == ready

	if capacityUpdated && readyUpdated {
		return
	}

	// Has node resource updated?
	if !capacityUpdated {
		cachedNode.setCapacity(common.GetNodeResource(&newNode.Status))
	}

	// Has node ready status flag updated?
	if !readyUpdated {
		cachedNode.setReadyStatus(ready)
	}

	log.Logger().Info("Node's ready status flag", zap.String("Node name", newNode.Name),
		zap.Bool("ready", ready))

	capacity, occupied, ready := cachedNode.snapshotState()
	request := common.CreateUpdateRequestForUpdatedNode(newNode.Name, capacity, occupied, ready)
	log.Logger().Info("report updated nodes to scheduler", zap.Any("request", request))
	if err := nc.proxy.UpdateNode(&request); err != nil {
		log.Logger().Info("hitting error while handling UpdateNode", zap.Error(err))
	}
}

func (nc *schedulerNodes) deleteNode(node *v1.Node) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	delete(nc.nodesMap, node.Name)

	request := common.CreateUpdateRequestForDeleteOrRestoreNode(node.Name, si.NodeInfo_DECOMISSION)
	log.Logger().Info("report updated nodes to scheduler", zap.Any("request", request.String()))
	if err := nc.proxy.UpdateNode(&request); err != nil {
		log.Logger().Error("hitting error while handling UpdateNode", zap.Error(err))
	}
}

func (nc *schedulerNodes) schedulerNodeEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.SchedulerNodeEvent); ok {
			if node := nc.getNode(event.GetNodeID()); node != nil {
				if node.canHandle(event) {
					if err := node.handle(event); err != nil {
						log.Logger().Error("failed to handle scheduler node event",
							zap.String("event", event.GetEvent()),
							zap.Error(err))
					}
				}
			}
		}
	}
}

func hasReadyCondition(node *v1.Node) bool {
	if node != nil && node.Status.String() != "nil" {
		for _, condition := range node.Status.Conditions {
			if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

func triggerEvent(node *SchedulerNode, currentState string, eventType SchedulerNodeEventType) {
	log.Logger().Info("scheduler node event ", zap.String("name", node.name),
		zap.String("current state ", currentState), zap.Stringer("transition to ", eventType))
	if node.getNodeState() == currentState {
		dispatcher.Dispatch(CachedSchedulerNodeEvent{
			NodeID: node.name,
			Event:  eventType,
		})
	}
}
