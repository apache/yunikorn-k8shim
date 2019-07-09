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

package controller

import (
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-k8shim/pkg/state/cache"
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
)

type NodeController struct {
	proxy api.SchedulerApi
	cache *cache.SchedulerCache
}

func NewNodeController(schedulerApi api.SchedulerApi, cache *cache.SchedulerCache) *NodeController {
	return &NodeController{
		proxy: schedulerApi,
		cache: cache,
	}
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

func (nc *NodeController) AddNode(obj interface{}) {
	log.Logger.Debug("node-controller: AddNode")
	node, err := convertToNode(obj)
	if err != nil {
		log.Logger.Error("node conversion failed", zap.Error(err))
		return
	}

	// add node to cache
	log.Logger.Info("adding node to cache", zap.String("NodeName", node.Name))
	nc.cache.AddNode(node)

	n := common.CreateFrom(node)
	request := common.CreateUpdateRequestForNewNode(n)
	log.Logger.Info("report new nodes to scheduler", zap.Any("request", request.String()))
	if err := nc.proxy.Update(&request); err != nil {
		log.Logger.Error("hitting error while handling AddNode", zap.Error(err))
	}
}

func (nc *NodeController) UpdateNode(oldObj, newObj interface{}) {
	// we only trigger update when resource changes
	oldNode, err := convertToNode(oldObj)
	if err != nil {
		log.Logger.Error("old node conversion failed", zap.Error(err))
		return
	}

	newNode, err := convertToNode(newObj)
	if err != nil {
		log.Logger.Error("new node conversion failed", zap.Error(err))
		return
	}

	// update cache
	log.Logger.Debug("updating node in cache", zap.String("OldNodeName", oldNode.Name))
	nc.cache.UpdateNode(oldNode, newNode)

	// node resource changes
	if equals(oldNode, newNode) {
		log.Logger.Info("Node status not changed, skip this UpdateNode event")
		return
	}

	log.Logger.Debug("node-controller: UpdateNode")
	node := common.CreateFrom(newNode)
	request := common.CreateUpdateRequestForUpdatedNode(node)
	log.Logger.Info("report updated nodes to scheduler", zap.Any("request", request))
	if err := nc.proxy.Update(&request); err != nil {
		log.Logger.Info("hitting error while handling UpdateNode", zap.Error(err))
	}
}

func (nc *NodeController) DeleteNode(obj interface{}) {
	log.Logger.Debug("node-controller: DeleteNode")
	node, err := convertToNode(obj)
	if err != nil {
		log.Logger.Error("node conversion failed", zap.Error(err))
		return
	}

	// add node to cache
	log.Logger.Debug("delete node from cache", zap.String("nodeName", node.Name))
	nc.cache.RemoveNode(node)

	n := common.CreateFrom(node)
	request := common.CreateUpdateRequestForDeleteNode(n)
	log.Logger.Info("report updated nodes to scheduler", zap.Any("request", request.String()))
	if err := nc.proxy.Update(&request); err != nil {
		log.Logger.Info("hitting error while handling UpdateNode", zap.Error(err))
	}
}
