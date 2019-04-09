/*
Copyright 2019 The Unity Scheduler Authors

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
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"k8s.io/api/core/v1"
)

type NodeController struct {
	proxy api.SchedulerApi
}

func NewNodeController(schedulerApi api.SchedulerApi) *NodeController {
	return &NodeController{
		proxy: schedulerApi,
	}
}

func convertToNode(obj interface{}) (*v1.Node, error) {
	if node, ok := obj.(*v1.Node); ok {
		return node, nil
	}
	return nil, errors.New(fmt.Sprintf("Cannot convert to *v1.Node: %v", obj))
}

func equals(n1 *v1.Node, n2 *v1.Node) bool {
	n1Resource := common.GetNodeResource(&n1.Status)
	n2Resource := common.GetNodeResource(&n2.Status)
	return common.Equals(n1Resource, n2Resource)
}

func (nc *NodeController) AddNode(obj interface{}) {
	glog.V(4).Infof("node-controller: AddNode")
	node, err := convertToNode(obj)
	if err != nil {
		glog.Errorf(err.Error())
		return
	}

	n := common.CreateFrom(node)
	request := common.CreateUpdateRequestForNewNode(n)
	glog.V(3).Infof("report new nodes to scheduler, request: %s", request.String())
	if err := nc.proxy.Update(&request); err != nil {
		glog.V(1).Infof("hitting error while handle AddNode, %#v", err)
	}
}

func (nc *NodeController) UpdateNode(oldObj, newObj interface{}) {
	// we only trigger update when resource changes
	oldNode, err := convertToNode(oldObj)
	if err != nil {
		glog.Errorf(err.Error())
		return
	}

	newNode, err := convertToNode(newObj)
	if err != nil {
		glog.Errorf(err.Error())
		return
	}

	// node resource changes
	if equals(oldNode, newNode) {
		glog.V(3).Infof("Node status not changed, skip this UpdateNode event")
		return
	}

	glog.V(4).Infof("node-controller: UpdateNode")
	node := common.CreateFrom(newNode)
	request := common.CreateUpdateRequestForUpdatedNode(node)
	glog.V(3).Infof("send updated nodes to scheduler, request: %s", request.String())
	if err := nc.proxy.Update(&request); err != nil {
		glog.V(1).Infof("hitting error while handle UpdateNode, %#v", err)
	}
}

func (nc *NodeController) DeleteNode(obj interface{}) {
	glog.V(4).Infof("node-controller: DeleteNode")
	node, err := convertToNode(obj)
	if err != nil {
		glog.Errorf(err.Error())
		return
	}

	n := common.CreateFrom(node)
	request := common.CreateUpdateRequestForDeleteNode(n)
	glog.V(3).Infof("send updated nodes to scheduler, request: %s", request.String())
	if err := nc.proxy.Update(&request); err != nil {
		glog.V(1).Infof("hitting error while handle UpdateNode, %#v", err)
	}
}
