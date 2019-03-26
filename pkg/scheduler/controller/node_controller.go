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

func (nc *NodeController) AddNode(obj interface{}) {
	glog.V(4).Infof("node-controller: AddNode")

	node, ok := obj.(*v1.Node)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Node: %v", obj)
		return
	}

	n := common.CreateFrom(node)
	request := common.CreateUpdateRequestForNode(n)
	glog.V(3).Infof("report new nodes to scheduler, request: %s", request.String())
	if err := nc.proxy.Update(&request); err != nil {
		glog.V(1).Infof("hitting error while handle AddNode, %#v", err)
	}
}

func (nc *NodeController) UpdateNode(oldObj, newObj interface{}) {
	// glog.V(4).Infof("### node-controller: UpdateNode")
}

func (nc *NodeController) DeleteNode(obj interface{}) {
	// glog.V(4).Infof("### node-controller: UpdateNode")
}