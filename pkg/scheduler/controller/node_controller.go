package controller

import (
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
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
	glog.V(4).Infof("### node-controller: AddNode")

	node, ok := obj.(*v1.Node)
	if !ok {
		glog.Errorf("Cannot convert to *v1.Node: %v", obj)
		return
	}

	// Use node's name as the NodeId, this is because when bind pod to node,
	// name of node is required but uid is optional.
	nodeInfo := &si.NewNodeInfo{
		NodeId: string(node.Name),
		SchedulableResource: common.GetNodeResource(&node.Status),
		// TODO is this required?
		Attributes: map[string]string{
			common.DefaultNodeAttributeHostNameKey : node.Name,
			common.DefaultNodeAttributeRackNameKey: common.DefaultRackName,
		},
	}

	glog.V(3).Infof("node ID %s, resource: %s, ",
		nodeInfo.NodeId,
		nodeInfo.SchedulableResource.String())

	nodes := make([]*si.NewNodeInfo, 1)
	nodes[0] = nodeInfo
	request := si.UpdateRequest{
		NewSchedulableNodes: nodes,
		RmId:                common.ClusterId,
	}

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