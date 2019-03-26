package common

import (
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"k8s.io/api/core/v1"
)

// stores info about what scheduler cares about a node
type Node struct {
	name string
	uid string
	resource *si.Resource
}

func CreateFrom(node *v1.Node) Node {
	return Node{
		name: node.Name,
		uid: string(node.UID),
		resource: GetNodeResource(&node.Status),
	}
}

func CreateFromNodeSpec(nodeName string, nodeUid string, nodeResource *si.Resource) Node {
	return Node {
		name: nodeName,
		uid: nodeUid,
		resource: nodeResource,
	}
}