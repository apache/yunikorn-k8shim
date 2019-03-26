package common

import (
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCreateNodeFromSpec(t *testing.T) {
	resource := CreateResource(999, 9)
	node := CreateFromNodeSpec("host0001", "uid_0001", &resource)
	assert.Equal(t, node.name, "host0001")
	assert.Equal(t, node.uid, "uid_0001")
	assert.Equal(t, len(node.resource.Resources), 2)
	assert.Equal(t, node.resource.Resources[Memory].Value, int64(999))
	assert.Equal(t, node.resource.Resources[CPU].Value, int64(9))
}

func TestCreateNode(t *testing.T) {
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(999*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(9, resource.DecimalSI)
	var k8sNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:            "host0001",
			GenerateName:    "",
			Namespace:       "",
			SelfLink:        "",
			UID:             "uid_0001",
			Labels:          nil,
			Annotations:     nil,
			ClusterName:     "",
		},
		Status: v1.NodeStatus{
			Capacity:        resourceList,
			Allocatable:     nil,
			Phase:           "",
			Conditions:      nil,
			Addresses:       nil,
			DaemonEndpoints: v1.NodeDaemonEndpoints{},
			NodeInfo:        v1.NodeSystemInfo{},
			Images:          nil,
			VolumesInUse:    nil,
			VolumesAttached: nil,
			Config:          nil,
		},
	}
	node := CreateFrom(&k8sNode)
	assert.Equal(t, node.name, "host0001")
	assert.Equal(t, node.uid, "uid_0001")
	assert.Equal(t, len(node.resource.Resources), 2)
	assert.Equal(t, node.resource.Resources[Memory].Value, int64(999))
	assert.Equal(t, node.resource.Resources[CPU].Value, int64(9000))
}