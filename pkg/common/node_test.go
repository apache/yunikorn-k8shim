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

package common

import (
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCreateNodeFromSpec(t *testing.T) {
	resource := NewResourceBuilder().
		AddResource(Memory, 999).
		AddResource(CPU, 9).
		Build()
	node := CreateFromNodeSpec("host0001", "uid_0001", resource)
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
			Name: "host0001",
			UID:  "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: resourceList,
		},
	}
	node := CreateFrom(&k8sNode)
	assert.Equal(t, node.name, "host0001")
	assert.Equal(t, node.uid, "uid_0001")
	assert.Equal(t, len(node.resource.Resources), 2)
	assert.Equal(t, node.resource.Resources[Memory].Value, int64(999))
	assert.Equal(t, node.resource.Resources[CPU].Value, int64(9000))
}

func TestCreateNodeWithCustomResource(t *testing.T) {
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(999*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(9, resource.DecimalSI)
	resourceList[v1.ResourceName("nvidia.com/gpu")] = *resource.NewQuantity(3, resource.DecimalSI)
	var k8sNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name: "host0001",
			UID:  "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: resourceList,
		},
	}
	node := CreateFrom(&k8sNode)
	assert.Equal(t, node.name, "host0001")
	assert.Equal(t, node.uid, "uid_0001")
	assert.Equal(t, len(node.resource.Resources), 3)
	assert.Equal(t, node.resource.Resources[Memory].Value, int64(999))
	assert.Equal(t, node.resource.Resources[CPU].Value, int64(9000))
	assert.Equal(t, node.resource.Resources["nvidia.com/gpu"].Value, int64(3))
}
