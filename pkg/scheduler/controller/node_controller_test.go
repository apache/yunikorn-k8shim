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
	"github.com/cloudera/k8s-shim/pkg/scheduler/conf"
	"github.com/cloudera/k8s-shim/pkg/state/cache"
	"github.com/cloudera/k8s-shim/pkg/test"
	"github.com/cloudera/scheduler-interface/lib/go/si"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestAddNode(t *testing.T) {
	api := test.FakeSchedulerApi{
		// register fn doesn't nothing than checking input
		UpdateFn: func(request *si.UpdateRequest) error {
			if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
				t.Fatalf("unexpected new nodes info from the request")
			}

			info := request.NewSchedulableNodes[0]
			if info.NodeId != "host0001" {
				t.Fatalf("unexpected node name %s", info.NodeId)
			}

			if memory := info.SchedulableResource.Resources[conf.Memory].Value; memory != int64(1024) {
				t.Fatalf("unexpected node memory %d", memory)
			}

			if cpu := info.SchedulableResource.Resources[conf.CPU].Value; cpu != int64(10000) {
				t.Fatalf("unexpected node CPU %d", cpu)
			}

			return nil
		},
	}

	nc := NodeController{
		proxy: &api,
		cache: NewTestSchedulerCache(),
	}
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: resourceList,
		},
	}

	nc.AddNode(&newNode)
	// values are verified in injected fn
	// verify register is not called, update is called and just called once
	assert.Equal(t, api.RegisterCount, 0)
	assert.Equal(t, api.UpdateCount, 1)
}

func TestUpdateNode(t *testing.T) {
	api := test.FakeSchedulerApi{
		// register fn doesn't nothing than checking input
		UpdateFn: func(request *si.UpdateRequest) error {
			if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
				t.Fatalf("unexpected new nodes info from the request")
			}

			info := request.NewSchedulableNodes[0]
			if info.NodeId != "host0001" {
				t.Fatalf("unexpected node name %s", info.NodeId)
			}

			if memory := info.SchedulableResource.Resources[conf.Memory].Value; memory != int64(1024) {
				t.Fatalf("unexpected node memory %d", memory)
			}

			if cpu := info.SchedulableResource.Resources[conf.CPU].Value; cpu != int64(10000) {
				t.Fatalf("unexpected node CPU %d", cpu)
			}

			return nil
		},
	}

	nc := NodeController{
		proxy: &api,
		cache: NewTestSchedulerCache(),
	}
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var oldNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: resourceList,
		},
	}

	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: resourceList,
		},
	}

	// if node resource stays same, update update should be ignored
	ignoreNodeUpdateFn := func(request *si.UpdateRequest) error {
		if request.UpdatedNodes != nil && len(request.UpdatedNodes) > 0 {
			t.Fatalf("expecting no update nodes sent to scheduler as node resource has no change")
		}

		return nil
	}
	api.UpdateFn = ignoreNodeUpdateFn
	nc.UpdateNode(&oldNode, &newNode)
	assert.Equal(t, api.RegisterCount, 0)
	assert.Equal(t, api.UpdateCount, 0)

	// change new node's resource, afterwards the update request should be sent to the scheduler
	newResourceList := make(map[v1.ResourceName]resource.Quantity)
	newResourceList[v1.ResourceName("memory")] = *resource.NewQuantity(2048*1000*1000, resource.DecimalSI)
	newResourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: newResourceList,
		},
	}

	api.UpdateFn = func(request *si.UpdateRequest) error {
		if request.UpdatedNodes == nil || len(request.UpdatedNodes) != 1 {
			t.Fatalf("unexpected new nodes info from the request")
		}

		info := request.UpdatedNodes[0]
		if info.NodeId != "host0001" {
			t.Fatalf("unexpected node name %s", info.NodeId)
		}

		if memory := info.SchedulableResource.Resources[conf.Memory].Value; memory != int64(2048) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[conf.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	}

	nc.UpdateNode(&oldNode, &newNode)
	assert.Equal(t, api.RegisterCount, 0)
	assert.Equal(t, api.UpdateCount, 1)
}

func TestDeleteNode(t *testing.T) {
	api := test.FakeSchedulerApi{
		// register fn doesn't nothing than checking input
		UpdateFn: func(request *si.UpdateRequest) error {
			if request.UpdatedNodes == nil || len(request.UpdatedNodes) != 1 {
				t.Fatalf("unexpected updated nodes info from the request")
			}

			info := request.UpdatedNodes[0]
			if info.NodeId != "host0001" {
				t.Fatalf("unexpected node name %s", info.NodeId)
			}

			if memory := info.SchedulableResource.Resources[conf.Memory].Value; memory != int64(1024) {
				t.Fatalf("unexpected node memory %d", memory)
			}

			if cpu := info.SchedulableResource.Resources[conf.CPU].Value; cpu != int64(10000) {
				t.Fatalf("unexpected node CPU %d", cpu)
			}

			return nil
		},
	}

	nc := NodeController{
		proxy: &api,
		cache: NewTestSchedulerCache(),
	}
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var node = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity: resourceList,
		},
	}

	ignoreNodeUpdateFn := func(request *si.UpdateRequest) error {
		// fake update
		return nil
	}
	api.UpdateFn = ignoreNodeUpdateFn
	nc.DeleteNode(&node)
	assert.Equal(t, api.RegisterCount, 0)
	assert.Equal(t, api.UpdateCount, 1)
}

// A wrapper around the scheduler cache which does not initialise the lister and volumebinder
func NewTestSchedulerCache() *cache.SchedulerCache {
	return cache.NewSchedulerCache(nil, nil, nil, nil)
}
