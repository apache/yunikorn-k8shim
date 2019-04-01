package controller

import (
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/test"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestAddNode(t *testing.T) {
	api := test.FakeSchedulerApi {
		// register fn doesn't nothing than checking input
		UpdateFn: func(request *si.UpdateRequest) error {
			if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
				t.Fatalf("unexpected new nodes info from the request")
			}

			info := request.NewSchedulableNodes[0]
			if info.NodeId != "host0001" {
				t.Fatalf("unexpected node name %s", info.NodeId)
			}

			if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(1024) {
				t.Fatalf("unexpected node memory %d", memory)
			}

			if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
				t.Fatalf("unexpected node CPU %d", cpu)
			}

			return nil
		},
	}

	nc := NodeController{proxy: &api}
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)
	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:            "host0001",
			Namespace:       "default",
			UID:             "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity:        resourceList,
		},
	}

	nc.AddNode(&newNode)
	// values are verified in injected fn
	// verify register is not called, update is called and just called once
	assert.Equal(t, api.RegisterCount, 0)
	assert.Equal(t, api.UpdateCount, 1)
}

func TestUpdateNode(t *testing.T) {
	api := test.FakeSchedulerApi {
		// register fn doesn't nothing than checking input
		UpdateFn: func(request *si.UpdateRequest) error {
			if request.NewSchedulableNodes == nil || len(request.NewSchedulableNodes) != 1 {
				t.Fatalf("unexpected new nodes info from the request")
			}

			info := request.NewSchedulableNodes[0]
			if info.NodeId != "host0001" {
				t.Fatalf("unexpected node name %s", info.NodeId)
			}

			if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(1024) {
				t.Fatalf("unexpected node memory %d", memory)
			}

			if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
				t.Fatalf("unexpected node CPU %d", cpu)
			}

			return nil
		},
	}

	nc := NodeController{proxy: &api}
	resourceList := make(map[v1.ResourceName]resource.Quantity)
	resourceList[v1.ResourceName("memory")] = *resource.NewQuantity(1024*1000*1000, resource.DecimalSI)
	resourceList[v1.ResourceName("cpu")] = *resource.NewQuantity(10, resource.DecimalSI)

	var oldNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:            "host0001",
			Namespace:       "default",
			UID:             "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity:        resourceList,
		},
	}

	var newNode = v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:            "host0001",
			Namespace:       "default",
			UID:             "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity:        resourceList,
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
			Name:            "host0001",
			Namespace:       "default",
			UID:             "uid_0001",
		},
		Status: v1.NodeStatus{
			Capacity:        newResourceList,
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

		if memory := info.SchedulableResource.Resources[common.Memory].Value; memory != int64(2048) {
			t.Fatalf("unexpected node memory %d", memory)
		}

		if cpu := info.SchedulableResource.Resources[common.CPU].Value; cpu != int64(10000) {
			t.Fatalf("unexpected node CPU %d", cpu)
		}

		return nil
	}

	nc.UpdateNode(&oldNode, &newNode)
	assert.Equal(t, api.RegisterCount, 0)
	assert.Equal(t, api.UpdateCount, 1)
}
