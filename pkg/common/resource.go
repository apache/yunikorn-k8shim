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

package common

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// resource builder is a helper struct to construct si resources
type ResourceBuilder struct {
	resourceMap map[string]*si.Quantity
}

func NewResourceBuilder() *ResourceBuilder {
	return &ResourceBuilder{
		resourceMap: make(map[string]*si.Quantity),
	}
}

func (w *ResourceBuilder) AddResource(name string, value int64) *ResourceBuilder {
	w.resourceMap[name] = &si.Quantity{Value: value}
	return w
}

func (w *ResourceBuilder) Build() *si.Resource {
	return &si.Resource{Resources: w.resourceMap}
}

// Get the resources from a pod's containers and convert that into a internal resource.
// A pod has two resource parts: Requests and Limits.
// Based on the values a pod gets a QOS assigned, as per
// https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod/
// QOS class Guaranteed and Burstable are supported. However Burstable is scheduled based on the request
// values, limits are ignored in the current setup.
// BestEffort pods are scheduled using a minimum resource of 1MB only.
func GetPodResource(pod *v1.Pod) (resource *si.Resource) {
	//var memory, vcore = int64(0), int64(0)
	var podResource *si.Resource
	// A QosBestEffort pod does not request any resources and thus cannot be
	// scheduled. Handle a QosBestEffort pod by setting a tiny memory value.
	if qos.GetPodQOS(pod) == v1.PodQOSBestEffort {
		resources := NewResourceBuilder()
		resources.AddResource(Memory, 1)
		return resources.Build()
	}

	for _, c := range pod.Spec.Containers {
		resourceList := c.Resources.Requests
		containerResource := getResource(resourceList)
		podResource = Add(podResource, containerResource)
	}
	return podResource
}

func GetNodeResource(nodeStatus *v1.NodeStatus) *si.Resource {
	return getResource(nodeStatus.Capacity)
}

func getResource(resourceList v1.ResourceList) *si.Resource {
	resources := NewResourceBuilder()
	for name, value := range resourceList {
		switch name {
		case v1.ResourceMemory:
			memory := value.ScaledValue(resource.Mega)
			resources.AddResource(Memory, memory)
		case v1.ResourceCPU:
			vcore := value.MilliValue()
			resources.AddResource(CPU, vcore)
		default:
			resources.AddResource(string(name), value.Value())
		}
	}
	return resources.Build()
}

func CreateUpdateRequestForTask(appID, taskID string, resource *si.Resource) si.UpdateRequest {
	ask := si.AllocationAsk{
		AllocationKey:  taskID,
		ResourceAsk:    resource,
		ApplicationID:  appID,
		MaxAllocations: 1,
	}

	result := si.UpdateRequest{
		Asks:                []*si.AllocationAsk{&ask},
		NewSchedulableNodes: nil,
		UpdatedNodes:        nil,
		UtilizationReports:  nil,
		RmID:                conf.GetSchedulerConf().ClusterID,
	}

	return result
}

func CreateReleaseAllocationRequestForTask(appID, allocUUID, partition string) si.UpdateRequest {
	toReleases := make([]*si.AllocationReleaseRequest, 0)
	toReleases = append(toReleases, &si.AllocationReleaseRequest{
		ApplicationID: appID,
		UUID:          allocUUID,
		PartitionName: partition,
		Message:       "task completed",
	})

	releaseRequest := si.AllocationReleasesRequest{
		AllocationsToRelease: toReleases,
	}

	result := si.UpdateRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}

	return result
}

func CreateUpdateRequestForNewNode(node Node) si.UpdateRequest {
	// Use node's name as the NodeID, this is because when bind pod to node,
	// name of node is required but uid is optional.
	nodeInfo := &si.NewNodeInfo{
		NodeID:              node.name,
		SchedulableResource: node.resource,
		// TODO is this required?
		Attributes: map[string]string{
			DefaultNodeAttributeHostNameKey: node.name,
			DefaultNodeAttributeRackNameKey: DefaultRackName,
		},
	}

	nodes := make([]*si.NewNodeInfo, 1)
	nodes[0] = nodeInfo
	request := si.UpdateRequest{
		NewSchedulableNodes: nodes,
		RmID:                conf.GetSchedulerConf().ClusterID,
	}
	return request
}

func CreateUpdateRequestForUpdatedNode(node Node) si.UpdateRequest {
	// Currently only includes resource in the update request
	nodeInfo := &si.UpdateNodeInfo{
		NodeID:              node.name,
		SchedulableResource: node.resource,
	}

	nodes := make([]*si.UpdateNodeInfo, 1)
	nodes[0] = nodeInfo
	request := si.UpdateRequest{
		UpdatedNodes: nodes,
		RmID:         conf.GetSchedulerConf().ClusterID,
	}
	return request
}

func CreateUpdateRequestForDeleteNode(node Node) si.UpdateRequest {
	deletedNodes := make([]*si.UpdateNodeInfo, 1)
	nodeInfo := &si.UpdateNodeInfo{
		NodeID:              node.name,
		SchedulableResource: node.resource,
		Action:              si.UpdateNodeInfo_DECOMISSION,
	}

	deletedNodes[0] = nodeInfo
	request := si.UpdateRequest{
		UpdatedNodes: deletedNodes,
		RmID:         conf.GetSchedulerConf().ClusterID,
	}
	return request
}

func Equals(left *si.Resource, right *si.Resource) bool {
	if left == right {
		return true
	}

	if left != nil && left.Resources != nil {
		for k, v := range left.Resources {
			if right == nil ||
				right.Resources[k] == nil ||
				right.Resources[k].Value != v.Value {
				return false
			}
		}
	}

	if right != nil && right.Resources != nil {
		for k, v := range right.Resources {
			if left == nil ||
				left.Resources[k] == nil ||
				left.Resources[k].Value != v.Value {
				return false
			}
		}
	}

	return true
}

func Add(left *si.Resource, right *si.Resource) *si.Resource {
	result := &si.Resource{Resources: make(map[string]*si.Quantity)}
	if left == nil && right == nil {
		return result
	}
	if right != nil {
		for k, v := range right.Resources {
			result.Resources[k] = v
		}
	}
	if left != nil {
		for k, v := range left.Resources {
			if er, ok := result.Resources[k]; ok {
				result.Resources[k] = &si.Quantity{Value: int64(er.Value + v.Value)}
				continue
			}
			result.Resources[k] = v
		}
	}
	return result
}
