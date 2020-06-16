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

	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func createTagsForTask(pod *v1.Pod) map[string]string {
	metaPrefix := common.DomainK8s + common.GroupMeta
	tags := map[string]string{
		metaPrefix + common.KeyNamespace: pod.Namespace,
		metaPrefix + common.KeyPodName:   pod.Name,
	}

	// add Pod labels to Task tags
	labelPrefix := common.DomainK8s + common.GroupLabel
	for k, v := range pod.Labels {
		tags[labelPrefix + k] = v
	}

	return tags
}

func CreateUpdateRequestForTask(appID, taskID string, resource *si.Resource, pod *v1.Pod) si.UpdateRequest {
	ask := si.AllocationAsk{
		AllocationKey:  taskID,
		ResourceAsk:    resource,
		ApplicationID:  appID,
		MaxAllocations: 1,
		Tags:			createTagsForTask(pod),
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

func CreateReleaseAskRequestForTask(appID, taskId, partition string) si.UpdateRequest {
	toReleases := make([]*si.AllocationAskReleaseRequest, 0)
	toReleases = append(toReleases, &si.AllocationAskReleaseRequest{
		ApplicationID: appID,
		Allocationkey: taskId,
		PartitionName: partition,
		Message:       "task request is canceled",
	})

	releaseRequest := si.AllocationReleasesRequest{
		AllocationAsksToRelease: toReleases,
	}

	result := si.UpdateRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
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
		SchedulableResource: node.capacity,
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
		Attributes:          make(map[string]string),
		SchedulableResource: node.capacity,
		OccupiedResource:    node.occupied,
		Action:              si.UpdateNodeInfo_UPDATE,
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
		SchedulableResource: node.capacity,
		OccupiedResource:    node.occupied,
		Attributes:          make(map[string]string),
		Action:              si.UpdateNodeInfo_DECOMISSION,
	}

	deletedNodes[0] = nodeInfo
	request := si.UpdateRequest{
		UpdatedNodes: deletedNodes,
		RmID:         conf.GetSchedulerConf().ClusterID,
	}
	return request
}
