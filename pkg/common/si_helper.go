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

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func CreateTagsForTask(pod *v1.Pod) map[string]string {
	metaPrefix := common.DomainK8s + common.GroupMeta
	tags := map[string]string{
		metaPrefix + common.KeyNamespace: pod.Namespace,
		metaPrefix + common.KeyPodName:   pod.Name,
	}
	owners := pod.GetOwnerReferences()
	if len(owners) > 0 {
		for _, value := range owners {
			if value.Kind == constants.DaemonSetType {
				if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
					pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
					log.Logger().Debug("DaemonSet pod's Affinity, NodeAffinity, RequiredDuringSchedulingIgnoredDuringExecution might empty")
					continue
				}
				nodeSelectorTerms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
				for _, term := range nodeSelectorTerms {
					for _, match := range term.MatchFields {
						if match.Key == "metadata.name" {
							tags[common.DomainYuniKorn+common.KeyRequiredNode] = match.Values[0]
						}
					}
				}
			}
		}
	}
	// add Pod labels to Task tags
	labelPrefix := common.DomainK8s + common.GroupLabel
	for k, v := range pod.Labels {
		tags[labelPrefix+k] = v
	}

	return tags
}

func CreateAllocationRequestForTask(appID, taskID string, resource *si.Resource, placeholder bool, taskGroupName string, pod *v1.Pod) si.AllocationRequest {
	ask := si.AllocationAsk{
		AllocationKey:  taskID,
		ResourceAsk:    resource,
		ApplicationID:  appID,
		MaxAllocations: 1,
		Tags:           CreateTagsForTask(pod),
		Placeholder:    placeholder,
		TaskGroupName:  taskGroupName,
	}

	result := si.AllocationRequest{
		Asks:                []*si.AllocationAsk{&ask},
		RmID:                conf.GetSchedulerConf().ClusterID,
	}

	return result
}

func CreateReleaseAskRequestForTask(appID, taskId, partition string) si.AllocationRequest {
	toReleases := make([]*si.AllocationAskRelease, 0)
	toReleases = append(toReleases, &si.AllocationAskRelease{
		ApplicationID: appID,
		Allocationkey: taskId,
		PartitionName: partition,
		Message:       "task request is canceled",
	})

	releaseRequest := si.AllocationReleasesRequest{
		AllocationAsksToRelease: toReleases,
	}

	result := si.AllocationRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}

	return result
}

func GetTerminationTypeFromString(terminationTypeStr string) si.TerminationType {
	if v, ok := si.TerminationType_value[terminationTypeStr]; ok {
		return si.TerminationType(v)
	}
	return si.TerminationType_STOPPED_BY_RM
}

func CreateReleaseAllocationRequestForTask(appID, allocUUID, partition, terminationType string) si.AllocationRequest {
	toReleases := make([]*si.AllocationRelease, 0)
	toReleases = append(toReleases, &si.AllocationRelease{
		ApplicationID:   appID,
		UUID:            allocUUID,
		PartitionName:   partition,
		TerminationType: GetTerminationTypeFromString(terminationType),
		Message:         "task completed",
	})

	releaseRequest := si.AllocationReleasesRequest{
		AllocationsToRelease: toReleases,
	}

	result := si.AllocationRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}

	return result
}

func CreateUpdateRequestForNewNode(node Node) si.NodeRequest {
	// Use node's name as the NodeID, this is because when bind pod to node,
	// name of node is required but uid is optional.
	nodeInfo := &si.NodeInfo{
		NodeID:              node.name,
		SchedulableResource: node.capacity,
		// TODO is this required?
		Attributes: map[string]string{
			constants.DefaultNodeAttributeHostNameKey: node.name,
			constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
		},
		Action: si.NodeInfo_CREATE,
	}

	nodes := make([]*si.NodeInfo, 1)
	nodes[0] = nodeInfo
	request := si.NodeRequest{
		Nodes: nodes,
		RmID:                conf.GetSchedulerConf().ClusterID,
	}
	return request
}

func CreateUpdateRequestForUpdatedNode(node Node) si.NodeRequest {
	// Currently only includes resource in the update request
	nodeInfo := &si.NodeInfo{
		NodeID:              node.name,
		Attributes:          make(map[string]string),
		SchedulableResource: node.capacity,
		OccupiedResource:    node.occupied,
		Action:              si.NodeInfo_UPDATE,
	}

	nodes := make([]*si.NodeInfo, 1)
	nodes[0] = nodeInfo
	request := si.NodeRequest{
		Nodes: nodes,
		RmID:         conf.GetSchedulerConf().ClusterID,
	}
	return request
}

func CreateUpdateRequestForDeleteNode(node Node) si.NodeRequest {
	deletedNodes := make([]*si.NodeInfo, 1)
	nodeInfo := &si.NodeInfo{
		NodeID:              node.name,
		SchedulableResource: node.capacity,
		OccupiedResource:    node.occupied,
		Attributes:          make(map[string]string),
		Action:              si.NodeInfo_DECOMISSION,
	}

	deletedNodes[0] = nodeInfo
	request := si.NodeRequest{
		Nodes: deletedNodes,
		RmID:         conf.GetSchedulerConf().ClusterID,
	}
	return request
}

func CreateUpdateRequestForRemoveApplication(appID, partition string) si.ApplicationRequest {
	removeApp := make([]*si.RemoveApplicationRequest, 0)
	removeApp = append(removeApp, &si.RemoveApplicationRequest{
		ApplicationID: appID,
		PartitionName: partition,
	})
	request := si.ApplicationRequest{
		Remove: removeApp,
		RmID:               conf.GetSchedulerConf().ClusterID,
	}

	return request
}
