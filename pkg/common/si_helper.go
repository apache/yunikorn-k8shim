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
	"strconv"

	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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
					log.Log(log.ShimUtils).Debug("DaemonSet pod's Affinity, NodeAffinity, RequiredDuringSchedulingIgnoredDuringExecution might empty")
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

func CreatePriorityForTask(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

func CreateAllocationRequestForTask(appID, taskID string, resource *si.Resource, placeholder bool, taskGroupName string, pod *v1.Pod, originator bool, preemptionPolicy *si.PreemptionPolicy) *si.AllocationRequest {
	ask := si.AllocationAsk{
		AllocationKey:    taskID,
		ResourceAsk:      resource,
		ApplicationID:    appID,
		MaxAllocations:   1,
		Tags:             CreateTagsForTask(pod),
		Placeholder:      placeholder,
		TaskGroupName:    taskGroupName,
		Originator:       originator,
		Priority:         CreatePriorityForTask(pod),
		PreemptionPolicy: preemptionPolicy,
	}

	return &si.AllocationRequest{
		Asks: []*si.AllocationAsk{&ask},
		RmID: conf.GetSchedulerConf().ClusterID,
	}
}

func CreateAllocationForTask(appID, taskID, nodeID string, resource *si.Resource, placeholder bool, taskGroupName string, pod *v1.Pod, originator bool, preemptionPolicy *si.PreemptionPolicy) *si.AllocationRequest {
	allocation := si.Allocation{
		AllocationKey:    taskID,
		AllocationTags:   CreateTagsForTask(pod),
		AllocationID:     taskID,
		ResourcePerAlloc: resource,
		Priority:         CreatePriorityForTask(pod),
		NodeID:           nodeID,
		ApplicationID:    appID,
		TaskGroupName:    taskGroupName,
		Placeholder:      placeholder,
		Originator:       originator,
		PreemptionPolicy: preemptionPolicy,
	}

	// add creation time for ask
	allocation.AllocationTags[common.CreationTime] = strconv.FormatInt(pod.CreationTimestamp.Unix(), 10)

	return &si.AllocationRequest{
		Allocations: []*si.Allocation{&allocation},
		RmID:        conf.GetSchedulerConf().ClusterID,
	}
}

func CreateReleaseAskRequestForTask(appID, taskID, partition string) *si.AllocationRequest {
	toReleases := make([]*si.AllocationAskRelease, 0)
	toReleases = append(toReleases, &si.AllocationAskRelease{
		ApplicationID: appID,
		AllocationKey: taskID,
		PartitionName: partition,
		Message:       "task request is canceled",
	})

	releaseRequest := si.AllocationReleasesRequest{
		AllocationAsksToRelease: toReleases,
	}

	return &si.AllocationRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}
}

func GetTerminationTypeFromString(terminationTypeStr string) si.TerminationType {
	if v, ok := si.TerminationType_value[terminationTypeStr]; ok {
		return si.TerminationType(v)
	}
	return si.TerminationType_STOPPED_BY_RM
}

func CreateReleaseAllocationRequestForTask(appID, allocationID, partition, terminationType string) *si.AllocationRequest {
	toReleases := make([]*si.AllocationRelease, 0)
	toReleases = append(toReleases, &si.AllocationRelease{
		ApplicationID:   appID,
		AllocationID:    allocationID,
		PartitionName:   partition,
		TerminationType: GetTerminationTypeFromString(terminationType),
		Message:         "task completed",
	})

	releaseRequest := si.AllocationReleasesRequest{
		AllocationsToRelease: toReleases,
	}

	return &si.AllocationRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}
}

// // CreateUpdateRequestForNewNode builds a NodeRequest for new node addition and restoring existing node
// func CreateUpdateRequestForNewNode(nodeID string, nodeLabels map[string]string, capacity *si.Resource, occupied *si.Resource,
// 	existingAllocations []*si.Allocation, ready bool) *si.NodeRequest {
// 	// Use node's name as the NodeID, this is because when bind pod to node,
// 	// name of node is required but uid is optional.
// 	nodeInfo := &si.NodeInfo{
// 		NodeID:              nodeID,
// 		SchedulableResource: capacity,
// 		OccupiedResource:    occupied,
// 		Attributes: map[string]string{
// 			constants.DefaultNodeAttributeHostNameKey: nodeID,
// 			constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
// 			common.NodeReadyAttribute:                 strconv.FormatBool(ready),
// 		},
// 		ExistingAllocations: existingAllocations,
// 		Action:              si.NodeInfo_CREATE,
// 	}

// 	// Add nodeLabels key value to Attributes map
// 	for k, v := range nodeLabels {
// 		nodeInfo.Attributes[k] = v
// 	}

// 	// Add instanceType to Attributes map
// 	nodeInfo.Attributes[common.InstanceType] = nodeLabels[conf.GetSchedulerConf().InstanceTypeNodeLabelKey]

// 	nodes := make([]*si.NodeInfo, 1)
// 	nodes[0] = nodeInfo
// 	return &si.NodeRequest{
// 		Nodes: nodes,
// 		RmID:  conf.GetSchedulerConf().ClusterID,
// 	}
// }
// ### NOTE =>  To bring this test back

// CreateUpdateRequestForUpdatedNode builds a NodeRequest for any node updates like capacity,
// ready status flag etc
func CreateUpdateRequestForUpdatedNode(nodeID string, capacity *si.Resource, occupied *si.Resource, ready bool) *si.NodeRequest {
	nodeInfo := &si.NodeInfo{
		NodeID: nodeID,
		Attributes: map[string]string{
			common.NodeReadyAttribute: strconv.FormatBool(ready),
		},
		SchedulableResource: capacity,
		OccupiedResource:    occupied,
		Action:              si.NodeInfo_UPDATE,
	}

	nodes := make([]*si.NodeInfo, 1)
	nodes[0] = nodeInfo
	return &si.NodeRequest{
		Nodes: nodes,
		RmID:  conf.GetSchedulerConf().ClusterID,
	}
}

// CreateUpdateRequestForDeleteOrRestoreNode builds a NodeRequest for Node actions like drain,
// decommissioning & restore
func CreateUpdateRequestForDeleteOrRestoreNode(nodeID string, action si.NodeInfo_ActionFromRM) *si.NodeRequest {
	deletedNodes := make([]*si.NodeInfo, 1)
	nodeInfo := &si.NodeInfo{
		NodeID: nodeID,
		Action: action,
	}

	deletedNodes[0] = nodeInfo
	return &si.NodeRequest{
		Nodes: deletedNodes,
		RmID:  conf.GetSchedulerConf().ClusterID,
	}
}

func CreateUpdateRequestForRemoveApplication(appID, partition string) *si.ApplicationRequest {
	removeApp := make([]*si.RemoveApplicationRequest, 0)
	removeApp = append(removeApp, &si.RemoveApplicationRequest{
		ApplicationID: appID,
		PartitionName: partition,
	})
	return &si.ApplicationRequest{
		Remove: removeApp,
		RmID:   conf.GetSchedulerConf().ClusterID,
	}
}
