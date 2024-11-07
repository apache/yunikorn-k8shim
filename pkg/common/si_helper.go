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
	ask := si.Allocation{
		AllocationKey:    taskID,
		ResourcePerAlloc: resource,
		ApplicationID:    appID,
		AllocationTags:   CreateTagsForTask(pod),
		Placeholder:      placeholder,
		TaskGroupName:    taskGroupName,
		Originator:       originator,
		Priority:         CreatePriorityForTask(pod),
		PreemptionPolicy: preemptionPolicy,
	}

	return &si.AllocationRequest{
		Allocations: []*si.Allocation{&ask},
		RmID:        conf.GetSchedulerConf().ClusterID,
	}
}

func CreateAllocationForTask(appID, taskID, nodeID string, resource *si.Resource, placeholder bool, taskGroupName string, pod *v1.Pod, originator bool, preemptionPolicy *si.PreemptionPolicy) *si.AllocationRequest {
	allocation := si.Allocation{
		AllocationKey:    taskID,
		AllocationTags:   CreateTagsForTask(pod),
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

func CreateAllocationForForeignPod(pod *v1.Pod) *si.AllocationRequest {
	podType := common.AllocTypeDefault
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == constants.NodeKind {
			podType = common.AllocTypeStatic
			break
		}
	}

	tags := CreateTagsForTask(pod)
	tags[common.Foreign] = podType
	tags[common.CreationTime] = strconv.FormatInt(pod.CreationTimestamp.Unix(), 10)
	allocation := si.Allocation{
		AllocationTags:   tags,
		AllocationKey:    string(pod.UID),
		ResourcePerAlloc: GetPodResource(pod),
		Priority:         CreatePriorityForTask(pod),
		NodeID:           pod.Spec.NodeName,
	}

	return &si.AllocationRequest{
		Allocations: []*si.Allocation{&allocation},
		RmID:        conf.GetSchedulerConf().ClusterID,
	}
}

func GetTerminationTypeFromString(terminationTypeStr string) si.TerminationType {
	if v, ok := si.TerminationType_value[terminationTypeStr]; ok {
		return si.TerminationType(v)
	}
	return si.TerminationType_STOPPED_BY_RM
}

func CreateReleaseRequestForTask(appID, taskID, partition string, terminationType si.TerminationType) *si.AllocationRequest {
	allocToRelease := make([]*si.AllocationRelease, 1)
	allocToRelease[0] = &si.AllocationRelease{
		ApplicationID:   appID,
		AllocationKey:   taskID,
		PartitionName:   partition,
		TerminationType: terminationType,
		Message:         "task completed",
	}

	releaseRequest := si.AllocationReleasesRequest{
		AllocationsToRelease: allocToRelease,
	}

	return &si.AllocationRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}
}

func CreateReleaseRequestForForeignPod(uid, partition string) *si.AllocationRequest {
	allocToRelease := make([]*si.AllocationRelease, 1)
	allocToRelease[0] = &si.AllocationRelease{
		AllocationKey:   uid,
		PartitionName:   partition,
		TerminationType: si.TerminationType_STOPPED_BY_RM,
		Message:         "pod terminated",
	}

	releaseRequest := si.AllocationReleasesRequest{
		AllocationsToRelease: allocToRelease,
	}

	return &si.AllocationRequest{
		Releases: &releaseRequest,
		RmID:     conf.GetSchedulerConf().ClusterID,
	}
}

// CreateUpdateRequestForUpdatedNode builds a NodeRequest for capacity updates
func CreateUpdateRequestForUpdatedNode(nodeID string, capacity *si.Resource) *si.NodeRequest {
	nodeInfo := &si.NodeInfo{
		NodeID:              nodeID,
		Attributes:          map[string]string{},
		SchedulableResource: capacity,
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
