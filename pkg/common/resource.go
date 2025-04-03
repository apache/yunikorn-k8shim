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
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	helpers "k8s.io/component-helpers/resource"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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

// GetPodResource from a pod's containers and convert that into an internal resource.
// Scheduling only accounts for requests.
// Convert the pod into a resource to allow for pod count checks in quotas and nodes.
func GetPodResource(pod *v1.Pod) (resource *si.Resource) {
	podResource := &si.Resource{
		Resources: map[string]*si.Quantity{"pods": {Value: 1}},
	}

	// Init container statuses and pod statuses are reported in separate places, so build a fixed map here to make
	// this easier to compute. This is ever so slightly more resource-intensive than computing inline, but is far more
	// readable and less error prone. Since resource calculation is only done once per pod update, this overhead is
	// negligible.
	containerStatuses := make(map[string]*v1.ContainerStatus, len(pod.Status.ContainerStatuses)+len(pod.Status.InitContainerStatuses))
	for idx := range pod.Status.ContainerStatuses {
		containerStatuses[pod.Status.ContainerStatuses[idx].Name] = &pod.Status.ContainerStatuses[idx]
	}
	for idx := range pod.Status.InitContainerStatuses {
		containerStatuses[pod.Status.InitContainerStatuses[idx].Name] = &pod.Status.InitContainerStatuses[idx]
	}

	// Add usage for each container
	for _, container := range pod.Spec.Containers {
		podResource = Add(podResource, computeContainerResource(pod, &container, containerStatuses))
	}

	// each resource compare between initcontainer and sum of containers
	// InitContainers(i) requirement=sum(Sidecar requirement i-1)+InitContainer(i) request
	// max(sum(Containers requirement)+sum(Sidecar requirement), InitContainer(i) requirement)
	if len(pod.Spec.InitContainers) > 0 {
		podResource = checkInitContainerRequest(pod, podResource, containerStatuses)
	}

	// PodLevelResources feature:
	// alpha: v1.32
	// beta: v1.33
	if pod.Spec.Resources != nil && len(pod.Spec.Resources.Requests) > 0 {
		// pod-level resources, if present, override sum of container-level resources
		// only memory and cpu are supported
		for name, value := range getPodLevelResource(pod.Spec.Resources.Requests).GetResources() {
			podResource.Resources[name] = value
		}
	}

	// K8s EnableOverHead feature:
	// Enables PodOverhead, for accounting pod overheads which are specific to a given RuntimeClass
	if pod.Spec.Overhead != nil {
		podOverHeadResource := getResource(pod.Spec.Overhead)
		podResource = Add(podResource, podOverHeadResource)
		// Logging the overall pod size and pod overhead
		log.Log(log.ShimResources).Debug("Pod overhead specified, overall pod size adjusted",
			zap.String("taskID", string(pod.UID)),
			zap.Stringer("overallSize", podResource),
			zap.Stringer("overheadSize", podOverHeadResource))
	}

	return podResource
}

// computeContainerResource computes the max(spec...resources, status...allocatedResources, status...resources)
// per KEP-1287 (in-place update of pod resources), unless resize status is PodResizeStatusInfeasible.
func computeContainerResource(pod *v1.Pod, container *v1.Container, containerStatuses map[string]*v1.ContainerStatus) *si.Resource {
	combined := &si.Resource{Resources: make(map[string]*si.Quantity)}
	updateMax(combined, getResource(container.Resources.Requests))
	if containerStatus := containerStatuses[container.Name]; containerStatus != nil {
		if isResizeInfeasible(pod) && containerStatus.Resources != nil {
			// resize request was denied; use container status requests as current value
			return getResource(containerStatus.Resources.Requests)
		}
		updateMax(combined, getResource(containerStatus.AllocatedResources))
		if containerStatus.Resources != nil {
			updateMax(combined, getResource(containerStatus.Resources.Requests))
		}
	}
	return combined
}

// isResizeInfeasible determines if a currently in-progress pod resize is infeasible. This takes into account both the
// current pod.Status.Resize field as well as the upcoming PodResizePending pod condition (in spec, but not yet
// implemented).
func isResizeInfeasible(pod *v1.Pod) bool {
	if pod.Status.Resize == v1.PodResizeStatusInfeasible {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == constants.PodStatusPodResizePending && condition.Reason == string(v1.PodResizeStatusInfeasible) {
			return true
		}
	}
	return false
}

// updateMax merges two resource lists into the leftmost, taking the max values of each resource type.
func updateMax(left *si.Resource, right *si.Resource) {
	// short-circuit out if empty
	if right == nil {
		return
	}
	for key, rValue := range right.GetResources() {
		lValue, ok := left.Resources[key]
		if !ok {
			// add new resource from right
			left.Resources[key] = rValue
			continue
		}
		if rValue.GetValue() > lValue.GetValue() {
			// update resource with larger right value
			left.Resources[key] = rValue
		}
	}
}

func checkInitContainerRequest(pod *v1.Pod, containersResources *si.Resource, containerStatuses map[string]*v1.ContainerStatus) *si.Resource {
	updatedRes := containersResources

	// update total pod resource usage with sidecar containers
	for _, c := range pod.Spec.InitContainers {
		if isSideCarContainer(&c) {
			sideCarResources := computeContainerResource(pod, &c, containerStatuses)
			updatedRes = Add(updatedRes, sideCarResources)
		}
	}

	var sideCarRequests *si.Resource // cumulative value of sidecar requests so far
	for _, c := range pod.Spec.InitContainers {
		ICResource := computeContainerResource(pod, &c, containerStatuses)
		if isSideCarContainer(&c) {
			sideCarRequests = Add(sideCarRequests, ICResource)
		}
		ICResource = Add(ICResource, sideCarRequests)
		for resourceName, ICRequest := range ICResource.Resources {
			containersRequests, exist := updatedRes.Resources[resourceName]
			// additional resource request from init cont, add it to request.
			if !exist {
				updatedRes.Resources[resourceName] = ICRequest
				continue
			}
			if ICRequest.GetValue() > containersRequests.GetValue() {
				updatedRes.Resources[resourceName] = ICRequest
			}
		}
	}

	return updatedRes
}

func isSideCarContainer(c *v1.Container) bool {
	return c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways
}

func GetNodeResource(nodeStatus *v1.NodeStatus) *si.Resource {
	// Capacity represents the total capacity of the node
	// Allocatable represents the resources of a node that are available for scheduling.
	// Each kubelet can reserve some resources from the scheduler.
	// We can rely on Allocatable resource here, because if it is not specified,
	// the default value is same as Capacity. (same behavior as the default-scheduler)
	return getResource(nodeStatus.Allocatable)
}

// parse cpu and memory from string to si.Resource, both of them are optional
// if parse failed with some errors, log the error and return a nil
func ParseResource(cpuStr, memStr string) *si.Resource {
	if cpuStr == "" && memStr == "" {
		return nil
	}

	result := NewResourceBuilder()
	if cpuStr != "" {
		if vcore, err := resource.ParseQuantity(cpuStr); err == nil {
			result.AddResource(siCommon.CPU, vcore.MilliValue())
		} else {
			log.Log(log.ShimResources).Error("failed to parse cpu resource",
				zap.String("cpuStr", cpuStr),
				zap.Error(err))
			return nil
		}
	}

	if memStr != "" {
		if mem, err := resource.ParseQuantity(memStr); err == nil {
			result.AddResource(siCommon.Memory, mem.Value())
		} else {
			log.Log(log.ShimResources).Error("failed to parse memory resource",
				zap.String("memStr", memStr),
				zap.Error(err))
			return nil
		}
	}

	return result.Build()
}

func GetResource(resMap map[string]string) *si.Resource {
	result := NewResourceBuilder()
	for resName, resValue := range resMap {
		switch resName {
		case v1.ResourceCPU.String():
			if actualValue, err := resource.ParseQuantity(resValue); err == nil {
				result.AddResource(siCommon.CPU, actualValue.MilliValue())
			} else {
				log.Log(log.ShimResources).Error("failed to parse cpu resource",
					zap.String("res name", "cpu"),
					zap.String("res value", resValue),
					zap.Error(err))
				return nil
			}
		default:
			if actualValue, err := resource.ParseQuantity(resValue); err == nil {
				result.AddResource(resName, actualValue.Value())
			} else {
				log.Log(log.ShimResources).Error("failed to parse resource",
					zap.String("res name", resName),
					zap.String("res value", resValue),
					zap.Error(err))
				return nil
			}
		}
	}
	return result.Build()
}

func GetTGResource(resMap map[string]resource.Quantity, members int64) *si.Resource {
	result := NewResourceBuilder()
	result.AddResource("pods", members)
	for resName, resValue := range resMap {
		switch resName {
		case v1.ResourceCPU.String():
			result.AddResource(siCommon.CPU, members*resValue.MilliValue())
		default:
			result.AddResource(resName, members*resValue.Value())
		}
	}
	return result.Build()
}

func getResource(resourceList v1.ResourceList) *si.Resource {
	resources := NewResourceBuilder()
	for name, value := range resourceList {
		switch name {
		case v1.ResourceCPU:
			vcore := value.MilliValue()
			resources.AddResource(siCommon.CPU, vcore)
		default:
			resources.AddResource(string(name), value.Value())
		}
	}
	return resources.Build()
}

func getPodLevelResource(resourceList v1.ResourceList) *si.Resource {
	resources := NewResourceBuilder()
	for name, value := range resourceList {
		if helpers.IsSupportedPodLevelResource(name) {
			switch name {
			case v1.ResourceCPU:
				vcore := value.MilliValue()
				resources.AddResource(siCommon.CPU, vcore)
			default:
				resources.AddResource(string(name), value.Value())
			}
		}
	}
	return resources.Build()
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

func Sub(left *si.Resource, right *si.Resource) *si.Resource {
	if left == nil {
		left = &si.Resource{}
	}
	if right == nil {
		return left
	}

	// clone left
	rb := NewResourceBuilder()
	for k, v := range left.Resources {
		rb.AddResource(k, v.Value)
	}
	result := rb.Build()

	// sub right
	for k, v := range right.Resources {
		if _, ok := result.Resources[k]; !ok {
			result.Resources[k] = &si.Quantity{
				Value: -v.Value,
			}
		} else {
			result.Resources[k].Value -= v.Value
		}
	}

	return result
}

func IsZero(r *si.Resource) bool {
	if r == nil {
		return true
	}
	for _, v := range r.Resources {
		if v.Value != 0 {
			return false
		}
	}
	return true
}
