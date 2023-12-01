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

package cache

import (
	"strconv"
	"sync/atomic"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sCache "k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// AppManagementService is a central service that interacts with
// one or more K8s operators for app scheduling.
type AppManagementService struct {
	apiProvider     client.APIProvider
	amProtocol      ApplicationManagementProtocol
	podEventHandler *PodEventHandler
	cancelRecovery  atomic.Bool
}

func NewAMService(amProtocol ApplicationManagementProtocol, apiProvider client.APIProvider) *AppManagementService {
	podEventHandler := NewPodEventHandler(amProtocol, true)

	log.Log(log.ShimCacheAppMgmt).Info("Initializing new AppMgmt service")
	return &AppManagementService{
		apiProvider:     apiProvider,
		amProtocol:      amProtocol,
		podEventHandler: podEventHandler,
	}
}

func (svc *AppManagementService) Start() error {
	svc.apiProvider.AddEventHandler(
		&client.ResourceEventHandlers{
			Type:     client.PodInformerHandlers,
			FilterFn: svc.filterPods,
			AddFn:    svc.AddPod,
			UpdateFn: svc.updatePod,
			DeleteFn: svc.deletePod,
		})
	return nil
}

func (svc *AppManagementService) ListPods() ([]*v1.Pod, error) {
	log.Log(log.ShimCacheAppMgmt).Info("Retrieving pod list")
	// list all pods on this cluster
	appPods, err := svc.apiProvider.GetAPIs().PodInformer.Lister().List(labels.NewSelector())
	if err != nil {
		return nil, err
	}
	log.Log(log.ShimCacheAppMgmt).Info("Pod list retrieved from api server", zap.Int("nr of pods", len(appPods)))
	// get existing apps
	existingApps := make(map[string]struct{})
	podsRecovered := 0
	podsWithoutMetaData := 0
	pods := make([]*v1.Pod, 0)
	for _, pod := range appPods {
		log.Log(log.ShimCacheAppMgmt).Debug("Looking at pod for recovery candidates", zap.String("podNamespace", pod.Namespace), zap.String("podName", pod.Name))
		// general filter passes, and pod is assigned
		// this means the pod is already scheduled by scheduler for an existing app
		if utils.GetApplicationIDFromPod(pod) != "" && utils.IsAssignedPod(pod) {
			if meta, ok := getAppMetadata(pod, true); ok {
				podsRecovered++
				pods = append(pods, pod)
				log.Log(log.ShimCacheAppMgmt).Debug("Adding appID as recovery candidate", zap.String("appID", meta.ApplicationID))
				existingApps[meta.ApplicationID] = struct{}{}
			} else {
				podsWithoutMetaData++
			}
		}
	}
	log.Log(log.ShimCacheAppMgmt).Info("Application recovery statistics",
		zap.Int("nr of recoverable apps", len(existingApps)),
		zap.Int("nr of total pods", len(appPods)),
		zap.Int("nr of pods without application metadata", podsWithoutMetaData),
		zap.Int("nr of pods to be recovered", podsRecovered))

	return pods, nil
}

func (svc *AppManagementService) GetExistingAllocation(pod *v1.Pod) *si.Allocation {
	if meta, valid := getAppMetadata(pod, false); valid {
		// when submit a task, we use pod UID as the allocationKey,
		// to keep consistent, during recovery, the pod UID is also used
		// for an Allocation.
		placeholder := utils.GetPlaceholderFlagFromPodSpec(pod)
		taskGroupName := utils.GetTaskGroupFromPodSpec(pod)

		creationTime := pod.CreationTimestamp.Unix()
		meta.Tags[siCommon.CreationTime] = strconv.FormatInt(creationTime, 10)

		return &si.Allocation{
			AllocationKey:    string(pod.UID),
			AllocationTags:   meta.Tags,
			AllocationID:     string(pod.UID),
			ResourcePerAlloc: common.GetPodResource(pod),
			NodeID:           pod.Spec.NodeName,
			ApplicationID:    meta.ApplicationID,
			Placeholder:      placeholder,
			TaskGroupName:    taskGroupName,
			PartitionName:    constants.DefaultPartition,
		}
	}
	return nil
}

// filter pods by scheduler name and state
func (svc *AppManagementService) filterPods(obj interface{}) bool {
	switch object := obj.(type) {
	case *v1.Pod:
		pod := object
		return utils.GetApplicationIDFromPod(pod) != ""
	default:
		return false
	}
}

// AddPod Add application and task using pod metadata
// Visibility: Public only for testing
func (svc *AppManagementService) AddPod(obj interface{}) {
	pod, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Log(log.ShimCacheAppMgmt).Error("failed to add pod", zap.Error(err))
		return
	}

	log.Log(log.ShimCacheAppMgmt).Debug("pod added",
		zap.String("Name", pod.Name),
		zap.String("Namespace", pod.Namespace))

	svc.podEventHandler.HandleEvent(AddPod, Informers, pod)
}

// when pod resource is modified, we need to act accordingly
// e.g vertical scale out the pod, this requires the scheduler to be aware of this
func (svc *AppManagementService) updatePod(old, new interface{}) {
	oldPod, err := utils.Convert2Pod(old)
	if err != nil {
		log.Log(log.ShimCacheAppMgmt).Error("expecting a pod object", zap.Error(err))
		return
	}

	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Log(log.ShimCacheAppMgmt).Error("expecting a pod object", zap.Error(err))
		return
	}

	// triggered when pod status' phase changes
	if oldPod.Status.Phase != newPod.Status.Phase {
		// pod succeed or failed means all containers in the pod have been terminated,
		// and these container won't be restarted. In this case, we can safely release
		// the resources for this allocation. And mark the task is done.
		if utils.IsPodTerminated(newPod) {
			log.Log(log.ShimCacheAppMgmt).Info("task completes",
				zap.String("namespace", newPod.Namespace),
				zap.String("podName", newPod.Name),
				zap.String("podUID", string(newPod.UID)),
				zap.String("podStatus", string(newPod.Status.Phase)))
			svc.podEventHandler.HandleEvent(UpdatePod, Informers, newPod)
		}
	}
}

// this function is called when a pod is deleted from api-server.
// when a pod is completed, the equivalent task's state will also be completed
// optionally, we run a completionHandler per workload, in order to determine
// if a application is completed along with this pod's completion
func (svc *AppManagementService) deletePod(obj interface{}) {
	// when a pod is deleted, we need to check its role.
	// for spark, if driver pod is deleted, then we consider the app is completed
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case k8sCache.DeletedFinalStateUnknown:
		var err error
		pod, err = utils.Convert2Pod(t.Obj)
		if err != nil {
			log.Log(log.ShimCacheAppMgmt).Error(err.Error())
			return
		}
	default:
		log.Log(log.ShimCacheAppMgmt).Error("cannot convert to pod")
		return
	}

	log.Log(log.ShimCacheAppMgmt).Info("delete pod",
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)))

	svc.podEventHandler.HandleEvent(DeletePod, Informers, pod)
}
