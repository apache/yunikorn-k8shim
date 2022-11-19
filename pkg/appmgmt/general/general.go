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

package general

import (
	"reflect"
	"strconv"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sCache "k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
)

// Manager implements interfaces#Recoverable, interfaces#AppManager
// generic app management service watches events from all the pods,
// it recognize apps by reading pod's spec labels, if there are proper info such as
// applicationID, queue name found, and claim it as an app or a app task,
// then report them to scheduler cache by calling am protocol
type Manager struct {
	apiProvider            client.APIProvider
	gangSchedulingDisabled bool
	podEventHandler        *PodEventHandler
}

func NewManager(apiProvider client.APIProvider, podEventHandler *PodEventHandler) *Manager {
	return &Manager{
		apiProvider:            apiProvider,
		gangSchedulingDisabled: conf.GetSchedulerConf().DisableGangScheduling,
		podEventHandler:        podEventHandler,
	}
}

// this implements AppManagementService interface
func (os *Manager) Name() string {
	return "general"
}

// this implements AppManagementService interface
func (os *Manager) ServiceInit() error {
	os.apiProvider.AddEventHandler(
		&client.ResourceEventHandlers{
			Type:     client.PodInformerHandlers,
			FilterFn: os.filterPods,
			AddFn:    os.AddPod,
			UpdateFn: os.updatePod,
			DeleteFn: os.deletePod,
		})
	return nil
}

// this implements AppManagementService interface
func (os *Manager) Start() error {
	// generic app manager leverages the shared context,
	// no other service, go routine is required to be started
	return nil
}

// this implements AppManagementService interface
func (os *Manager) Stop() {
	// noop
}

func isStateAwareDisabled(pod *v1.Pod) bool {
	value := utils.GetPodLabelValue(pod, constants.LabelDisableStateAware)
	if value == "" {
		return false
	}
	result, err := strconv.ParseBool(value)
	if err != nil {
		log.Logger().Debug("unable to parse label for pod",
			zap.String("namespace", pod.Namespace),
			zap.String("name", pod.Name),
			zap.String("label", constants.LabelDisableStateAware),
			zap.Error(err))
		return false
	}
	return result
}

func getOwnerReferences(pod *v1.Pod) []metav1.OwnerReference {
	if len(pod.OwnerReferences) > 0 {
		return pod.OwnerReferences
	}
	controller := false
	blockOwnerDeletion := true
	ref := metav1.OwnerReference{
		APIVersion:         v1.SchemeGroupVersion.String(),
		Kind:               reflect.TypeOf(v1.Pod{}).Name(),
		Name:               pod.Name,
		UID:                pod.UID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
	return []metav1.OwnerReference{ref}
}

// filter pods by scheduler name and state
func (os *Manager) filterPods(obj interface{}) bool {
	switch obj.(type) {
	case *v1.Pod:
		pod := obj.(*v1.Pod)
		if utils.GeneralPodFilter(pod) {
			// only application ID is required
			if _, err := utils.GetApplicationIDFromPod(pod); err == nil {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// AddPod Add application and task using pod metadata
// Visibility: Public only for testing
func (os *Manager) AddPod(obj interface{}) {
	pod, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Logger().Error("failed to add pod", zap.Error(err))
		return
	}

	log.Logger().Debug("pod added",
		zap.String("appType", os.Name()),
		zap.String("Name", pod.Name),
		zap.String("Namespace", pod.Namespace))

	os.podEventHandler.HandleEvent(AddPod, Informers, pod)
}

// when pod resource is modified, we need to act accordingly
// e.g vertical scale out the pod, this requires the scheduler to be aware of this
func (os *Manager) updatePod(old, new interface{}) {
	oldPod, err := utils.Convert2Pod(old)
	if err != nil {
		log.Logger().Error("expecting a pod object", zap.Error(err))
		return
	}

	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Logger().Error("expecting a pod object", zap.Error(err))
		return
	}

	// triggered when pod status' phase changes
	if oldPod.Status.Phase != newPod.Status.Phase {
		// pod succeed or failed means all containers in the pod have been terminated,
		// and these container won't be restarted. In this case, we can safely release
		// the resources for this allocation. And mark the task is done.
		if utils.IsPodTerminated(newPod) {
			log.Logger().Info("task completes",
				zap.String("appType", os.Name()),
				zap.String("namespace", newPod.Namespace),
				zap.String("podName", newPod.Name),
				zap.String("podUID", string(newPod.UID)),
				zap.String("podStatus", string(newPod.Status.Phase)))
			os.podEventHandler.HandleEvent(UpdatePod, Informers, newPod)
		}
	}
}

// this function is called when a pod is deleted from api-server.
// when a pod is completed, the equivalent task's state will also be completed
// optionally, we run a completionHandler per workload, in order to determine
// if a application is completed along with this pod's completion
func (os *Manager) deletePod(obj interface{}) {
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
			log.Logger().Error(err.Error())
			return
		}
	default:
		log.Logger().Error("cannot convert to pod")
		return
	}

	log.Logger().Info("delete pod",
		zap.String("appType", os.Name()),
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)))

	os.podEventHandler.HandleEvent(DeletePod, Informers, pod)
}

func (os *Manager) ListPods() ([]*v1.Pod, error) {
	log.Logger().Info("Retrieving pod list")
	// list all pods on this cluster
	appPods, err := os.apiProvider.GetAPIs().PodInformer.Lister().List(labels.NewSelector())
	if err != nil {
		return nil, err
	}
	log.Logger().Info("Pod list retrieved from api server", zap.Int("nr of pods", len(appPods)))
	// get existing apps
	existingApps := make(map[string]struct{})
	podsRecovered := 0
	podsWithoutMetaData := 0
	pods := make([]*v1.Pod, 0)
	for _, pod := range appPods {
		log.Logger().Debug("Looking at pod for recovery candidates", zap.String("podNamespace", pod.Namespace), zap.String("podName", pod.Name))
		// general filter passes, and pod is assigned
		// this means the pod is already scheduled by scheduler for an existing app
		if utils.GeneralPodFilter(pod) && utils.IsAssignedPod(pod) {
			if meta, ok := getAppMetadata(pod, true); ok {
				podsRecovered++
				pods = append(pods, pod)
				log.Logger().Debug("Adding appID as recovery candidate", zap.String("appID", meta.ApplicationID))
				existingApps[meta.ApplicationID] = struct{}{}
			} else {
				podsWithoutMetaData++
			}
		}
	}
	log.Logger().Info("Application recovery statistics",
		zap.Int("nr of recoverable apps", len(existingApps)),
		zap.Int("nr of total pods", len(appPods)),
		zap.Int("nr of pods without application metadata", podsWithoutMetaData),
		zap.Int("nr of pods to be recovered", podsRecovered))

	return pods, nil
}

func (os *Manager) GetExistingAllocation(pod *v1.Pod) *si.Allocation {
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
			UUID:             string(pod.UID),
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
