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
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	k8sCache "k8s.io/client-go/tools/cache"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// implements interfaces#Recoverable, interfaces#AppManager
// generic app management service watches events from all the pods,
// it recognize apps by reading pod's spec labels, if there are proper info such as
// applicationID, queue name found, and claim it as an app or a app task,
// then report them to scheduler cache by calling am protocol
type Manager struct {
	apiProvider client.APIProvider
	amProtocol  interfaces.ApplicationManagementProtocol
}

func NewManager(amProtocol interfaces.ApplicationManagementProtocol, apiProvider client.APIProvider) *Manager {
	return &Manager{
		apiProvider: apiProvider,
		amProtocol:  amProtocol,
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
			AddFn:    os.addPod,
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

func (os *Manager) getTaskMetadata(pod *v1.Pod) (interfaces.TaskMetadata, bool) {
	appId, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger.Debug("unable to get task by given pod", zap.Error(err))
		return interfaces.TaskMetadata{}, false
	}

	return interfaces.TaskMetadata{
		ApplicationID: appId,
		TaskID:        string(pod.UID),
		Pod:           pod,
	}, true
}

func (os *Manager) getAppMetadata(pod *v1.Pod) (interfaces.ApplicationMetadata, bool) {
	appId, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger.Debug("unable to get application by given pod", zap.Error(err))
		return interfaces.ApplicationMetadata{}, false
	}

	// tags will at least have namespace info
	// labels or annotations from the pod can be added when needed
	// user info is retrieved via service account
	tags := map[string]string{}
	if pod.Namespace == "" {
		tags[common.AppTagNamespace] = common.DefaultAppNamespace
	} else {
		tags[common.AppTagNamespace] = pod.Namespace
	}
	// get the application owner (this is all that is available as far as we can find)
	user := pod.Spec.ServiceAccountName

	return interfaces.ApplicationMetadata{
		ApplicationID: appId,
		QueueName:     utils.GetQueueNameFromPod(pod),
		User:          user,
		Tags:          tags,
	}, true
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

func (os *Manager) addPod(obj interface{}) {
	pod, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Logger.Error("failed to add pod", zap.Error(err))
		return
	}

	recovery, err := utils.NeedRecovery(pod)
	if err != nil {
		log.Logger.Error("we can't tell to add or recover this pod",
			zap.Error(err))
		return
	}

	log.Logger.Debug("pod added",
		zap.String("appType", os.Name()),
		zap.String("Name", pod.Name),
		zap.String("Namespace", pod.Namespace),
		zap.Bool("NeedsRecovery", recovery))

	// add app
	if appMeta, ok := os.getAppMetadata(pod); ok {
		// check if app already exist
		if app := os.amProtocol.GetApplication(appMeta.ApplicationID); app == nil {
			os.amProtocol.AddApplication(&interfaces.AddApplicationRequest{
				Metadata: appMeta,
				Recovery: recovery,
			})
		}
	}

	// add task
	if taskMeta, ok := os.getTaskMetadata(pod); ok {
		if app := os.amProtocol.GetApplication(taskMeta.ApplicationID); app != nil {
			if _, taskErr := app.GetTask(string(pod.UID)); taskErr != nil {
				os.amProtocol.AddTask(&interfaces.AddTaskRequest{
					Metadata: taskMeta,
					Recovery: recovery,
				})
			}
		}
	}
}

// when pod resource is modified, we need to act accordingly
// e.g vertical scale out the pod, this requires the scheduler to be aware of this
func (os *Manager) updatePod(old, new interface{}) {
	oldPod, err := utils.Convert2Pod(old)
	if err != nil {
		log.Logger.Error("expecting a pod object", zap.Error(err))
		return
	}

	newPod, err := utils.Convert2Pod(new)
	if err != nil {
		log.Logger.Error("expecting a pod object", zap.Error(err))
		return
	}

	// triggered when pod status' phase changes
	if oldPod.Status.Phase != newPod.Status.Phase {
		// pod succeed or failed means all containers in the pod have been terminated,
		// and these container won't be restarted. In this case, we can safely release
		// the resources for this allocation. And mark the task is done.
		if utils.IsPodTerminated(newPod) {
			log.Logger.Info("task completes",
				zap.String("appType", os.Name()),
				zap.String("namespace", newPod.Namespace),
				zap.String("podName", newPod.Name),
				zap.String("podUID", string(newPod.UID)),
				zap.String("podStatus", string(newPod.Status.Phase)))
			if taskMeta, ok := os.getTaskMetadata(newPod); ok {
				if app := os.amProtocol.GetApplication(taskMeta.ApplicationID); app != nil {
					os.amProtocol.NotifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
				}
			}
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
			log.Logger.Error(err.Error())
			return
		}
	default:
		log.Logger.Error("cannot convert to pod")
		return
	}

	log.Logger.Info("delete pod",
		zap.String("appType", os.Name()),
		zap.String("namespace", pod.Namespace),
		zap.String("podName", pod.Name),
		zap.String("podUID", string(pod.UID)))

	if taskMeta, ok := os.getTaskMetadata(pod); ok {
		if app := os.amProtocol.GetApplication(taskMeta.ApplicationID); app != nil {
			os.amProtocol.NotifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
		}
	}
}

func (os *Manager) ListApplications() (map[string]interfaces.ApplicationMetadata, error) {
	// selector: applicationID exist
	slt := labels.NewSelector()
	req, err := labels.NewRequirement(common.LabelApplicationID, selection.Exists, nil)
	if err != nil {
		return nil, err
	}
	slt = slt.Add(*req)

	// list all pods on this cluster
	appPods, err := os.apiProvider.GetAPIs().PodInformer.Lister().List(slt)
	if err != nil {
		return nil, err
	}

	// get existing apps
	existingApps := make(map[string]interfaces.ApplicationMetadata)
	if appPods != nil && len(appPods) > 0 {
		for _, pod := range appPods {
			// general filter passes, and pod is assigned
			// this means the pod is already scheduled by scheduler for an existing app
			if utils.GeneralPodFilter(pod) && utils.IsAssignedPod(pod) {
				if meta, ok := os.getAppMetadata(pod); ok {
					if _, exist := existingApps[meta.ApplicationID]; !exist {
						existingApps[meta.ApplicationID] = meta
					}
				}
			}
		}
	}

	return existingApps, nil
}

func (os *Manager) GetExistingAllocation(pod *v1.Pod) *si.Allocation {
	if meta, valid := os.getAppMetadata(pod); valid {
		// when submit a task, we use pod UID as the allocationKey,
		// to keep consistent, during recovery, the pod UID is also used
		// for an Allocation.
		return &si.Allocation{
			AllocationKey:    string(pod.UID),
			AllocationTags:   meta.Tags,
			UUID:             string(pod.UID),
			ResourcePerAlloc: common.GetPodResource(pod),
			QueueName:        meta.QueueName,
			NodeID:           pod.Spec.NodeName,
			ApplicationID:    meta.ApplicationID,
			PartitionName:    common.DefaultPartition,
		}
	}
	return nil
}
