/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package general

import (
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/utils"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	k8sCache "k8s.io/client-go/tools/cache"
)

// generic app management service watches events from all the pods,
// it recognize apps by reading pod's spec labels, if there are proper info such as
// applicationID, queue name found, and claim it as an app or a app task,
// then report them to scheduler cache by calling am protocol
type Manager struct {
	apiProvider client.APIProvider
	amProtocol cache.ApplicationManagementProtocol
}

func New(amProtocol cache.ApplicationManagementProtocol, apiProvider client.APIProvider) *Manager {
	return &Manager{
		apiProvider: apiProvider,
		amProtocol: amProtocol,
	}
}

// this implements AppManagementService interface
func (os *Manager) Name() string {
	return "generic-app-management-service"
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
func (os *Manager) Stop() error {
	// noop
	return nil
}

func (os *Manager) getTaskMetadata(pod *v1.Pod) (cache.TaskMetadata, bool) {
	appId, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger.Debug("unable to get task by given pod", zap.Error(err))
		return cache.TaskMetadata{}, false
	}

	return cache.TaskMetadata{
		ApplicationID: appId,
		TaskID:        string(pod.UID),
		Pod:           pod,
	}, true
}

func (os *Manager) getAppMetadata(pod *v1.Pod) (cache.ApplicationMetadata, bool) {
	appId, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger.Debug("unable to get application by given pod", zap.Error(err))
		return cache.ApplicationMetadata{}, false
	}

	// tags will at least have namespace info
	// labels or annotations from the pod can be added when needed
	// user info is retrieved via service account
	tags := map[string]string{}
	if pod.Namespace == "" {
		tags["namespace"] = "default"
	} else {
		tags["namespace"] = pod.Namespace
	}
	// get the application owner (this is all that is available as far as we can find)
	user := pod.Spec.ServiceAccountName

	return cache.ApplicationMetadata{
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
		return utils.IsSchedulablePod(pod)
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
		if _, exist := os.amProtocol.GetApplication(appMeta.ApplicationID); !exist {
			os.amProtocol.AddApplication(&cache.AddApplicationRequest{
				Metadata: appMeta,
				Recovery: recovery,
			})
		}
	}

	// add task
	if taskMeta, ok := os.getTaskMetadata(pod); ok {
		if app, exist := os.amProtocol.GetApplication(taskMeta.ApplicationID); exist {
			if _, err := app.GetTask(string(pod.UID)); err != nil {
				os.amProtocol.AddTask(&cache.AddTaskRequest{
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
	// TODO
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
		if application, ok := os.amProtocol.GetApplication(taskMeta.ApplicationID); ok {
			os.amProtocol.NotifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
			os.startCompletionHandlerIfNecessary(application, pod)
		}
	}
}

func (os *Manager) startCompletionHandlerIfNecessary(app *cache.Application, pod *v1.Pod) {
	for name, value := range pod.Labels {
		if name == common.SparkLabelRole && value == common.SparkLabelRoleDriver {
			app.StartCompletionHandler(cache.CompletionHandler{
				CompleteFn: func() {
					podWatch, err := os.apiProvider.GetAPIs().
						KubeClient.GetClientSet().CoreV1().
						Pods(pod.Namespace).Watch(metaV1.ListOptions{Watch: true})
					if err != nil {
						log.Logger.Info("unable to create Watch for pod",
							zap.String("pod", pod.Name),
							zap.Error(err))
						return
					}

					for {
						select {
						case targetPod, ok := <-podWatch.ResultChan():
							if !ok {
								return
							}
							resp := targetPod.Object.(*v1.Pod)
							if resp.Status.Phase == v1.PodSucceeded && resp.UID == pod.UID {
								log.Logger.Info("spark driver completed, app completed",
									zap.String("pod", resp.Name),
									zap.String("appId", app.GetApplicationID()))
								os.amProtocol.NotifyApplicationComplete(app.GetApplicationID())
								return
							}
						}
					}
				},
			})
			return
		}
	}
}

func (os *Manager) ListApplications() (map[string]cache.ApplicationMetadata, error) {
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
	existingApps := make(map[string]cache.ApplicationMetadata)
	if appPods != nil && len(appPods) > 0 {
		for _, pod := range appPods {
			if meta, ok := os.getAppMetadata(pod); ok {
				if _, exist := existingApps[meta.ApplicationID]; !exist {
					existingApps[meta.ApplicationID] = meta
				}
			}
		}
	}

	return existingApps, nil
}
