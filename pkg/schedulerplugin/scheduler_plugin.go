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

package schedulerplugin

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/client"

	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/shim"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

const (
	SchedulerPluginName = "YuniKornPlugin"
)

// YuniKornSchedulerPlugin provides an implementation of several lifecycle methods of the Kubernetes scheduling framework:
//   https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/
//
// PreFilter: Used to notify the default scheduler that a particular pod has been marked ready for scheduling by YuniKorn
//
// Filter: Used to notify the default scheduler that a particular pod/node combination is ready to be scheduled
//
// PostBind: Used to notify YuniKorn that a pod has been scheduled successfully
//
// Pod Allocations:
//
// The YuniKorn scheduler is always running in the background, making decisions about which pods to allocate to which
// nodes. When a decision is made, that pod is marked as having a "pending" pod allocation, which means YuniKorn has
// allocated the pod, but the default scheduler (via the plugin interface) has not yet been notified.
//
// Once PreFilter() has been called for a particular pod, that allocation is marked as "in progress" meaning it has been
// communicated to the default scheduler, but has not yet been fulfilled.
//
// Finally, in PostBind(), the allocation is removed as we now know that the pod has been allocated successfully.
// If a pending or in-progress allocation is detected for a pod in PreFilter(), we remove the allocation and force the
// pod to be rescheduled, as this means the prior allocation could not be completed successfully by the default
// scheduler for some reason.
type YuniKornSchedulerPlugin struct {
	sync.RWMutex
	context *cache.Context
}

// ensure all required interfaces are implemented
var _ framework.PreFilterPlugin = &YuniKornSchedulerPlugin{}
var _ framework.FilterPlugin = &YuniKornSchedulerPlugin{}
var _ framework.PostBindPlugin = &YuniKornSchedulerPlugin{}
var _ framework.EnqueueExtensions = &YuniKornSchedulerPlugin{}

// Name returns the name of the plugin
func (sp *YuniKornSchedulerPlugin) Name() string {
	return SchedulerPluginName
}

// PreFilter is used to release pods to scheduler
func (sp *YuniKornSchedulerPlugin) PreFilter(_ context.Context, state *framework.CycleState, pod *v1.Pod) *framework.Status {
	log.Logger().Debug("PreFilter check",
		zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)))

	// we don't process pods without appID defined
	appID, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger().Info(fmt.Sprintf("Skipping pod %s/%s in the prefilter plugin because no applicationID is defined",
			pod.Namespace, pod.Name))
		return framework.NewStatus(framework.Success, "Deferring to default scheduler")
	}

	if app := sp.context.GetApplication(appID); app != nil {
		if task, err := app.GetTask(string(pod.UID)); err == nil {
			_, ok := sp.context.GetInProgressPodAllocation(string(pod.UID))
			if ok {
				// pod must have failed scheduling, reject it and return unschedulable
				log.Logger().Info("Task failed scheduling, marking as rejected",
					zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
					zap.String("taskID", task.GetTaskID()))
				sp.context.RemovePodAllocation(string(pod.UID))
				dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationID(), task.GetTaskID(),
					fmt.Sprintf("task %s rejected by scheduler", task.GetTaskID())))
				return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not ready for scheduling")
			}

			nodeID, ok := sp.context.GetPendingPodAllocation(string(pod.UID))
			if task.GetTaskState() == cache.TaskStates().Bound && ok {
				log.Logger().Info("Releasing pod for scheduling (prefilter phase)",
					zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
					zap.String("taskID", task.GetTaskID()),
					zap.String("assignedNode", nodeID))

				return framework.NewStatus(framework.Success, "")
			}
		}
	}

	return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not ready for scheduling")
}

// PreFilterExtensions is unused
func (sp *YuniKornSchedulerPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Filter is used to release specific pod/node combinations to scheduler
func (sp *YuniKornSchedulerPlugin) Filter(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	log.Logger().Debug("Filter check",
		zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
		zap.String("node", nodeInfo.Node().Name))

	// we don't process pods without appID defined
	appID, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger().Info(fmt.Sprintf("Skipping pod %s/%s in the filter plugin because no applicationID is defined",
			pod.Namespace, pod.Name))
		return framework.NewStatus(framework.Success, "Deferring to default scheduler")
	}

	if app := sp.context.GetApplication(appID); app != nil {
		if task, err := app.GetTask(string(pod.UID)); err == nil {
			if task.GetTaskState() == cache.TaskStates().Bound {
				// attempt to start a pod allocation. Filter() gets called once per {Pod,Node} candidate; we only want
				// to proceed in the case where the Node we are asked about matches the one YuniKorn has selected.
				// this check is fairly cheap (one map lookup); if we fail the check here the scheduling framework will
				// immediately call Filter() again with a different candidate Node.
				if sp.context.StartPodAllocation(string(pod.UID), nodeInfo.Node().Name) {
					log.Logger().Info("Releasing pod for scheduling (filter phase)",
						zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
						zap.String("taskID", task.GetTaskID()),
						zap.String("assignedNode", nodeInfo.Node().Name))
					return framework.NewStatus(framework.Success, "")
				}
			}
		}
	}

	return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not fit for node")
}

func (sp *YuniKornSchedulerPlugin) EventsToRegister() []framework.ClusterEvent {
	// register for all events
	return []framework.ClusterEvent{
		{Resource: framework.Pod, ActionType: framework.All},
		{Resource: framework.Node, ActionType: framework.All},
		{Resource: framework.CSINode, ActionType: framework.All},
		{Resource: framework.PersistentVolume, ActionType: framework.All},
		{Resource: framework.PersistentVolumeClaim, ActionType: framework.All},
		{Resource: framework.StorageClass, ActionType: framework.All},
	}
}

// PostBind is used to mark allocations as completed once scheduling run is finished
func (sp *YuniKornSchedulerPlugin) PostBind(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) {
	log.Logger().Debug("PostBind handler",
		zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
		zap.String("assignedNode", nodeName))

	// we don't process pods without appID defined
	appID, err := utils.GetApplicationIDFromPod(pod)
	if err != nil {
		log.Logger().Info(fmt.Sprintf("Skipping pod %s/%s in the postbind plugin because no applicationID is defined",
			pod.Namespace, pod.Name))
		return
	}

	if app := sp.context.GetApplication(appID); app != nil {
		if task, err := app.GetTask(string(pod.UID)); err == nil {
			log.Logger().Info("Pod bound successfully",
				zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
				zap.String("taskID", task.GetTaskID()),
				zap.String("assignedNode", nodeName))
			sp.context.RemovePodAllocation(string(pod.UID))
		}
	}
}

// NewSchedulerPlugin initializes a new plugin and returns it
func NewSchedulerPlugin(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	log.Logger().Info("Build info", zap.String("version", conf.BuildVersion), zap.String("date", conf.BuildDate))

	configMaps, err := client.LoadBootstrapConfigMaps(conf.GetSchedulerNamespace())
	if err != nil {
		log.Logger().Fatal("Unable to bootstrap configuration", zap.Error(err))
	}

	err = conf.UpdateConfigMaps(configMaps, true)
	if err != nil {
		log.Logger().Fatal("Unable to load initial configmaps", zap.Error(err))
	}

	// start the YK core scheduler
	serviceContext := entrypoint.StartAllServicesWithLogger(log.Logger(), log.GetZapConfigs())
	if sa, ok := serviceContext.RMProxy.(api.SchedulerAPI); ok {
		// we need our own informer factory here because the informers we get from the framework handle aren't yet initialized
		informerFactory := informers.NewSharedInformerFactory(handle.ClientSet(), 0)
		ss := shim.NewShimSchedulerForPlugin(sa, informerFactory, conf.GetSchedulerConf())
		ss.Run()

		p := &YuniKornSchedulerPlugin{
			context: ss.GetContext(),
		}
		events.SetRecorder(handle.EventRecorder())
		return p, nil
	}

	return nil, fmt.Errorf("internal error: serviceContext should implement interface api.SchedulerAPI")
}
