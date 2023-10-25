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

package plugin

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/shim"
)

const (
	SchedulerPluginName = "YuniKornPlugin"
)

// YuniKornSchedulerPlugin provides an implementation of several lifecycle methods of the Kubernetes scheduling framework:
//
//	https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/
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
var _ framework.PreEnqueuePlugin = &YuniKornSchedulerPlugin{}
var _ framework.PreFilterPlugin = &YuniKornSchedulerPlugin{}
var _ framework.FilterPlugin = &YuniKornSchedulerPlugin{}
var _ framework.PostBindPlugin = &YuniKornSchedulerPlugin{}
var _ framework.EnqueueExtensions = &YuniKornSchedulerPlugin{}

// Name returns the name of the plugin
func (sp *YuniKornSchedulerPlugin) Name() string {
	return SchedulerPluginName
}

// PreEnqueue is called prior to adding Pods to activeQ
func (sp *YuniKornSchedulerPlugin) PreEnqueue(_ context.Context, pod *v1.Pod) *framework.Status {
	log.Log(log.ShimSchedulerPlugin).Debug("PreEnqueue check",
		zap.String("namespace", pod.Namespace),
		zap.String("pod", pod.Name))

	// we don't process pods without appID defined
	appID := utils.GetApplicationIDFromPod(pod)
	if appID == "" {
		log.Log(log.ShimSchedulerPlugin).Debug("Releasing non-managed Pod for scheduling (PreEnqueue phase)",
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name))
		return nil
	}

	taskID := string(pod.UID)
	if app, task, ok := sp.getTask(appID, taskID); ok {
		if _, ok := sp.context.GetInProgressPodAllocation(taskID); ok {
			// pod must have failed scheduling in a prior run, reject it and return unschedulable
			sp.failTask(pod, app, task)
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not ready for scheduling")
		}

		nodeID, ok := sp.context.GetPendingPodAllocation(taskID)
		if task.GetTaskState() == cache.TaskStates().Bound && ok {
			log.Log(log.ShimSchedulerPlugin).Info("Releasing pod for scheduling (PreEnqueue phase)",
				zap.String("namespace", pod.Namespace),
				zap.String("pod", pod.Name),
				zap.String("taskID", taskID),
				zap.String("assignedNode", nodeID))
			return nil
		}

		schedState := task.GetTaskSchedulingState()
		switch schedState {
		case interfaces.TaskSchedPending:
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is pending scheduling")
		case interfaces.TaskSchedFailed:
			// allow the pod to proceed so that it will be marked unschedulable by PreFilter
			return nil
		case interfaces.TaskSchedSkipped:
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod doesn't fit within queue")
		default:
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("Pod unschedulable: %s", schedState.String()))
		}
	}

	// task not found (yet?) -- probably means cache update hasn't come through yet
	return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod not ready for scheduling")
}

// PreFilter is used to release pods to scheduler
func (sp *YuniKornSchedulerPlugin) PreFilter(_ context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	log.Log(log.ShimSchedulerPlugin).Debug("PreFilter check",
		zap.String("namespace", pod.Namespace),
		zap.String("pod", pod.Name))

	// we don't process pods without appID defined
	appID := utils.GetApplicationIDFromPod(pod)
	if appID == "" {
		log.Log(log.ShimSchedulerPlugin).Debug("Releasing non-managed Pod for scheduling (PreFilter phase)",
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name))

		return nil, framework.NewStatus(framework.Skip)
	}

	taskID := string(pod.UID)
	if app, task, ok := sp.getTask(appID, taskID); ok {
		if _, ok := sp.context.GetInProgressPodAllocation(taskID); ok {
			// pod must have failed scheduling, reject it and return unschedulable
			sp.failTask(pod, app, task)
			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not ready for scheduling")
		}

		nodeID, ok := sp.context.GetPendingPodAllocation(taskID)
		if task.GetTaskState() == cache.TaskStates().Bound && ok {
			log.Log(log.ShimSchedulerPlugin).Info("Releasing pod for scheduling (PreFilter phase)",
				zap.String("namespace", pod.Namespace),
				zap.String("pod", pod.Name),
				zap.String("taskID", taskID),
				zap.String("assignedNode", nodeID))
			return &framework.PreFilterResult{NodeNames: sets.NewString(nodeID)}, nil
		}
	}

	return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not ready for scheduling")
}

// PreFilterExtensions is unused
func (sp *YuniKornSchedulerPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Filter is used to release specific pod/node combinations to scheduler
func (sp *YuniKornSchedulerPlugin) Filter(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	log.Log(log.ShimSchedulerPlugin).Debug("Filter check",
		zap.String("namespace", pod.Namespace),
		zap.String("pod", pod.Name),
		zap.String("node", nodeInfo.Node().Name))

	// we don't process pods without appID defined
	appID := utils.GetApplicationIDFromPod(pod)
	if appID == "" {
		log.Log(log.ShimSchedulerPlugin).Debug("Releasing non-managed Pod fo scheduling (Filter phase)",
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name))
		return nil
	}

	taskID := string(pod.UID)
	if _, task, ok := sp.getTask(appID, taskID); ok {
		if task.GetTaskState() == cache.TaskStates().Bound {
			// attempt to start a pod allocation. Filter() gets called once per {Pod,Node} candidate; we only want
			// to proceed in the case where the Node we are asked about matches the one YuniKorn has selected.
			// this check is fairly cheap (one map lookup); if we fail the check here the scheduling framework will
			// immediately call Filter() again with a different candidate Node.
			if sp.context.StartPodAllocation(taskID, nodeInfo.Node().Name) {
				log.Log(log.ShimSchedulerPlugin).Info("Releasing pod for scheduling (Filter phase)",
					zap.String("namespace", pod.Namespace),
					zap.String("pod", pod.Name),
					zap.String("taskID", taskID),
					zap.String("assignedNode", nodeInfo.Node().Name))
				return nil
			}
		}
	}

	return framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod is not fit for node")
}

func (sp *YuniKornSchedulerPlugin) EventsToRegister() []framework.ClusterEvent {
	return sp.context.EventsToRegister()
}

// PostBind is used to mark allocations as completed once scheduling run is finished
func (sp *YuniKornSchedulerPlugin) PostBind(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) {
	log.Log(log.ShimSchedulerPlugin).Debug("PostBind handler",
		zap.String("namespace", pod.Namespace),
		zap.String("pod", pod.Name),
		zap.String("assignedNode", nodeName))

	// we don't process pods without appID defined
	appID := utils.GetApplicationIDFromPod(pod)
	if appID == "" {
		log.Log(log.ShimSchedulerPlugin).Debug("Non-managed Pod bound successfully",
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name))
		return
	}

	taskID := string(pod.UID)
	if _, _, ok := sp.getTask(appID, taskID); ok {
		log.Log(log.ShimSchedulerPlugin).Info("Managed Pod bound successfully",
			zap.String("namespace", pod.Namespace),
			zap.String("pod", pod.Name),
			zap.String("taskID", taskID),
			zap.String("assignedNode", nodeName))
		sp.context.RemovePodAllocation(taskID)
	}
}

// NewSchedulerPlugin initializes a new plugin and returns it
func NewSchedulerPlugin(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	log.Log(log.ShimSchedulerPlugin).Info(conf.GetBuildInfoString())

	configMaps, err := client.LoadBootstrapConfigMaps()
	if err != nil {
		log.Log(log.ShimSchedulerPlugin).Fatal("Unable to bootstrap configuration", zap.Error(err))
	}

	err = conf.UpdateConfigMaps(configMaps, true)
	if err != nil {
		log.Log(log.ShimSchedulerPlugin).Fatal("Unable to load initial configmaps", zap.Error(err))
	}

	// start the YK core scheduler
	serviceContext := entrypoint.StartAllServicesWithLogger(log.RootLogger(), log.GetZapConfigs())
	if serviceContext.RMProxy == nil {
		return nil, fmt.Errorf("internal error: serviceContext should implement interface api.SchedulerAPI")
	}

	// we need our own informer factory here because the informers we get from the framework handle aren't yet initialized
	informerFactory := informers.NewSharedInformerFactory(handle.ClientSet(), 0)
	ss := shim.NewShimSchedulerForPlugin(serviceContext.RMProxy, informerFactory, conf.GetSchedulerConf(), configMaps)
	if err := ss.Run(); err != nil {
		log.Log(log.ShimSchedulerPlugin).Fatal("Unable to start scheduler", zap.Error(err))
	}

	p := &YuniKornSchedulerPlugin{
		context: ss.GetContext(),
	}
	events.SetRecorder(handle.EventRecorder())
	return p, nil
}

func (sp *YuniKornSchedulerPlugin) getTask(appID, taskID string) (app interfaces.ManagedApp, task interfaces.ManagedTask, ok bool) {
	if app := sp.context.GetApplication(appID); app != nil {
		if task, err := app.GetTask(taskID); err == nil {
			return app, task, true
		}
	}
	return nil, nil, false
}

func (sp *YuniKornSchedulerPlugin) failTask(pod *v1.Pod, app interfaces.ManagedApp, task interfaces.ManagedTask) {
	taskID := task.GetTaskID()
	log.Log(log.ShimSchedulerPlugin).Info("Task failed scheduling, marking as rejected",
		zap.String("namespace", pod.Namespace),
		zap.String("pod", pod.Name),
		zap.String("taskID", taskID))
	sp.context.RemovePodAllocation(taskID)
	dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationID(), taskID, fmt.Sprintf("task %s rejected by scheduler", taskID)))
	task.SetTaskSchedulingState(interfaces.TaskSchedFailed)
}
