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

	"github.com/apache/incubator-yunikorn-core/pkg/entrypoint"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	pluginconf "github.com/apache/incubator-yunikorn-k8shim/pkg/schedulerplugin/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/shim"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/api"
)

const (
	SchedulerPluginName = "YuniKornPlugin"
)

type YuniKornSchedulerPlugin struct {
	sync.RWMutex
	context *cache.Context
}

// ensure all required interfaces are implemented
var _ framework.PreFilterPlugin = &YuniKornSchedulerPlugin{}
var _ framework.FilterPlugin = &YuniKornSchedulerPlugin{}
var _ framework.PostBindPlugin = &YuniKornSchedulerPlugin{}

// Name returns the name of the plugin
func (sp *YuniKornSchedulerPlugin) Name() string {
	return SchedulerPluginName
}

// PreFilter is used to release pods to scheduler
func (sp *YuniKornSchedulerPlugin) PreFilter(_ context.Context, _ *framework.CycleState, pod *v1.Pod) *framework.Status {
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
				sp.context.RemovePendingPodAllocation(string(pod.UID))
				dispatcher.Dispatch(cache.NewRejectTaskEvent(app.GetApplicationID(), task.GetTaskID(),
					fmt.Sprintf("task %s rejected by scheduler", task.GetTaskID())))
				return framework.NewStatus(framework.Unschedulable, "Pod is not ready for scheduling")
			}

			nodeID, ok := sp.context.GetPendingPodAllocation(string(pod.UID))
			if !ok {
				nodeID = "<none>"
			}

			if task.GetTaskState() == events.States().Task.Bound && ok {
				log.Logger().Info("Releasing pod for scheduling (prefilter phase)",
					zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
					zap.String("taskID", task.GetTaskID()),
					zap.String("assignedNode", nodeID))

				return framework.NewStatus(framework.Success, "")
			}
		}
	}

	return framework.NewStatus(framework.Unschedulable, "Pod is not ready for scheduling")
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
			if task.GetTaskState() == events.States().Task.Bound {
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

	return framework.NewStatus(framework.Unschedulable, "Pod is not fit for node")
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
			sp.context.RemovePendingPodAllocation(string(pod.UID))
		}
	}
}

// NewSchedulerPlugin initializes a new plugin and returns it
func NewSchedulerPlugin(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	log.Logger().Info("Build info", zap.String("version", pluginconf.BuildVersion), zap.String("date", pluginconf.BuildDate))

	// start the YK core scheduler
	serviceContext := entrypoint.StartAllServices()
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
