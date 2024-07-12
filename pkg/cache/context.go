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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	schedulercache "github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	schedulerconf "github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/predicates"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/support"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const registerNodeContextHandler = "RegisterNodeContextHandler"

var (
	ErrorPodNotFound  = errors.New("predicates were not run because pod was not found in cache")
	ErrorNodeNotFound = errors.New("predicates were not run because node was not found in cache")
)

// context maintains scheduling state, like apps and apps' tasks.
type Context struct {
	applications   map[string]*Application        // apps
	schedulerCache *schedulercache.SchedulerCache // external cache
	apiProvider    client.APIProvider             // apis to interact with api-server, scheduler-core, etc
	predManager    predicates.PredicateManager    // K8s predicates
	pluginMode     bool                           // true if we are configured as a scheduler plugin
	namespace      string                         // yunikorn namespace
	configMaps     []*v1.ConfigMap                // cached yunikorn configmaps
	lock           *locking.RWMutex               // lock
	txnID          atomic.Uint64                  // transaction ID counter
	klogger        klog.Logger
}

// NewContext create a new context for the scheduler using a default (empty) configuration
// VisibleForTesting
func NewContext(apis client.APIProvider) *Context {
	return NewContextWithBootstrapConfigMaps(apis, []*v1.ConfigMap{nil, nil})
}

// NewContextWithBootstrapConfigMaps creates a new context for the scheduler using configuration bootstrapped from Kubernetes ConfigMaps
func NewContextWithBootstrapConfigMaps(apis client.APIProvider, bootstrapConfigMaps []*v1.ConfigMap) *Context {
	// create the context note that order is important:
	// volumebinder needs the informers
	// the cache needs informers and volumebinder
	// nodecontroller needs the cache
	// predictor need the cache, volumebinder and informers
	ctx := &Context{
		applications: make(map[string]*Application),
		apiProvider:  apis,
		namespace:    schedulerconf.GetSchedulerConf().Namespace,
		configMaps:   bootstrapConfigMaps,
		lock:         &locking.RWMutex{},
		klogger:      klog.NewKlogr(),
	}

	// create the cache
	ctx.schedulerCache = schedulercache.NewSchedulerCache(apis.GetAPIs())

	// create the predicate manager
	sharedLister := support.NewSharedLister(ctx.schedulerCache)
	clientSet := apis.GetAPIs().KubeClient.GetClientSet()
	informerFactory := apis.GetAPIs().InformerFactory
	ctx.predManager = predicates.NewPredicateManager(support.NewFrameworkHandle(sharedLister, informerFactory, clientSet))

	return ctx
}

func (ctx *Context) AddSchedulingEventHandlers() error {
	err := ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.ConfigMapInformerHandlers,
		FilterFn: ctx.filterConfigMaps,
		AddFn:    ctx.addConfigMaps,
		UpdateFn: ctx.updateConfigMaps,
		DeleteFn: ctx.deleteConfigMaps,
	})
	if err != nil {
		return err
	}

	err = ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.PriorityClassInformerHandlers,
		FilterFn: ctx.filterPriorityClasses,
		AddFn:    ctx.addPriorityClass,
		UpdateFn: ctx.updatePriorityClass,
		DeleteFn: ctx.deletePriorityClass,
	})
	if err != nil {
		return err
	}

	err = ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.NodeInformerHandlers,
		AddFn:    ctx.addNode,
		UpdateFn: ctx.updateNode,
		DeleteFn: ctx.deleteNode,
	})
	if err != nil {
		return err
	}

	err = ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.PodInformerHandlers,
		AddFn:    ctx.AddPod,
		UpdateFn: ctx.UpdatePod,
		DeleteFn: ctx.DeletePod,
	})
	if err != nil {
		return err
	}

	return nil
}

func (ctx *Context) IsPluginMode() bool {
	return ctx.pluginMode
}

func (ctx *Context) addNode(obj interface{}) {
	ctx.updateNode(nil, obj)
}

func (ctx *Context) updateNode(_, obj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	node, err := convertToNode(obj)
	if err != nil {
		log.Log(log.ShimContext).Error("node conversion failed", zap.Error(err))
		return
	}
	ctx.updateNodeInternal(node, true)
}

func (ctx *Context) updateNodeInternal(node *v1.Node, register bool) {
	// update scheduler cache
	if prevNode, adoptedPods := ctx.schedulerCache.UpdateNode(node); prevNode == nil {
		// newly added node

		// if requested, register this node with the scheduler core. this is optional to allow for bulk registration
		// during scheduler initialization.
		if register {
			if err := ctx.registerNode(node); err != nil {
				// remove from secondary cache and return
				log.Log(log.ShimContext).Error("node registration failed", zap.Error(err))
				ctx.schedulerCache.RemoveNode(node)
				return
			}
		}

		// iterate newly adopted pods and register them with the scheduler
		for _, pod := range adoptedPods {
			log.Log(log.ShimContext).Info("Adopting previously orphaned pod",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("nodeName", node.Name))
			applicationID := utils.GetApplicationIDFromPod(pod)
			if applicationID == "" {
				ctx.updateForeignPod(pod)
			} else {
				ctx.updateYuniKornPod(pod)
			}
		}

		// if node was registered in-line, enable it in the core
		if err := ctx.enableNode(node); err != nil {
			log.Log(log.ShimContext).Warn("Failed to enable node", zap.Error(err))
		}
	} else {
		// existing node
		prevCapacity := common.GetNodeResource(&prevNode.Status)
		newCapacity := common.GetNodeResource(&node.Status)

		if !common.Equals(prevCapacity, newCapacity) {
			// update capacity
			if capacity, occupied, ok := ctx.schedulerCache.UpdateCapacity(node.Name, newCapacity); ok {
				if err := ctx.updateNodeResources(node, capacity, occupied); err != nil {
					log.Log(log.ShimContext).Warn("Failed to update node capacity", zap.Error(err))
				}
			} else {
				log.Log(log.ShimContext).Warn("Failed to update cached node capacity", zap.String("nodeName", node.Name))
			}
		}
	}
}

func (ctx *Context) deleteNode(obj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			log.Log(log.ShimContext).Error("cannot convert to *v1.Node", zap.Any("object", t.Obj))
			return
		}
	default:
		log.Log(log.ShimContext).Error("cannot convert to *v1.Node", zap.Any("object", t))
		return
	}
	ctx.deleteNodeInternal(node)
}

func (ctx *Context) addNodesWithoutRegistering(nodes []*v1.Node) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	for _, node := range nodes {
		ctx.updateNodeInternal(node, false)
	}
}

func (ctx *Context) deleteNodeInternal(node *v1.Node) {
	// remove node from scheduler cache
	prevNode, orphanedPods := ctx.schedulerCache.RemoveNode(node)
	if prevNode == nil {
		// nothing to do if node wasn't there
		return
	}

	// log the number of orphaned pods, but we shouldn't need to do any processing of them as the core will send
	// back remove events for each of them
	log.Log(log.ShimContext).Info("Removing node",
		zap.String("nodeName", node.Name),
		zap.Int("assignedPods", len(orphanedPods)))

	// decommission node
	log.Log(log.ShimContext).Info("Decommissioning node", zap.String("nodeName", node.Name))
	if err := ctx.decommissionNode(node); err != nil {
		log.Log(log.ShimContext).Warn("Unable to decommission node", zap.Error(err))
	}

	// post the event
	events.GetRecorder().Eventf(node.DeepCopy(), nil, v1.EventTypeNormal, "NodeDeleted", "NodeDeleted",
		fmt.Sprintf("node %s is deleted from the scheduler", node.Name))
}

func (ctx *Context) AddPod(obj interface{}) {
	ctx.UpdatePod(nil, obj)
}

func (ctx *Context) UpdatePod(_, newObj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	pod, err := utils.Convert2Pod(newObj)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to update pod", zap.Error(err))
		return
	}
	if utils.GetApplicationIDFromPod(pod) == "" {
		ctx.updateForeignPod(pod)
	} else {
		ctx.updateYuniKornPod(pod)
	}
}

func (ctx *Context) updateYuniKornPod(pod *v1.Pod) {
	// treat terminated pods like a remove
	if utils.IsPodTerminated(pod) {
		if taskMeta, ok := getTaskMetadata(pod); ok {
			if app := ctx.getApplication(taskMeta.ApplicationID); app != nil {
				ctx.notifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
			}
		}

		log.Log(log.ShimContext).Debug("Request to update terminated pod, removing from cache", zap.String("podName", pod.Name))
		ctx.schedulerCache.RemovePod(pod)
		return
	}

	if ctx.schedulerCache.UpdatePod(pod) {
		// pod was accepted; ensure the application and task objects have been created
		ctx.ensureAppAndTaskCreated(pod)
	}
}

func (ctx *Context) ensureAppAndTaskCreated(pod *v1.Pod) {
	// get app metadata
	appMeta, ok := getAppMetadata(pod)
	if !ok {
		log.Log(log.ShimContext).Warn("BUG: Unable to retrieve application metadata from YuniKorn-managed Pod",
			zap.String("namespace", pod.Namespace),
			zap.String("name", pod.Name))
		return
	}

	// add app if it doesn't already exist
	app := ctx.getApplication(appMeta.ApplicationID)
	if app == nil {
		app = ctx.addApplication(&AddApplicationRequest{
			Metadata: appMeta,
		})
	}

	// get task metadata
	taskMeta, ok := getTaskMetadata(pod)
	if !ok {
		log.Log(log.ShimContext).Warn("BUG: Unable to retrieve task metadata from YuniKorn-managed Pod",
			zap.String("namespace", pod.Namespace),
			zap.String("name", pod.Name))
		return
	}

	// add task if it doesn't already exist
	if _, taskErr := app.GetTask(string(pod.UID)); taskErr != nil {
		ctx.addTask(&AddTaskRequest{
			Metadata: taskMeta,
		})
	}
}

func (ctx *Context) updateForeignPod(pod *v1.Pod) {
	podStatusBefore := ""
	oldPod := ctx.schedulerCache.GetPod(string(pod.UID))
	if oldPod != nil {
		podStatusBefore = string(oldPod.Status.Phase)
	}

	// conditions for allocate:
	//   1. pod was previously assigned
	//   2. pod is now assigned
	//   3. pod is not in terminated state
	//   4. pod references a known node
	if oldPod == nil && utils.IsAssignedPod(pod) && !utils.IsPodTerminated(pod) {
		if ctx.schedulerCache.UpdatePod(pod) {
			// pod was accepted by a real node
			log.Log(log.ShimContext).Debug("pod is assigned to a node, trigger occupied resource update",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("podStatusBefore", podStatusBefore),
				zap.String("podStatusCurrent", string(pod.Status.Phase)))
			ctx.updateNodeOccupiedResources(pod.Spec.NodeName, pod.Namespace, pod.Name, common.GetPodResource(pod), schedulercache.AddOccupiedResource)
		} else {
			// pod is orphaned (references an unknown node)
			log.Log(log.ShimContext).Info("skipping occupied resource update for assigned orphaned pod",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("nodeName", pod.Spec.NodeName))
		}
		return
	}

	// conditions for release:
	//   1. pod was previously assigned
	//   2. pod is now in a terminated state
	//   3. pod references a known node
	if oldPod != nil && utils.IsPodTerminated(pod) {
		if !ctx.schedulerCache.IsPodOrphaned(string(pod.UID)) {
			log.Log(log.ShimContext).Debug("pod terminated, trigger occupied resource update",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("podStatusBefore", podStatusBefore),
				zap.String("podStatusCurrent", string(pod.Status.Phase)))
			// this means pod is terminated
			// we need sub the occupied resource and re-sync with the scheduler-core
			ctx.updateNodeOccupiedResources(pod.Spec.NodeName, pod.Namespace, pod.Name, common.GetPodResource(pod), schedulercache.SubOccupiedResource)
			ctx.schedulerCache.RemovePod(pod)
		} else {
			// pod is orphaned (references an unknown node)
			log.Log(log.ShimContext).Info("skipping occupied resource update for terminated orphaned pod",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("nodeName", pod.Spec.NodeName))
		}
		return
	}
}

func (ctx *Context) DeletePod(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			log.Log(log.ShimContext).Error("Cannot convert to *v1.Pod", zap.Any("pod", obj))
			return
		}
	default:
		log.Log(log.ShimContext).Error("Cannot convert to *v1.Pod", zap.Any("pod", obj))
		return
	}

	if utils.GetApplicationIDFromPod(pod) == "" {
		ctx.deleteForeignPod(pod)
	} else {
		ctx.deleteYuniKornPod(pod)
	}
}

func (ctx *Context) deleteYuniKornPod(pod *v1.Pod) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if taskMeta, ok := getTaskMetadata(pod); ok {
		if app := ctx.getApplication(taskMeta.ApplicationID); app != nil {
			ctx.notifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
		}
	}

	log.Log(log.ShimContext).Debug("removing pod from cache", zap.String("podName", pod.Name))
	ctx.schedulerCache.RemovePod(pod)
}

func (ctx *Context) deleteForeignPod(pod *v1.Pod) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	oldPod := ctx.schedulerCache.GetPod(string(pod.UID))
	if oldPod == nil {
		// if pod is not in scheduler cache, no node updates are needed
		log.Log(log.ShimContext).Debug("unknown foreign pod deleted, no resource updated needed",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name))
		return
	}

	// conditions for release:
	//   1. pod is already assigned to a node
	//   2. pod was not in a terminal state before
	//   3. pod references a known node
	if !utils.IsPodTerminated(oldPod) {
		if !ctx.schedulerCache.IsPodOrphaned(string(oldPod.UID)) {
			log.Log(log.ShimContext).Debug("foreign pod deleted, triggering occupied resource update",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("podStatusBefore", string(oldPod.Status.Phase)),
				zap.String("podStatusCurrent", string(pod.Status.Phase)))
			// this means pod is terminated
			// we need sub the occupied resource and re-sync with the scheduler-core
			ctx.updateNodeOccupiedResources(pod.Spec.NodeName, pod.Namespace, pod.Name, common.GetPodResource(pod), schedulercache.SubOccupiedResource)
		} else {
			// pod is orphaned (references an unknown node)
			log.Log(log.ShimContext).Info("skipping occupied resource update for removed orphaned pod",
				zap.String("namespace", pod.Namespace),
				zap.String("podName", pod.Name),
				zap.String("nodeName", pod.Spec.NodeName))
		}
		ctx.schedulerCache.RemovePod(pod)
	}
}

func (ctx *Context) updateNodeOccupiedResources(nodeName string, namespace string, podName string, resource *si.Resource, opt schedulercache.UpdateType) {
	if common.IsZero(resource) {
		return
	}
	if node, capacity, occupied, ok := ctx.schedulerCache.UpdateOccupiedResource(nodeName, namespace, podName, resource, opt); ok {
		if err := ctx.updateNodeResources(node, capacity, occupied); err != nil {
			log.Log(log.ShimContext).Warn("scheduler rejected update to node occupied resources", zap.Error(err))
		}
	} else {
		log.Log(log.ShimContext).Warn("unable to update occupied resources for node", zap.String("nodeName", nodeName))
	}
}

// filter configMap for the scheduler
func (ctx *Context) filterConfigMaps(obj interface{}) bool {
	switch obj := obj.(type) {
	case *v1.ConfigMap:
		return (obj.Name == constants.DefaultConfigMapName || obj.Name == constants.ConfigMapName) && obj.Namespace == ctx.namespace
	case cache.DeletedFinalStateUnknown:
		return ctx.filterConfigMaps(obj.Obj)
	default:
		return false
	}
}

// when the configMap for the scheduler is added, trigger hot-refresh
func (ctx *Context) addConfigMaps(obj interface{}) {
	log.Log(log.ShimContext).Debug("configMap added")
	configmap := utils.Convert2ConfigMap(obj)
	switch configmap.Name {
	case constants.DefaultConfigMapName:
		ctx.triggerReloadConfig(0, configmap)
	case constants.ConfigMapName:
		ctx.triggerReloadConfig(1, configmap)
	default:
		// ignore
		return
	}
}

// when the configMap for the scheduler is updated, trigger hot-refresh
func (ctx *Context) updateConfigMaps(_, newObj interface{}) {
	log.Log(log.ShimContext).Debug("configMap updated")
	configmap := utils.Convert2ConfigMap(newObj)
	switch configmap.Name {
	case constants.DefaultConfigMapName:
		ctx.triggerReloadConfig(0, configmap)
	case constants.ConfigMapName:
		ctx.triggerReloadConfig(1, configmap)
	default:
		// ignore
		return
	}
}

// when the configMap for the scheduler is deleted, trigger refresh using default config
func (ctx *Context) deleteConfigMaps(obj interface{}) {
	log.Log(log.ShimContext).Debug("configMap deleted")
	var configmap *v1.ConfigMap = nil
	switch t := obj.(type) {
	case *v1.ConfigMap:
		configmap = t
	case cache.DeletedFinalStateUnknown:
		configmap = utils.Convert2ConfigMap(obj)
	default:
		log.Log(log.ShimContext).Warn("unable to convert to configmap")
		return
	}

	switch configmap.Name {
	case constants.DefaultConfigMapName:
		ctx.triggerReloadConfig(0, nil)
	case constants.ConfigMapName:
		ctx.triggerReloadConfig(1, nil)
	default:
		// ignore
		return
	}
}

func (ctx *Context) filterPriorityClasses(obj interface{}) bool {
	switch obj := obj.(type) {
	case *schedulingv1.PriorityClass:
		return true
	case cache.DeletedFinalStateUnknown:
		return ctx.filterPriorityClasses(obj.Obj)
	default:
		return false
	}
}

func (ctx *Context) addPriorityClass(obj interface{}) {
	ctx.updatePriorityClass(nil, obj)
}

func (ctx *Context) updatePriorityClass(_, newObj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if priorityClass := utils.Convert2PriorityClass(newObj); priorityClass != nil {
		ctx.updatePriorityClassInternal(priorityClass)
	}
}

func (ctx *Context) updatePriorityClassInternal(priorityClass *schedulingv1.PriorityClass) {
	ctx.schedulerCache.UpdatePriorityClass(priorityClass)
}

func (ctx *Context) deletePriorityClass(obj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	log.Log(log.ShimContext).Debug("priorityClass deleted")
	var priorityClass *schedulingv1.PriorityClass
	switch t := obj.(type) {
	case *schedulingv1.PriorityClass:
		priorityClass = t
	case cache.DeletedFinalStateUnknown:
		priorityClass = utils.Convert2PriorityClass(obj)
	default:
		log.Log(log.ShimContext).Warn("unable to convert to priorityClass")
		return
	}
	if priorityClass != nil {
		ctx.schedulerCache.RemovePriorityClass(priorityClass)
	}
}

func (ctx *Context) triggerReloadConfig(index int, configMap *v1.ConfigMap) {
	// hot reload is turned off do nothing
	// hot reload can be turned off by an update: safety first access under lock to prevent data race
	if !schedulerconf.GetSchedulerConf().IsConfigReloadable() {
		log.Log(log.ShimContext).Info("hot-refresh disabled, skipping scheduler configuration update")
		return
	}
	// update the maps in the context: return on failure, logged in the called method
	confMap := ctx.setConfigMap(index, configMap)
	if confMap == nil {
		return
	}
	log.Log(log.ShimContext).Info("reloading scheduler configuration")
	config := utils.GetCoreSchedulerConfigFromConfigMap(confMap)
	extraConfig := utils.GetExtraConfigFromConfigMap(confMap)

	request := &si.UpdateConfigurationRequest{
		RmID:        schedulerconf.GetSchedulerConf().ClusterID,
		PolicyGroup: schedulerconf.GetSchedulerConf().PolicyGroup,
		Config:      config,
		ExtraConfig: extraConfig,
	}
	// tell the core to update: sync call that is serialised on the core side
	if err := ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateConfiguration(request); err != nil {
		log.Log(log.ShimContext).Error("reload configuration failed", zap.Error(err))
	}
}

// setConfigMap sets the new config map object in the list of maps maintained in the context and returns a flat map
// of the settings from both maps
func (ctx *Context) setConfigMap(index int, configMap *v1.ConfigMap) map[string]string {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.configMaps[index] = configMap
	err := schedulerconf.UpdateConfigMaps(ctx.configMaps, false)
	if err != nil {
		log.Log(log.ShimContext).Error("Unable to update configmap, ignoring changes", zap.Error(err))
		return nil
	}
	return schedulerconf.FlattenConfigMaps(ctx.configMaps)
}

// EventsToRegister returns the Kubernetes events that should be watched for updates which may effect predicate processing
func (ctx *Context) EventsToRegister(queueingHintFn framework.QueueingHintFn) []framework.ClusterEventWithHint {
	return ctx.predManager.EventsToRegister(queueingHintFn)
}

// IsPodFitNode evaluates given predicates based on current context
func (ctx *Context) IsPodFitNode(name, node string, allocate bool) error {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	pod := ctx.schedulerCache.GetPod(name)
	if pod == nil {
		return ErrorPodNotFound
	}
	// if pod exists in cache, try to run predicates
	targetNode := ctx.schedulerCache.GetNode(node)
	if targetNode == nil {
		return ErrorNodeNotFound
	}
	// need to lock cache here as predicates need a stable view into the cache
	ctx.schedulerCache.LockForReads()
	defer ctx.schedulerCache.UnlockForReads()
	plugin, err := ctx.predManager.Predicates(pod, targetNode, allocate)
	if err != nil {
		err = errors.Join(fmt.Errorf("failed plugin: '%s'", plugin), err)
	}
	return err
}

func (ctx *Context) IsPodFitNodeViaPreemption(name, node string, allocations []string, startIndex int) (int, bool) {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	if pod := ctx.schedulerCache.GetPod(name); pod != nil {
		// if pod exists in cache, try to run predicates
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			// need to lock cache here as predicates need a stable view into the cache
			ctx.schedulerCache.LockForReads()
			defer ctx.schedulerCache.UnlockForReads()

			// look up each victim in the scheduler cache
			victims := make([]*v1.Pod, len(allocations))
			for index, uid := range allocations {
				victim := ctx.schedulerCache.GetPodNoLock(uid)
				victims[index] = victim
			}

			// check predicates for a match
			if index := ctx.predManager.PreemptionPredicates(pod, targetNode, victims, startIndex); index != -1 {
				return index, true
			}
		}
	}
	return -1, false
}

// call volume binder to bind pod volumes if necessary,
// internally, volume binder maintains a cache (podBindingCache) for pod volumes,
// and before calling this, they should have been updated by FindPodVolumes and AssumePodVolumes.
func (ctx *Context) bindPodVolumes(pod *v1.Pod) error {
	podKey := string(pod.UID)
	// the assumePodVolumes was done in scheduler-core, because these assumed pods are cached
	// during scheduling process as they have directly impact to other scheduling processes.
	// when assumePodVolumes was called, we caches the value if all pod volumes are bound in schedulerCache,
	// then here we just need to retrieve that value from cache, to skip bindings if volumes are already bound.
	if assumedPod := ctx.schedulerCache.GetPod(podKey); assumedPod != nil {
		if ctx.schedulerCache.ArePodVolumesAllBound(podKey) {
			log.Log(log.ShimContext).Info("Binding Pod Volumes skipped: all volumes already bound",
				zap.String("podName", pod.Name))
		} else {
			log.Log(log.ShimContext).Info("Binding Pod Volumes", zap.String("podName", pod.Name))

			// retrieve the volume claims
			podVolumeClaims, err := ctx.apiProvider.GetAPIs().VolumeBinder.GetPodVolumeClaims(ctx.klogger, pod)
			if err != nil {
				log.Log(log.ShimContext).Error("Failed to get pod volume claims",
					zap.String("podName", assumedPod.Name),
					zap.Error(err))
				return err
			}

			// get node information
			node, err := ctx.schedulerCache.GetNodeInfo(assumedPod.Spec.NodeName)
			if err != nil {
				log.Log(log.ShimContext).Error("Failed to get node info",
					zap.String("podName", assumedPod.Name),
					zap.String("nodeName", assumedPod.Spec.NodeName),
					zap.Error(err))
				return err
			}

			// retrieve volumes
			volumes, reasons, err := ctx.apiProvider.GetAPIs().VolumeBinder.FindPodVolumes(ctx.klogger, pod, podVolumeClaims, node)
			if err != nil {
				log.Log(log.ShimContext).Error("Failed to find pod volumes",
					zap.String("podName", assumedPod.Name),
					zap.String("nodeName", assumedPod.Spec.NodeName),
					zap.Error(err))
				return err
			}
			if len(reasons) > 0 {
				sReasons := make([]string, 0)
				for _, reason := range reasons {
					sReasons = append(sReasons, string(reason))
				}
				sReason := strings.Join(sReasons, ", ")
				err = fmt.Errorf("pod %s has conflicting volume claims: %s", pod.Name, sReason)
				log.Log(log.ShimContext).Error("Pod has conflicting volume claims",
					zap.String("podName", assumedPod.Name),
					zap.String("nodeName", assumedPod.Spec.NodeName),
					zap.Error(err))
				return err
			}
			if volumes.StaticBindings == nil {
				// convert nil to empty array
				volumes.StaticBindings = make([]*volumebinding.BindingInfo, 0)
			}
			if volumes.DynamicProvisions == nil {
				// convert nil to empty array
				volumes.DynamicProvisions = make([]*v1.PersistentVolumeClaim, 0)
			}
			err = ctx.apiProvider.GetAPIs().VolumeBinder.BindPodVolumes(context.Background(), assumedPod, volumes)
			if err != nil {
				log.Log(log.ShimContext).Error("Failed to bind pod volumes",
					zap.String("podName", assumedPod.Name),
					zap.String("nodeName", assumedPod.Spec.NodeName),
					zap.Int("dynamicProvisions", len(volumes.DynamicProvisions)),
					zap.Int("staticBindings", len(volumes.StaticBindings)))
				return err
			}
		}
	}
	return nil
}

// assume a pod will be running on a node, in scheduler, we maintain
// a cache where stores info for each node what pods are supposed to
// be running on it. And we keep this cache in-sync between core and the shim.
// this way, the core can make allocation decisions with consideration of
// other assumed pods before they are actually bound to the node (bound is slow).
func (ctx *Context) AssumePod(name, node string) error {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if pod := ctx.schedulerCache.GetPod(name); pod != nil {
		// when add assumed pod, we make a copy of the pod to avoid
		// modifying its original reference. otherwise, it may have
		// race when some other go-routines accessing it in parallel.
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			assumedPod := pod.DeepCopy()
			// assume pod volumes, this will update bindings info in cache
			// assume pod volumes before assuming the pod
			// this will update scheduler cache with essential PV/PVC binding info
			var allBound bool
			var err error
			// retrieve the volume claims
			podVolumeClaims, err := ctx.apiProvider.GetAPIs().VolumeBinder.GetPodVolumeClaims(ctx.klogger, pod)
			if err != nil {
				log.Log(log.ShimContext).Error("Failed to get pod volume claims",
					zap.String("podName", assumedPod.Name),
					zap.Error(err))
				return err
			}

			// retrieve volumes
			volumes, reasons, err := ctx.apiProvider.GetAPIs().VolumeBinder.FindPodVolumes(ctx.klogger, pod, podVolumeClaims, targetNode.Node())
			if err != nil {
				log.Log(log.ShimContext).Error("Failed to find pod volumes",
					zap.String("podName", assumedPod.Name),
					zap.String("nodeName", assumedPod.Spec.NodeName),
					zap.Error(err))
				return err
			}
			if len(reasons) > 0 {
				sReasons := make([]string, len(reasons))
				for i, reason := range reasons {
					sReasons[i] = string(reason)
				}
				sReason := strings.Join(sReasons, ", ")
				err = fmt.Errorf("pod %s has conflicting volume claims: %s", pod.Name, sReason)
				log.Log(log.ShimContext).Error("Pod has conflicting volume claims",
					zap.String("podName", assumedPod.Name),
					zap.String("nodeName", assumedPod.Spec.NodeName),
					zap.Error(err))
				return err
			}
			allBound, err = ctx.apiProvider.GetAPIs().VolumeBinder.AssumePodVolumes(ctx.klogger, pod, node, volumes)
			if err != nil {
				return err
			}

			// assign the node name for pod
			assumedPod.Spec.NodeName = node
			ctx.schedulerCache.AssumePod(assumedPod, allBound)
			return nil
		}
	}
	return nil
}

// forget pod must be called when a pod is assumed to be running on a node,
// but then for some reason it is failed to bind or released.
func (ctx *Context) ForgetPod(name string) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	if pod := ctx.schedulerCache.GetPod(name); pod != nil {
		log.Log(log.ShimContext).Debug("forget pod", zap.String("pod", pod.Name))
		ctx.schedulerCache.ForgetPod(pod)
		return
	}
	log.Log(log.ShimContext).Debug("unable to forget pod: not found in cache", zap.String("pod", name))
}

func (ctx *Context) UpdateApplication(app *Application) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.applications[app.applicationID] = app
}

// IsTaskMaybeSchedulable returns true if a task might be currently able to be scheduled. This uses a bloom filter
// cached from a set of taskIDs to perform efficient negative lookups.
func (ctx *Context) IsTaskMaybeSchedulable(taskID string) bool {
	return ctx.schedulerCache.IsTaskMaybeSchedulable(taskID)
}

func (ctx *Context) AddPendingPodAllocation(podKey string, nodeID string) {
	ctx.schedulerCache.AddPendingPodAllocation(podKey, nodeID)
}

func (ctx *Context) RemovePodAllocation(podKey string) {
	ctx.schedulerCache.RemovePodAllocation(podKey)
}

func (ctx *Context) GetPendingPodAllocation(podKey string) (nodeID string, ok bool) {
	nodeID, ok = ctx.schedulerCache.GetPendingPodAllocation(podKey)
	return nodeID, ok
}

func (ctx *Context) GetInProgressPodAllocation(podKey string) (nodeID string, ok bool) {
	nodeID, ok = ctx.schedulerCache.GetInProgressPodAllocation(podKey)
	return nodeID, ok
}

func (ctx *Context) StartPodAllocation(podKey string, nodeID string) bool {
	return ctx.schedulerCache.StartPodAllocation(podKey, nodeID)
}

func (ctx *Context) NotifyTaskComplete(appID, taskID string) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.notifyTaskComplete(appID, taskID)
}

func (ctx *Context) notifyTaskComplete(appID, taskID string) {
	log.Log(log.ShimContext).Debug("NotifyTaskComplete",
		zap.String("appID", appID),
		zap.String("taskID", taskID))
	if app := ctx.getApplication(appID); app != nil {
		log.Log(log.ShimContext).Debug("release allocation",
			zap.String("appID", appID),
			zap.String("taskID", taskID))
		ev := NewSimpleTaskEvent(appID, taskID, CompleteTask)
		dispatcher.Dispatch(ev)
		if app.GetApplicationState() == ApplicationStates().Resuming {
			dispatcher.Dispatch(NewSimpleApplicationEvent(appID, AppTaskCompleted))
		}
	}
}

// update application tags in the AddApplicationRequest based on the namespace annotation
// adds the following tags to the request based on annotations (if exist):
//   - namespace.resourcequota
//   - namespace.parentqueue
func (ctx *Context) updateApplicationTags(request *AddApplicationRequest, namespace string) {
	namespaceObj := ctx.getNamespaceObject(namespace)
	if namespaceObj == nil {
		return
	}
	// add resource quota info as an app tag
	resourceQuota := utils.GetNamespaceQuotaFromAnnotation(namespaceObj)
	if resourceQuota != nil && !common.IsZero(resourceQuota) {
		if quota, err := json.Marshal(resourceQuota); err == nil {
			request.Metadata.Tags[siCommon.AppTagNamespaceResourceQuota] = string(quota)
		}
	}

	// add guaranteed resource info as an app tag
	guaranteedResource := utils.GetNamespaceGuaranteedFromAnnotation(namespaceObj)
	if guaranteedResource != nil && !common.IsZero(guaranteedResource) {
		if guaranteed, err := json.Marshal(guaranteedResource); err == nil {
			request.Metadata.Tags[siCommon.AppTagNamespaceResourceGuaranteed] = string(guaranteed)
		}
	}

	// add parent queue info as an app tag
	parentQueue := utils.GetNameSpaceAnnotationValue(namespaceObj, constants.AnnotationParentQueue)
	if parentQueue != "" {
		request.Metadata.Tags[constants.AppTagNamespaceParentQueue] = parentQueue
	}
}

// returns the namespace object from the namespace's name
// if the namespace is unable to be listed from api-server, a nil is returned
func (ctx *Context) getNamespaceObject(namespace string) *v1.Namespace {
	if namespace == "" {
		log.Log(log.ShimContext).Debug("could not get namespace from empty string")
		return nil
	}

	nsLister := ctx.apiProvider.GetAPIs().NamespaceInformer.Lister()
	namespaceObj, err := nsLister.Get(namespace)
	if err != nil {
		// every app should belong to a namespace,
		// if we cannot list the namespace here, probably something is wrong
		// log an error here and skip retrieving the resource quota
		log.Log(log.ShimContext).Error("failed to get app namespace", zap.Error(err))
		return nil
	}
	return namespaceObj
}

func (ctx *Context) AddApplication(request *AddApplicationRequest) *Application {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	return ctx.addApplication(request)
}

func (ctx *Context) addApplication(request *AddApplicationRequest) *Application {
	log.Log(log.ShimContext).Debug("AddApplication", zap.Any("Request", request))
	if app := ctx.getApplication(request.Metadata.ApplicationID); app != nil {
		return app
	}

	if ns, ok := request.Metadata.Tags[constants.AppTagNamespace]; ok {
		log.Log(log.ShimContext).Debug("app namespace info",
			zap.String("appID", request.Metadata.ApplicationID),
			zap.String("namespace", ns))
		ctx.updateApplicationTags(request, ns)
	}

	app := NewApplication(
		request.Metadata.ApplicationID,
		request.Metadata.QueueName,
		request.Metadata.User,
		request.Metadata.Groups,
		request.Metadata.Tags,
		ctx.apiProvider.GetAPIs().SchedulerAPI)
	app.setTaskGroups(request.Metadata.TaskGroups)
	app.setTaskGroupsDefinition(request.Metadata.Tags[constants.AnnotationTaskGroups])
	app.setSchedulingParamsDefinition(request.Metadata.Tags[constants.AnnotationSchedulingPolicyParam])
	if request.Metadata.CreationTime != 0 {
		app.tags[siCommon.DomainYuniKorn+siCommon.CreationTime] = strconv.FormatInt(request.Metadata.CreationTime, 10)
	}
	if request.Metadata.SchedulingPolicyParameters != nil {
		app.SetPlaceholderTimeout(request.Metadata.SchedulingPolicyParameters.GetPlaceholderTimeout())
		app.setSchedulingStyle(request.Metadata.SchedulingPolicyParameters.GetGangSchedulingStyle())
	}
	app.setPlaceholderOwnerReferences(request.Metadata.OwnerReferences)

	// add into cache
	ctx.applications[app.applicationID] = app
	log.Log(log.ShimContext).Info("app added",
		zap.String("appID", app.applicationID))

	return app
}

func (ctx *Context) IsPreemptSelfAllowed(priorityClassName string) bool {
	priorityClass := ctx.schedulerCache.GetPriorityClass(priorityClassName)
	if priorityClass == nil {
		return true
	}
	if value, ok := priorityClass.Annotations[constants.AnnotationAllowPreemption]; ok {
		if value == constants.False {
			return false
		}
	}
	return true
}

func (ctx *Context) GetApplication(appID string) *Application {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	return ctx.getApplication(appID)
}

func (ctx *Context) getApplication(appID string) *Application {
	if app, ok := ctx.applications[appID]; ok {
		return app
	}
	return nil
}

func (ctx *Context) RemoveApplication(appID string) error {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if app, exist := ctx.applications[appID]; exist {
		// get the non-terminated task alias
		nonTerminatedTaskAlias := app.getNonTerminatedTaskAlias()
		// check there are any non-terminated task or not
		if len(nonTerminatedTaskAlias) > 0 {
			return fmt.Errorf("failed to remove application %s because it still has task in non-terminated task, tasks: %s", appID, strings.Join(nonTerminatedTaskAlias, ","))
		}
		// send the update request to scheduler core
		rr := common.CreateUpdateRequestForRemoveApplication(app.applicationID, app.partition)
		if err := ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateApplication(rr); err != nil {
			log.Log(log.ShimContext).Error("failed to send remove application request to core", zap.Error(err))
		}
		delete(ctx.applications, appID)
		log.Log(log.ShimContext).Info("app removed",
			zap.String("appID", appID))

		return nil
	}
	return fmt.Errorf("application %s is not found in the context", appID)
}

func (ctx *Context) RemoveApplicationInternal(appID string) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if _, exist := ctx.applications[appID]; !exist {
		log.Log(log.ShimContext).Debug("Attempted to remove non-existent application", zap.String("appID", appID))
		return
	}
	delete(ctx.applications, appID)
}

// this implements ApplicationManagementProtocol
func (ctx *Context) AddTask(request *AddTaskRequest) *Task {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	return ctx.addTask(request)
}

func (ctx *Context) addTask(request *AddTaskRequest) *Task {
	log.Log(log.ShimContext).Debug("AddTask",
		zap.String("appID", request.Metadata.ApplicationID),
		zap.String("taskID", request.Metadata.TaskID))
	if app := ctx.getApplication(request.Metadata.ApplicationID); app != nil {
		existingTask, err := app.GetTask(request.Metadata.TaskID)
		if err != nil {
			var originator bool

			// Is this task the originator of the application?
			// If yes, then make it as "first pod/owner/driver" of the application and set the task as originator
			// At any cost, placeholder cannot become originator
			if !request.Metadata.Placeholder && app.GetOriginatingTask() == nil {
				for _, ownerReference := range app.getPlaceholderOwnerReferences() {
					referenceID := string(ownerReference.UID)
					if request.Metadata.TaskID == referenceID {
						originator = true
						break
					}
				}
			}
			task := NewFromTaskMeta(request.Metadata.TaskID, app, ctx, request.Metadata, originator)
			app.addTask(task)
			log.Log(log.ShimContext).Info("task added",
				zap.String("appID", app.applicationID),
				zap.String("taskID", task.taskID),
				zap.String("taskState", task.GetTaskState()))
			if originator {
				if app.GetOriginatingTask() != nil {
					log.Log(log.ShimContext).Error("Inconsistent state - found another originator task for an application",
						zap.String("taskId", task.GetTaskID()))
				}
				app.setOriginatingTask(task)
				log.Log(log.ShimContext).Info("app request originating pod added",
					zap.String("appID", app.applicationID),
					zap.String("original task", task.GetTaskID()))
			}
			return task
		}
		return existingTask
	}
	return nil
}

func (ctx *Context) RemoveTask(appID, taskID string) {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	app, ok := ctx.applications[appID]
	if !ok {
		log.Log(log.ShimContext).Debug("Attempted to remove task from non-existent application", zap.String("appID", appID))
		return
	}
	app.RemoveTask(taskID)
}

func (ctx *Context) getTask(appID string, taskID string) *Task {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	app := ctx.getApplication(appID)
	if app == nil {
		log.Log(log.ShimContext).Debug("application is not found in the context",
			zap.String("appID", appID))
		return nil
	}
	task, err := app.GetTask(taskID)
	if err != nil {
		log.Log(log.ShimContext).Debug("task is not found in applications",
			zap.String("taskID", taskID),
			zap.String("appID", appID))
		return nil
	}
	return task
}

func (ctx *Context) GetAllApplications() []*Application {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()

	apps := make([]*Application, 0, len(ctx.applications))
	for _, app := range ctx.applications {
		apps = append(apps, app)
	}

	return apps
}

func (ctx *Context) PublishEvents(eventRecords []*si.EventRecord) {
	if len(eventRecords) > 0 {
		for _, record := range eventRecords {
			switch record.Type {
			case si.EventRecord_REQUEST:
				appID := record.ReferenceID
				taskID := record.ObjectID
				if task := ctx.getTask(appID, taskID); task != nil {
					events.GetRecorder().Eventf(task.GetTaskPod().DeepCopy(), nil,
						v1.EventTypeNormal, "Informational", "Informational", record.Message)
				} else {
					log.Log(log.ShimContext).Warn("task event is not published because task is not found",
						zap.String("appID", appID),
						zap.String("taskID", taskID),
						zap.Stringer("event", record))
				}
			case si.EventRecord_NODE:
				if !isPublishableNodeEvent(record) {
					continue
				}
				nodeID := record.ObjectID
				nodeInfo := ctx.schedulerCache.GetNode(nodeID)
				if nodeInfo == nil {
					log.Log(log.ShimContext).Warn("node event is not published because nodeInfo is not found",
						zap.String("nodeID", nodeID),
						zap.Stringer("event", record))
					continue
				}
				node := nodeInfo.Node()
				if node == nil {
					log.Log(log.ShimContext).Warn("node event is not published because node is not found",
						zap.String("nodeID", nodeID),
						zap.Stringer("event", record))
					continue
				}
				events.GetRecorder().Eventf(node.DeepCopy(), nil,
					v1.EventTypeNormal, "Informational", "Informational", record.Message)
			}
		}
	}
}

// update task's pod condition when the condition has not yet updated,
// return true if the update was done and false if the update is skipped due to any error, or a dup operation
func (ctx *Context) updatePodCondition(task *Task, podCondition *v1.PodCondition) bool {
	if task.GetTaskState() == TaskStates().Scheduling {
		// only update the pod when pod condition changes
		// minimize the overhead added to the api-server/etcd
		if ok, podCopy := task.UpdatePodCondition(podCondition); ok {
			_, err := ctx.apiProvider.GetAPIs().KubeClient.UpdateStatus(podCopy)
			if err == nil {
				return true
			}
			// only log the error here, no need to handle it if the update failed
			log.Log(log.ShimContext).Error("update pod condition failed",
				zap.Error(err))
		}
	}

	return false
}

// this function handles the pod scheduling failures with respect to the different causes,
// and update the pod condition accordingly. the cluster autoscaler depends on the certain
// pod condition in order to trigger auto-scaling.
func (ctx *Context) HandleContainerStateUpdate(request *si.UpdateContainerSchedulingStateRequest) {
	// the allocationKey equals to the taskID
	if task := ctx.getTask(request.ApplicationID, request.AllocationKey); task != nil {
		switch request.State {
		case si.UpdateContainerSchedulingStateRequest_SKIPPED:
			// auto-scaler scans pods whose pod condition is PodScheduled=false && reason=Unschedulable
			// if the pod is skipped because the queue quota has been exceeded, we do not trigger the auto-scaling
			task.SetTaskSchedulingState(TaskSchedSkipped)
			ctx.schedulerCache.NotifyTaskSchedulerAction(task.taskID)
			if ctx.updatePodCondition(task,
				&v1.PodCondition{
					Type:    v1.PodScheduled,
					Status:  v1.ConditionFalse,
					Reason:  "SchedulingSkipped",
					Message: request.Reason,
				}) {
				events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
					v1.EventTypeNormal, "PodUnschedulable", "PodUnschedulable",
					"Task %s is skipped from scheduling because the queue quota has been exceed", task.alias)
			}
		case si.UpdateContainerSchedulingStateRequest_FAILED:
			task.SetTaskSchedulingState(TaskSchedFailed)
			ctx.schedulerCache.NotifyTaskSchedulerAction(task.taskID)
			// set pod condition to Unschedulable in order to trigger auto-scaling
			if ctx.updatePodCondition(task,
				&v1.PodCondition{
					Type:    v1.PodScheduled,
					Status:  v1.ConditionFalse,
					Reason:  v1.PodReasonUnschedulable,
					Message: request.Reason,
				}) {
				events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
					v1.EventTypeNormal, "PodUnschedulable", "PodUnschedulable",
					"Task %s is pending for the requested resources become available", task.alias)
			}
		default:
			log.Log(log.ShimContext).Warn("no handler for container scheduling state",
				zap.Stringer("state", request.State))
		}
	}
}

func (ctx *Context) ApplicationEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			appID := event.GetApplicationID()
			app := ctx.GetApplication(appID)
			if app == nil {
				log.Log(log.ShimContext).Error("failed to handle application event, application does not exist",
					zap.String("applicationID", appID))
				return
			}

			if app.canHandle(event) {
				if err := app.handle(event); err != nil {
					log.Log(log.ShimContext).Error("failed to handle application event",
						zap.String("event", event.GetEvent()),
						zap.String("applicationID", appID),
						zap.Error(err))
				}
				return
			}

			log.Log(log.ShimContext).Error("application event cannot be handled in the current state",
				zap.String("applicationID", appID),
				zap.String("event", event.GetEvent()),
				zap.String("state", app.sm.Current()))
			return
		}

		log.Log(log.ShimContext).Error("could not handle application event",
			zap.String("type", reflect.TypeOf(obj).Name()))
	}
}

func (ctx *Context) TaskEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.TaskEvent); ok {
			appID := event.GetApplicationID()
			taskID := event.GetTaskID()
			task := ctx.getTask(appID, taskID)
			if task == nil {
				log.Log(log.ShimContext).Error("failed to handle task event, task does not exist",
					zap.String("applicationID", appID),
					zap.String("taskID", taskID))
				return
			}

			if task.canHandle(event) {
				if err := task.handle(event); err != nil {
					log.Log(log.ShimContext).Error("failed to handle task event",
						zap.String("applicationID", appID),
						zap.String("taskID", taskID),
						zap.String("event", event.GetEvent()),
						zap.Error(err))
				}
				return
			}

			log.Log(log.ShimContext).Error("task event cannot be handled in the current state",
				zap.String("applicationID", appID),
				zap.String("taskID", taskID),
				zap.String("event", event.GetEvent()),
				zap.String("state", task.sm.Current()))
			return
		}

		log.Log(log.ShimContext).Error("could not handle task event",
			zap.String("type", reflect.TypeOf(obj).Name()))
	}
}

func (ctx *Context) LoadConfigMaps() ([]*v1.ConfigMap, error) {
	kubeClient := ctx.apiProvider.GetAPIs().KubeClient

	defaults, err := kubeClient.GetConfigMap(ctx.namespace, constants.DefaultConfigMapName)
	if err != nil {
		return nil, err
	}

	config, err := kubeClient.GetConfigMap(ctx.namespace, constants.ConfigMapName)
	if err != nil {
		return nil, err
	}

	return []*v1.ConfigMap{defaults, config}, nil
}

func (ctx *Context) GetStateDump() (string, error) {
	log.Log(log.ShimContext).Info("State dump requested")

	dump := map[string]interface{}{
		"cache": ctx.schedulerCache.GetSchedulerCacheDao(),
	}

	bytes, err := json.Marshal(dump)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func isPublishableNodeEvent(event *si.EventRecord) bool {
	// we only send node added & removed event
	if event.Type == si.EventRecord_NODE &&
		((event.EventChangeDetail == si.EventRecord_DETAILS_NONE && event.EventChangeType == si.EventRecord_ADD) ||
			(event.EventChangeDetail == si.EventRecord_NODE_DECOMISSION && event.EventChangeType == si.EventRecord_REMOVE)) {
		return true
	}

	return false
}

// VisibleForTesting
func (ctx *Context) GetSchedulerCache() *schedulercache.SchedulerCache {
	return ctx.schedulerCache
}

// InitializeState is used to initialize the state of the scheduler context using the Kubernetes informers.
// This registers priority classes, nodes, and pods and ensures the scheduler core is synchronized.
func (ctx *Context) InitializeState() error {
	// Step 1: Register priority classes. This is first so that we can rely on the information they
	// provide to properly register tasks with correct priority and preemption metadata.
	priorityClasses, err := ctx.registerPriorityClasses()
	if err != nil {
		log.Log(log.ShimContext).Error("failed to register priority classes", zap.Error(err))
		return err
	}

	// Step 2: Register nodes. Nodes are registered with the scheduler core in an initially disabled state.
	// This allows the existing allocations for each node to be processed before activating the node.
	nodes, err := ctx.loadNodes()
	if err != nil {
		log.Log(log.ShimContext).Error("failed to load nodes", zap.Error(err))
		return err
	}
	acceptedNodes, err := ctx.registerNodes(nodes)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to register nodes", zap.Error(err))
		return err
	}
	ctx.addNodesWithoutRegistering(acceptedNodes)

	// Step 3: Register pods. Pods are handled in creation order to provide consistency with previous scheduler runs.
	// If pods are associated with existing nodes, they are treated as allocations (rather than asks).
	pods, err := ctx.registerPods()
	if err != nil {
		log.Log(log.ShimContext).Error("failed to register pods", zap.Error(err))
		return err
	}

	// Step 4: Enable nodes. At this point all allocations and asks have been processed, so it is safe to allow the
	// core to begin scheduling.
	err = ctx.enableNodes(acceptedNodes)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to enable nodes", zap.Error(err))
		return err
	}

	// Step 5: Start scheduling event handlers. At this point, initialization is mostly complete, and any existing
	// objects will show up as newly added objects. Since the add/update event handlers are idempotent, this is fine.
	err = ctx.AddSchedulingEventHandlers()
	if err != nil {
		log.Log(log.Admission).Error("failed to add scheduling event handlers", zap.Error(err))
		return err
	}

	// Step 6: Finalize priority classes. Between the start of initialization and when the informer event handlers are
	// registered, it is possible that a priority class object was deleted. Process them again and remove
	// any that no longer exist.
	err = ctx.finalizePriorityClasses(priorityClasses)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to finalize priority classes", zap.Error(err))
		return err
	}

	// Step 7: Finalize nodes. Between the start of initialization and when the informer event handlers are registered,
	// it is possible that a node object was deleted. Process them again and remove any that no longer exist.
	err = ctx.finalizeNodes(nodes)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to finalize nodes", zap.Error(err))
		return err
	}

	// Step 8: Finalize pods. Between the start of initialization and when the informer event handlers are registered,
	// it is possible that a pod object was deleted. Process them again and remove any that no longer exist.
	err = ctx.finalizePods(pods)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to finalize pods", zap.Error(err))
		return err
	}

	return nil
}

func (ctx *Context) registerPriorityClasses() ([]*schedulingv1.PriorityClass, error) {
	// list all priority classes via the informer
	priorityClasses, err := ctx.apiProvider.GetAPIs().PriorityClassInformer.Lister().List(labels.Everything())
	if err != nil {
		log.Log(log.ShimContext).Error("Failed to read priority classes from informer", zap.Error(err))
		return nil, err
	}
	for _, priorityClass := range priorityClasses {
		ctx.schedulerCache.UpdatePriorityClass(priorityClass)
	}
	return priorityClasses, nil
}

func (ctx *Context) finalizePriorityClasses(existingPriorityClasses []*schedulingv1.PriorityClass) error {
	// list all priority classes via the informer
	priorityClasses, err := ctx.apiProvider.GetAPIs().PriorityClassInformer.Lister().List(labels.Everything())
	if err != nil {
		log.Log(log.ShimContext).Error("Failed to read priority classes from informer", zap.Error(err))
		return err
	}

	// convert the priority class list into a map
	pcMap := make(map[string]*schedulingv1.PriorityClass)
	for _, priorityClass := range priorityClasses {
		pcMap[priorityClass.Name] = priorityClass
	}

	// find any existing priority classes that no longer exist
	for _, priorityClass := range existingPriorityClasses {
		if _, ok := pcMap[priorityClass.Name]; !ok {
			// priority class no longer exists, delete it
			log.Log(log.ShimContext).Info("Removing priority class which went away during initialization",
				zap.String("name", priorityClass.Name))
			ctx.deletePriorityClass(priorityClass)
		}
	}

	return nil
}

func (ctx *Context) loadNodes() ([]*v1.Node, error) {
	// list all nodes via the informer
	nodes, err := ctx.apiProvider.GetAPIs().NodeInformer.Lister().List(labels.Everything())
	if err != nil {
		log.Log(log.ShimContext).Error("Failed to read nodes from informer", zap.Error(err))
		return nil, err
	}
	return nodes, err
}

func (ctx *Context) registerNode(node *v1.Node) error {
	acceptedNodes, err := ctx.registerNodes([]*v1.Node{node})
	if err != nil {
		return err
	}
	if len(acceptedNodes) != 1 {
		return fmt.Errorf("node rejected: %s", node.Name)
	}
	return nil
}

func (ctx *Context) registerNodes(nodes []*v1.Node) ([]*v1.Node, error) {
	nodesToRegister := make([]*si.NodeInfo, 0)
	pendingNodes := make(map[string]*v1.Node)
	acceptedNodes := make([]*v1.Node, 0)
	rejectedNodes := make([]*v1.Node, 0)

	// Generate a NodeInfo object for each node and add to the registration request
	for _, node := range nodes {
		log.Log(log.ShimContext).Info("Registering node", zap.String("name", node.Name))
		nodeStatus := node.Status
		nodesToRegister = append(nodesToRegister, &si.NodeInfo{
			NodeID: node.Name,
			Action: si.NodeInfo_CREATE_DRAIN,
			Attributes: map[string]string{
				constants.DefaultNodeAttributeHostNameKey: node.Name,
				constants.DefaultNodeAttributeRackNameKey: constants.DefaultRackName,
			},
			SchedulableResource: common.GetNodeResource(&nodeStatus),
			OccupiedResource:    common.NewResourceBuilder().Build(),
			ExistingAllocations: make([]*si.Allocation, 0),
		})
		pendingNodes[node.Name] = node
	}

	var wg sync.WaitGroup

	// initialize wait group with the number of responses we expect
	wg.Add(len(pendingNodes))

	// register with the dispatcher so that we can track our response
	handlerID := fmt.Sprintf("%s-%d", registerNodeContextHandler, ctx.txnID.Add(1))
	dispatcher.RegisterEventHandler(handlerID, dispatcher.EventTypeNode, func(event interface{}) {
		nodeEvent, ok := event.(CachedSchedulerNodeEvent)
		if !ok {
			return
		}
		node, ok := pendingNodes[nodeEvent.NodeID]
		if !ok {
			return
		}
		delete(pendingNodes, nodeEvent.NodeID)

		switch nodeEvent.Event {
		case NodeAccepted:
			log.Log(log.ShimContext).Info("Node registration accepted", zap.String("name", nodeEvent.NodeID))
			acceptedNodes = append(acceptedNodes, node)
		case NodeRejected:
			log.Log(log.ShimContext).Warn("Node registration rejected", zap.String("name", nodeEvent.NodeID))
			rejectedNodes = append(rejectedNodes, node)
		default:
			log.Log(log.ShimContext).Error("BUG: Unexpected node event", zap.Stringer("eventType", nodeEvent.Event))
		}
		wg.Done()
	})
	defer dispatcher.UnregisterEventHandler(handlerID, dispatcher.EventTypeNode)

	if err := ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateNode(&si.NodeRequest{
		Nodes: nodesToRegister,
		RmID:  schedulerconf.GetSchedulerConf().ClusterID,
	}); err != nil {
		log.Log(log.ShimContext).Error("Failed to register nodes", zap.Error(err))
		return nil, err
	}

	// wait for all responses to accumulate
	wg.Wait()

	for _, node := range acceptedNodes {
		// post a successful event to the node
		events.GetRecorder().Eventf(node.DeepCopy(), nil, v1.EventTypeNormal, "NodeAccepted", "NodeAccepted",
			fmt.Sprintf("node %s is accepted by the scheduler", node.Name))
	}
	for _, node := range rejectedNodes {
		// post a failure event to the node
		events.GetRecorder().Eventf(node.DeepCopy(), nil, v1.EventTypeWarning, "NodeRejected", "NodeRejected",
			fmt.Sprintf("node %s is rejected by the scheduler", node.Name))
	}

	return acceptedNodes, nil
}

func (ctx *Context) decommissionNode(node *v1.Node) error {
	request := common.CreateUpdateRequestForDeleteOrRestoreNode(node.Name, si.NodeInfo_DECOMISSION)
	return ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateNode(request)
}

func (ctx *Context) updateNodeResources(node *v1.Node, capacity *si.Resource, occupied *si.Resource) error {
	request := common.CreateUpdateRequestForUpdatedNode(node.Name, capacity, occupied)
	return ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateNode(request)
}

func (ctx *Context) enableNode(node *v1.Node) error {
	return ctx.enableNodes([]*v1.Node{node})
}

func (ctx *Context) enableNodes(nodes []*v1.Node) error {
	nodesToEnable := make([]*si.NodeInfo, 0)

	// Generate a NodeInfo object for each node and add to the enablement request
	for _, node := range nodes {
		log.Log(log.ShimContext).Info("Enabling node", zap.String("name", node.Name))
		nodesToEnable = append(nodesToEnable, &si.NodeInfo{
			NodeID:     node.Name,
			Action:     si.NodeInfo_DRAIN_TO_SCHEDULABLE,
			Attributes: map[string]string{},
		})
	}

	// enable scheduling on all nodes
	if err := ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateNode(&si.NodeRequest{
		Nodes: nodesToEnable,
		RmID:  schedulerconf.GetSchedulerConf().ClusterID,
	}); err != nil {
		log.Log(log.ShimContext).Error("Failed to enable nodes", zap.Error(err))
		return err
	}
	return nil
}

func (ctx *Context) finalizeNodes(existingNodes []*v1.Node) error {
	// list all nodes via the informer
	nodes, err := ctx.apiProvider.GetAPIs().NodeInformer.Lister().List(labels.Everything())
	if err != nil {
		log.Log(log.ShimContext).Error("Failed to read nodes from informer", zap.Error(err))
		return err
	}

	// convert the node list into a map
	nodeMap := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeMap[node.Name] = node
	}

	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	// find any existing nodes that no longer exist
	for _, node := range existingNodes {
		if _, ok := nodeMap[node.Name]; !ok {
			// node no longer exists, delete it
			log.Log(log.ShimContext).Info("Removing node which went away during initialization",
				zap.String("name", node.Name))
			ctx.deleteNodeInternal(node)
		}
	}

	return nil
}

func (ctx *Context) registerPods() ([]*v1.Pod, error) {
	log.Log(log.ShimContext).Info("Starting pod registration...")

	// list all pods via the informer
	pods, err := ctx.apiProvider.GetAPIs().PodInformer.Lister().List(labels.Everything())
	if err != nil {
		log.Log(log.ShimContext).Error("Failed to read pods from informer", zap.Error(err))
		return nil, err
	}

	// sort pods by creation time so that overall queue ordering is consistent with prior runs
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.Unix() < pods[j].CreationTimestamp.Unix()
	})

	// add all pods to the context
	for i, pod := range pods {
		// skip terminated pods: we do not add or finalise them later
		if utils.IsPodTerminated(pod) {
			pods[i] = nil
			continue
		}
		ctx.AddPod(pod)
	}

	log.Log(log.ShimContext).Info("Finished pod registration...")
	return pods, nil
}

func (ctx *Context) finalizePods(existingPods []*v1.Pod) error {
	// list all pods via the informer
	pods, err := ctx.apiProvider.GetAPIs().PodInformer.Lister().List(labels.Everything())
	if err != nil {
		log.Log(log.ShimContext).Error("Failed to read pods from informer", zap.Error(err))
		return err
	}

	// convert the pod list into a map
	podMap := make(map[types.UID]*v1.Pod)
	for _, pod := range pods {
		// if the pod is terminated finalising should remove it if it was running in register
		if utils.IsPodTerminated(pod) {
			continue
		}
		podMap[pod.UID] = pod
	}

	// find any existing pods that no longer exist
	for _, pod := range existingPods {
		// skip if the pod was already terminated during register
		if pod == nil {
			continue
		}
		if _, ok := podMap[pod.UID]; !ok {
			// pod no longer exists, delete it
			log.Log(log.ShimContext).Info("Removing pod which went away during initialization",
				zap.String("namespace", pod.Namespace),
				zap.String("name", pod.Name),
				zap.String("uid", string(pod.UID)))
			ctx.DeletePod(pod)
		}
	}

	return nil
}

// for a given pod, return an allocation if found
func getExistingAllocation(pod *v1.Pod) *si.Allocation {
	// skip terminated pods
	if utils.IsPodTerminated(pod) {
		return nil
	}

	if meta, valid := getAppMetadata(pod); valid {
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

func convertToNode(obj interface{}) (*v1.Node, error) {
	if node, ok := obj.(*v1.Node); ok {
		return node, nil
	}
	return nil, fmt.Errorf("cannot convert to *v1.Node: %v", obj)
}
