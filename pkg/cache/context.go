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
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	schedulercache "github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	schedulerconf "github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/predicates"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/support"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// context maintains scheduling state, like apps and apps' tasks.
type Context struct {
	applications   map[string]*Application        // apps
	nodes          *schedulerNodes                // nodes
	schedulerCache *schedulercache.SchedulerCache // external cache
	apiProvider    client.APIProvider             // apis to interact with api-server, scheduler-core, etc
	predManager    predicates.PredicateManager    // K8s predicates
	pluginMode     bool                           // true if we are configured as a scheduler plugin
	namespace      string                         // yunikorn namespace
	configMaps     []*v1.ConfigMap                // cached yunikorn configmaps
	lock           *sync.RWMutex                  // lock
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
		namespace:    apis.GetAPIs().GetConf().Namespace,
		configMaps:   bootstrapConfigMaps,
		lock:         &sync.RWMutex{},
	}

	// create the cache
	ctx.schedulerCache = schedulercache.NewSchedulerCache(apis.GetAPIs())

	// init the controllers and plugins (need the cache)
	ctx.nodes = newSchedulerNodes(apis.GetAPIs().SchedulerAPI, ctx.schedulerCache)

	// create the predicate manager
	sharedLister := support.NewSharedLister(ctx.schedulerCache)
	clientSet := apis.GetAPIs().KubeClient.GetClientSet()
	informerFactory := apis.GetAPIs().InformerFactory
	ctx.predManager = predicates.NewPredicateManager(support.NewFrameworkHandle(sharedLister, informerFactory, clientSet))

	return ctx
}

func (ctx *Context) AddSchedulingEventHandlers() {
	ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.NodeInformerHandlers,
		AddFn:    ctx.addNode,
		UpdateFn: ctx.updateNode,
		DeleteFn: ctx.deleteNode,
	})

	ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.PodInformerHandlers,
		AddFn:    ctx.addPod,
		UpdateFn: ctx.updatePod,
		DeleteFn: ctx.deletePod,
	})

	ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.ConfigMapInformerHandlers,
		FilterFn: ctx.filterConfigMaps,
		AddFn:    ctx.addConfigMaps,
		UpdateFn: ctx.updateConfigMaps,
		DeleteFn: ctx.deleteConfigMaps,
	})
	ctx.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.PriorityClassInformerHandlers,
		FilterFn: ctx.filterPriorityClasses,
		AddFn:    ctx.addPriorityClass,
		UpdateFn: ctx.updatePriorityClass,
		DeleteFn: ctx.deletePriorityClass,
	})
}

func (ctx *Context) IsPluginMode() bool {
	return ctx.pluginMode
}

func (ctx *Context) addNode(obj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	node, err := convertToNode(obj)
	if err != nil {
		log.Log(log.ShimContext).Error("node conversion failed", zap.Error(err))
		return
	}

	// add node to secondary scheduler cache
	log.Log(log.ShimContext).Warn("adding node to cache", zap.String("NodeName", node.Name))
	ctx.schedulerCache.AddNode(node)

	// add node to internal cache
	ctx.nodes.addNode(node)

	// post the event
	events.GetRecorder().Eventf(node.DeepCopy(), nil, v1.EventTypeNormal, "NodeAccepted", "NodeAccepted",
		fmt.Sprintf("node %s is accepted by the scheduler", node.Name))
}

func (ctx *Context) updateNode(oldObj, newObj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	// we only trigger update when resource changes
	oldNode, err := convertToNode(oldObj)
	if err != nil {
		log.Log(log.ShimContext).Error("old node conversion failed",
			zap.Error(err))
		return
	}

	newNode, err := convertToNode(newObj)
	if err != nil {
		log.Log(log.ShimContext).Error("new node conversion failed",
			zap.Error(err))
		return
	}

	// update secondary cache
	ctx.schedulerCache.UpdateNode(newNode)

	// update primary cache
	ctx.nodes.updateNode(oldNode, newNode)
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

	// delete node from secondary cache
	log.Log(log.ShimContext).Debug("delete node from cache", zap.String("nodeName", node.Name))
	ctx.schedulerCache.RemoveNode(node)

	// delete node from primary cache
	ctx.nodes.deleteNode(node)

	// post the event
	events.GetRecorder().Eventf(node.DeepCopy(), nil, v1.EventTypeNormal, "NodeDeleted", "NodeDeleted",
		fmt.Sprintf("node %s is deleted from the scheduler", node.Name))
}

func (ctx *Context) addPod(obj interface{}) {
	pod, err := utils.Convert2Pod(obj)
	if err != nil {
		log.Log(log.ShimContext).Error("failed to add pod", zap.Error(err))
		return
	}
	if utils.GetApplicationIDFromPod(pod) == "" {
		ctx.updateForeignPod(pod)
	} else {
		ctx.updateYuniKornPod(pod)
	}
}

func (ctx *Context) updatePod(_, newObj interface{}) {
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
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	// treat terminated pods like a remove
	if utils.IsPodTerminated(pod) {
		log.Log(log.ShimContext).Debug("Request to update terminated pod, removing from cache", zap.String("podName", pod.Name))
		ctx.schedulerCache.RemovePod(pod)
		return
	}
	ctx.schedulerCache.UpdatePod(pod)
}

func (ctx *Context) updateForeignPod(pod *v1.Pod) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	podStatusBefore := ""
	oldPod, ok := ctx.schedulerCache.GetPod(string(pod.UID))
	if ok {
		podStatusBefore = string(oldPod.Status.Phase)
	}

	// conditions for allocate:
	//   1. pod was previously assigned
	//   2. pod is now assigned
	//   3. pod is not in terminated state
	if oldPod == nil && utils.IsAssignedPod(pod) && !utils.IsPodTerminated(pod) {
		log.Log(log.ShimContext).Debug("pod is assigned to a node, trigger occupied resource update",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.String("podStatusBefore", podStatusBefore),
			zap.String("podStatusCurrent", string(pod.Status.Phase)))
		podResource := common.GetPodResource(pod)
		ctx.nodes.updateNodeOccupiedResources(pod.Spec.NodeName, podResource, AddOccupiedResource)
		ctx.schedulerCache.AddPod(pod)
		return
	}

	// conditions for release:
	//   1. pod was previously assigned
	//   2. pod is now in a terminated state
	if oldPod != nil && utils.IsPodTerminated(pod) {
		log.Log(log.ShimContext).Debug("pod terminated, trigger occupied resource update",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.String("podStatusBefore", podStatusBefore),
			zap.String("podStatusCurrent", string(pod.Status.Phase)))
		// this means pod is terminated
		// we need sub the occupied resource and re-sync with the scheduler-core
		podResource := common.GetPodResource(pod)
		ctx.nodes.updateNodeOccupiedResources(pod.Spec.NodeName, podResource, SubOccupiedResource)
		ctx.schedulerCache.RemovePod(pod)
		return
	}
}

func (ctx *Context) deletePod(obj interface{}) {
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

	log.Log(log.ShimContext).Debug("removing pod from cache", zap.String("podName", pod.Name))
	ctx.schedulerCache.RemovePod(pod)
}

func (ctx *Context) deleteForeignPod(pod *v1.Pod) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	oldPod, ok := ctx.schedulerCache.GetPod(string(pod.UID))
	if !ok {
		// if pod is not in scheduler cache, no node updates are needed
		log.Log(log.ShimContext).Debug("unknown foreign pod deleted, no resource updated needed",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name))
		return
	}

	// conditions for release:
	//   1. pod is already assigned to a node
	if oldPod != nil {
		log.Log(log.ShimContext).Debug("foreign pod deleted, triggering occupied resource update",
			zap.String("namespace", pod.Namespace),
			zap.String("podName", pod.Name),
			zap.String("podStatusBefore", string(oldPod.Status.Phase)),
			zap.String("podStatusCurrent", string(pod.Status.Phase)))
		// this means pod is terminated
		// we need sub the occupied resource and re-sync with the scheduler-core
		podResource := common.GetPodResource(pod)
		ctx.nodes.updateNodeOccupiedResources(pod.Spec.NodeName, podResource, SubOccupiedResource)
		ctx.schedulerCache.RemovePod(pod)
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
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	log.Log(log.ShimContext).Debug("priority class added")
	priorityClass := utils.Convert2PriorityClass(obj)
	if priorityClass != nil {
		ctx.schedulerCache.AddPriorityClass(priorityClass)
	}
}

func (ctx *Context) updatePriorityClass(_, newObj interface{}) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	log.Log(log.ShimContext).Debug("priority class updated")
	priorityClass := utils.Convert2PriorityClass(newObj)
	if priorityClass != nil {
		ctx.schedulerCache.UpdatePriorityClass(priorityClass)
	}
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
	ctx.lock.Lock()
	defer ctx.lock.Unlock()

	conf := ctx.apiProvider.GetAPIs().GetConf()
	if !conf.EnableConfigHotRefresh {
		log.Log(log.ShimContext).Info("hot-refresh disabled, skipping scheduler configuration update")
		return
	}

	ctx.configMaps[index] = configMap
	err := schedulerconf.UpdateConfigMaps(ctx.configMaps, false)
	if err != nil {
		log.Log(log.ShimContext).Error("Unable to update configmap, ignoring changes", zap.Error(err))
		return
	}

	confMap := schedulerconf.FlattenConfigMaps(ctx.configMaps)

	conf = ctx.apiProvider.GetAPIs().GetConf()
	log.Log(log.ShimContext).Info("reloading scheduler configuration")
	config := utils.GetCoreSchedulerConfigFromConfigMap(confMap)
	extraConfig := utils.GetExtraConfigFromConfigMap(confMap)

	request := &si.UpdateConfigurationRequest{
		RmID:        conf.ClusterID,
		PolicyGroup: conf.PolicyGroup,
		Config:      config,
		ExtraConfig: extraConfig,
	}
	if err := ctx.apiProvider.GetAPIs().SchedulerAPI.UpdateConfiguration(request); err != nil {
		log.Log(log.ShimContext).Error("reload configuration failed", zap.Error(err))
	}
}

// EventsToRegister returns the Kubernetes events that should be watched for updates which may effect predicate processing
func (ctx *Context) EventsToRegister() []framework.ClusterEvent {
	return ctx.predManager.EventsToRegister()
}

// evaluate given predicates based on current context
func (ctx *Context) IsPodFitNode(name, node string, allocate bool) error {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	if pod, ok := ctx.schedulerCache.GetPod(name); ok {
		// if pod exists in cache, try to run predicates
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			// need to lock cache here as predicates need a stable view into the cache
			ctx.schedulerCache.LockForReads()
			defer ctx.schedulerCache.UnlockForReads()
			_, err := ctx.predManager.Predicates(pod, targetNode, allocate)
			return err
		}
	}
	return fmt.Errorf("predicates were not running because pod or node was not found in cache")
}

func (ctx *Context) IsPodFitNodeViaPreemption(name, node string, allocations []string, startIndex int) (index int, ok bool) {
	// assume minimal pods need killing if running in testing mode
	if ctx.apiProvider.IsTestingMode() {
		return startIndex, ok
	}

	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	if pod, ok := ctx.schedulerCache.GetPod(name); ok {
		// if pod exists in cache, try to run predicates
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			// need to lock cache here as predicates need a stable view into the cache
			ctx.schedulerCache.LockForReads()
			defer ctx.schedulerCache.UnlockForReads()

			// look up each victim in the scheduler cache
			victims := make([]*v1.Pod, 0)
			for _, uid := range allocations {
				if victim, ok := ctx.schedulerCache.GetPodNoLock(uid); ok {
					victims = append(victims, victim)
				} else {
					// if pod isn't found, add a placeholder so that the list is still the same size
					victims = append(victims, nil)
				}
			}

			// check predicates for a match
			if index, ok := ctx.predManager.PreemptionPredicates(pod, targetNode, victims, startIndex); ok {
				return index, ok
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
	if assumedPod, exist := ctx.schedulerCache.GetPod(podKey); exist {
		if ctx.schedulerCache.ArePodVolumesAllBound(podKey) {
			log.Log(log.ShimContext).Info("Binding Pod Volumes skipped: all volumes already bound",
				zap.String("podName", pod.Name))
		} else {
			log.Log(log.ShimContext).Info("Binding Pod Volumes", zap.String("podName", pod.Name))

			// retrieve the volume claims
			podVolumeClaims, err := ctx.apiProvider.GetAPIs().VolumeBinder.GetPodVolumeClaims(pod)
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
			volumes, reasons, err := ctx.apiProvider.GetAPIs().VolumeBinder.FindPodVolumes(pod, podVolumeClaims, node)
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
func (ctx *Context) AssumePod(name string, node string) error {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if pod, ok := ctx.schedulerCache.GetPod(name); ok {
		// when add assumed pod, we make a copy of the pod to avoid
		// modifying its original reference. otherwise, it may have
		// race when some other go-routines accessing it in parallel.
		if targetNode := ctx.schedulerCache.GetNode(node); targetNode != nil {
			assumedPod := pod.DeepCopy()
			// assume pod volumes, this will update bindings info in cache
			// assume pod volumes before assuming the pod
			// this will update scheduler cache with essential PV/PVC binding info
			var allBound = true
			// volume builder might be null in UTs
			if ctx.apiProvider.GetAPIs().VolumeBinder != nil {
				var err error
				// retrieve the volume claims
				podVolumeClaims, err := ctx.apiProvider.GetAPIs().VolumeBinder.GetPodVolumeClaims(pod)
				if err != nil {
					log.Log(log.ShimContext).Error("Failed to get pod volume claims",
						zap.String("podName", assumedPod.Name),
						zap.Error(err))
					return err
				}

				// retrieve volumes
				volumes, reasons, err := ctx.apiProvider.GetAPIs().VolumeBinder.FindPodVolumes(pod, podVolumeClaims, targetNode.Node())
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
				allBound, err = ctx.apiProvider.GetAPIs().VolumeBinder.AssumePodVolumes(pod, node, volumes)
				if err != nil {
					return err
				}
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

	if pod, ok := ctx.schedulerCache.GetPod(name); ok {
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

// inform the scheduler that the application is completed,
// the complete state may further explained to completed_with_errors(failed) or successfully_completed,
// either way we need to release all allocations (if exists) for this application
func (ctx *Context) NotifyApplicationComplete(appID string) {
	if app := ctx.GetApplication(appID); app != nil {
		log.Log(log.ShimContext).Debug("NotifyApplicationComplete",
			zap.String("appID", appID),
			zap.String("currentAppState", app.GetApplicationState()))
		ev := NewSimpleApplicationEvent(appID, CompleteApplication)
		dispatcher.Dispatch(ev)
	}
}

func (ctx *Context) NotifyApplicationFail(appID string) {
	if app := ctx.GetApplication(appID); app != nil {
		log.Log(log.ShimContext).Debug("NotifyApplicationFail",
			zap.String("appID", appID),
			zap.String("currentAppState", app.GetApplicationState()))
		ev := NewSimpleApplicationEvent(appID, FailApplication)
		dispatcher.Dispatch(ev)
	}
}

func (ctx *Context) NotifyTaskComplete(appID, taskID string) {
	log.Log(log.ShimContext).Debug("NotifyTaskComplete",
		zap.String("appID", appID),
		zap.String("taskID", taskID))
	if app := ctx.GetApplication(appID); app != nil {
		log.Log(log.ShimContext).Debug("release allocation",
			zap.String("appID", appID),
			zap.String("taskID", taskID))
		ev := NewSimpleTaskEvent(appID, taskID, CompleteTask)
		dispatcher.Dispatch(ev)
		appEv := NewSimpleApplicationEvent(appID, AppTaskCompleted)
		dispatcher.Dispatch(appEv)
	}
}

// update application tags in the AddApplicationRequest based on the namespace annotation
// adds the following tags to the request based on annotations (if exist):
//   - namespace.resourcequota
//   - namespace.parentqueue
func (ctx *Context) updateApplicationTags(request *interfaces.AddApplicationRequest, namespace string) {
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

func (ctx *Context) AddApplication(request *interfaces.AddApplicationRequest) interfaces.ManagedApp {
	log.Log(log.ShimContext).Debug("AddApplication", zap.Any("Request", request))
	if app := ctx.GetApplication(request.Metadata.ApplicationID); app != nil {
		return app
	}

	ctx.lock.Lock()
	defer ctx.lock.Unlock()

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

func (ctx *Context) GetApplication(appID string) interfaces.ManagedApp {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	return ctx.getApplication(appID)
}

func (ctx *Context) getApplication(appID string) interfaces.ManagedApp {
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
func (ctx *Context) AddTask(request *interfaces.AddTaskRequest) interfaces.ManagedTask {
	log.Log(log.ShimContext).Debug("AddTask",
		zap.String("appID", request.Metadata.ApplicationID),
		zap.String("taskID", request.Metadata.TaskID))
	if managedApp := ctx.GetApplication(request.Metadata.ApplicationID); managedApp != nil {
		if app, valid := managedApp.(*Application); valid {
			existingTask, err := app.GetTask(request.Metadata.TaskID)
			if err != nil {
				var originator bool

				// Is this task the originator of the application?
				// If yes, then make it as "first pod/owner/driver" of the application and set the task as originator
				if app.GetOriginatingTask() == nil {
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
	app.removeTask(taskID)
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
	managedTask, err := app.GetTask(taskID)
	if err != nil {
		log.Log(log.ShimContext).Debug("task is not found in applications",
			zap.String("taskID", taskID),
			zap.String("appID", appID))
		return nil
	}
	task, valid := managedTask.(*Task)
	if !valid {
		log.Log(log.ShimContext).Debug("managedTask conversion failed",
			zap.String("taskID", taskID))
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
				appID := record.ObjectID
				taskID := record.ReferenceID
				if task := ctx.getTask(appID, taskID); task != nil {
					events.GetRecorder().Eventf(task.GetTaskPod().DeepCopy(), nil,
						v1.EventTypeNormal, "", "", record.Message)
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
					v1.EventTypeNormal, "", "", record.Message)
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
			// if the pod is skipped because the queue quota has been exceed, we do not trigger the auto-scaling
			task.SetTaskSchedulingState(interfaces.TaskSchedSkipped)
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
			task.SetTaskSchedulingState(interfaces.TaskSchedFailed)
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
			managedApp := ctx.GetApplication(event.GetApplicationID())
			if managedApp == nil {
				log.Log(log.ShimContext).Error("failed to handle application event",
					zap.String("reason", "application not exist"))
				return
			}

			if app, ok := managedApp.(*Application); ok {
				if app.canHandle(event) {
					if err := app.handle(event); err != nil {
						log.Log(log.ShimContext).Error("failed to handle application event",
							zap.String("event", event.GetEvent()),
							zap.Error(err))
					}
				}
			}
		}
	}
}

func (ctx *Context) TaskEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.TaskEvent); ok {
			task := ctx.getTask(event.GetApplicationID(), event.GetTaskID())
			if task == nil {
				log.Log(log.ShimContext).Error("failed to handle application event")
				return
			}
			if task.canHandle(event) {
				if err := task.handle(event); err != nil {
					log.Log(log.ShimContext).Error("failed to handle task event",
						zap.String("applicationID", task.applicationID),
						zap.String("taskID", task.taskID),
						zap.String("event", event.GetEvent()),
						zap.Error(err))
				}
			}
		}
	}
}

func (ctx *Context) SchedulerNodeEventHandler() func(obj interface{}) {
	if ctx != nil && ctx.nodes != nil {
		return ctx.nodes.schedulerNodeEventHandler()
	}
	// this is not required in some tests
	return nil
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
