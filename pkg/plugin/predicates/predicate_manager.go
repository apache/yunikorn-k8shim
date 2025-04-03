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

package predicates

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/config/v1alpha1"
	"k8s.io/klog/v2"
	schedConfig "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/features"
	apiConfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	fwruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"

	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type PredicateManager interface {
	EventsToRegister(queueingHintFn framework.QueueingHintFn) []framework.ClusterEventWithHint
	Predicates(pod *v1.Pod, node *framework.NodeInfo, allocate bool) (plugin string, error error)
	PreemptionPredicates(pod *v1.Pod, node *framework.NodeInfo, victims []*v1.Pod, startIndex int) (index int)
}

var _ PredicateManager = &predicateManagerImpl{}

var configDecoder = scheme.Codecs.UniversalDecoder()

type predicateManagerImpl struct {
	reservationPreFilters *[]framework.PreFilterPlugin
	allocationPreFilters  *[]framework.PreFilterPlugin
	reservationFilters    *[]framework.FilterPlugin
	allocationFilters     *[]framework.FilterPlugin
	klogger               klog.Logger
}

func (p *predicateManagerImpl) EventsToRegister(queueingHintFn framework.QueueingHintFn) []framework.ClusterEventWithHint {
	actionMap := make(map[framework.EventResource]framework.ActionType)
	for _, plugin := range *p.allocationPreFilters {
		mergePluginEvents(actionMap, pluginEvents(plugin))
	}
	for _, plugin := range *p.allocationFilters {
		mergePluginEvents(actionMap, pluginEvents(plugin))
	}
	return buildClusterEvents(actionMap, queueingHintFn)
}

func pluginEvents(plugin framework.Plugin) []framework.ClusterEventWithHint {
	ext, ok := plugin.(framework.EnqueueExtensions)
	if !ok {
		// legacy plugins that don't register for EnqueueExtensions get a default list of events
		return framework.UnrollWildCardResource()
	}
	events, err := ext.EventsToRegister(context.Background())
	if err != nil {
		log.Log(log.ShimPredicates).Fatal("Failed to configure predicate plugin", zap.String("name", ext.Name()), zap.Error(err))
	}
	return events
}

func mergePluginEvents(actionMap map[framework.EventResource]framework.ActionType, events []framework.ClusterEventWithHint) {
	if _, ok := actionMap[framework.WildCard]; ok {
		// already registered for all events; skip further processing
		return
	}
	for _, event := range events {
		if event.Event.IsWildCard() {
			// clear existing entries and add a wildcard entry
			for k := range actionMap {
				delete(actionMap, k)
			}
			actionMap[framework.WildCard] = framework.All
			return
		}
		action, ok := actionMap[event.Event.Resource]
		if !ok {
			action = event.Event.ActionType
		} else {
			action |= event.Event.ActionType
		}
		actionMap[event.Event.Resource] = action
	}
}

func buildClusterEvents(actionMap map[framework.EventResource]framework.ActionType, queueingHintFn framework.QueueingHintFn) []framework.ClusterEventWithHint {
	events := make([]framework.ClusterEventWithHint, 0)
	for resource, actionType := range actionMap {
		events = append(events, framework.ClusterEventWithHint{
			Event: framework.ClusterEvent{
				Resource:   resource,
				ActionType: actionType},
			QueueingHintFn: queueingHintFn,
		})
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Event.Resource < events[j].Event.Resource
	})
	return events
}

func (p *predicateManagerImpl) Predicates(pod *v1.Pod, node *framework.NodeInfo, allocate bool) (plugin string, error error) {
	if allocate {
		return p.predicatesAllocate(pod, node)
	}
	return p.predicatesReserve(pod, node)
}

func (p *predicateManagerImpl) PreemptionPredicates(pod *v1.Pod, node *framework.NodeInfo, victims []*v1.Pod, startIndex int) int {
	ctx := context.Background()
	state := framework.NewCycleState()

	// run prefilter checks as pod cannot be scheduled otherwise
	s, plugin, skip := p.runPreFilterPlugins(ctx, state, *p.allocationPreFilters, pod, node)
	if !s.IsSuccess() && !s.IsSkip() {
		// prefilter check failed, log and return
		log.Log(log.ShimPredicates).Debug("PreFilter check failed during preemption check",
			zap.String("podUID", string(pod.UID)),
			zap.String("plugin", plugin),
			zap.String("message", s.Message()))

		return -1
	}

	// clone node so that we can modify it here for predicate checks
	preemptingNode := node.Snapshot()

	// remove pods up through startIndex -- all of these are required to be removed to satisfy resource constraints
	for i := 0; i < startIndex && i < len(victims); i++ {
		p.removePodFromNodeNoFail(preemptingNode, victims[i])
	}

	// loop through remaining pods
	for i := startIndex; i < len(victims); i++ {
		p.removePodFromNodeNoFail(preemptingNode, victims[i])
		status, _ := p.runFilterPlugins(ctx, *p.allocationFilters, state, pod, preemptingNode, skip)
		if status.IsSuccess() {
			return i
		}
	}

	// no fit, log and return
	log.Log(log.ShimPredicates).Debug("Filter checks failed during preemption check, no fit",
		zap.String("podUID", string(pod.UID)),
		zap.String("nodeID", node.Node().Name))
	return -1
}

func (p *predicateManagerImpl) removePodFromNodeNoFail(node *framework.NodeInfo, pod *v1.Pod) {
	if pod == nil {
		return
	}
	if err := node.RemovePod(p.klogger, pod); err != nil {
		// annoyingly, RemovePod() throws an error if the pod is gone; just log at debug and continue
		log.Log(log.ShimPredicates).Debug("Failed to remove pod from nodeInfo during preemption check",
			zap.String("podUID", string(pod.UID)),
			zap.String("nodeID", node.Node().Name),
			zap.Error(err))
	}
}

func (p *predicateManagerImpl) predicatesReserve(pod *v1.Pod, node *framework.NodeInfo) (string, error) {
	ctx := context.Background()
	state := framework.NewCycleState()
	return p.podFitsNode(ctx, state, *p.reservationPreFilters, *p.reservationFilters, pod, node)
}

func (p *predicateManagerImpl) predicatesAllocate(pod *v1.Pod, node *framework.NodeInfo) (string, error) {
	ctx := context.Background()
	state := framework.NewCycleState()
	return p.podFitsNode(ctx, state, *p.allocationPreFilters, *p.allocationFilters, pod, node)
}

func (p *predicateManagerImpl) podFitsNode(ctx context.Context, state *framework.CycleState, preFilters []framework.PreFilterPlugin, filters []framework.FilterPlugin, pod *v1.Pod, node *framework.NodeInfo) (string, error) {
	// Run "prefilter" plugins.
	status, plugin, skip := p.runPreFilterPlugins(ctx, state, preFilters, pod, node)
	if !status.IsSuccess() && !status.IsSkip() {
		return plugin, errors.New(status.Message())
	}

	// Run "filter" plugins on node
	status, plugin = p.runFilterPlugins(ctx, filters, state, pod, node, skip)
	if !status.IsSuccess() {
		return plugin, errors.New(status.Message())
	}
	return "", nil
}

func (p *predicateManagerImpl) runPreFilterPlugins(ctx context.Context, state *framework.CycleState, plugins []framework.PreFilterPlugin, pod *v1.Pod, node *framework.NodeInfo) (*framework.Status, string, map[string]bool) {
	var mergedNodes *framework.PreFilterResult
	skip := make(map[string]bool)
	for _, pl := range plugins {
		plugin := pl.Name()
		nodes, status := p.runPreFilterPlugin(ctx, pl, state, pod)
		if status.IsSkip() {
			skip[plugin] = true
		} else if !status.IsSuccess() {
			if status.IsRejected() {
				return status, "", skip
			}
			err := errors.New(status.Message())
			log.Log(log.ShimPredicates).Error("failed running PreFilter plugin",
				zap.String("pluginName", plugin),
				zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
				zap.Error(err))
			return framework.AsStatus(errors.Join(fmt.Errorf("running PreFilter plugin %q: ", plugin), err)), plugin, skip
		}
		// Merge is nil safe and returns a new PreFilterResult result if mergedNodes was nil
		mergedNodes = mergedNodes.Merge(nodes)
		if !mergedNodes.AllNodes() && !mergedNodes.NodeNames.Has(node.Node().Name) {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, "node not eligible"), plugin, skip
		}
	}

	return nil, "", skip
}

func (p *predicateManagerImpl) runPreFilterPlugin(ctx context.Context, pl framework.PreFilterPlugin, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	return pl.PreFilter(ctx, state, pod)
}

func (p *predicateManagerImpl) runFilterPlugins(ctx context.Context, plugins []framework.FilterPlugin, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo, skip map[string]bool) (*framework.Status, string) {
	for _, pl := range plugins {
		plugin := pl.Name()
		// skip plugin if prefilter returned skip
		if skip[plugin] {
			continue
		}
		status := p.runFilterPlugin(ctx, pl, state, pod, nodeInfo)
		if !status.IsSuccess() {
			if !status.IsRejected() {
				// Filter plugins are not supposed to return any status other than
				// Success or Unschedulable.
				status = framework.NewStatus(framework.Error, fmt.Sprintf("running %q filter plugin for pod %q: %v", plugin, pod.Name, status.Message()))
				log.Log(log.ShimPredicates).Error("failed running Filter plugin",
					zap.String("pluginName", plugin),
					zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
					zap.String("message", status.Message()))
				return status, plugin
			}
			return status, plugin
		}
	}
	return framework.NewStatus(framework.Success), ""
}

func (p *predicateManagerImpl) runFilterPlugin(ctx context.Context, pl framework.FilterPlugin, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	return pl.Filter(ctx, state, pod, nodeInfo)
}

// EnableOptionalKubernetesFeatureGates ensures that any optional Kubernetes feature gates that YuniKorn supports are
// enabled. Currently, as of Kubernetes 1.32, this includes PodLevelResources and InPlacePodVerticalScaling. These are
// both safe to enable as part of our default configuration, as they also require the appropriate feature gates to be
// enabled at the API server to be functional.
// This needs to be called before any Kubernetes-specific code is initialized (ideally top of main()) and in any unit
// tests which require this functionality.
func EnableOptionalKubernetesFeatureGates() {
	log.Log(log.ShimPredicates).Debug("Enabling PodLevelResources feature gate")
	if err := feature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%s=true", features.PodLevelResources)); err != nil {
		log.Log(log.ShimPredicates).Fatal("Unable to set PodLevelResources feature gate", zap.Error(err))
	}
	log.Log(log.ShimPredicates).Debug("Enabling InPlacePodVerticalScaling feature gate")
	if err := feature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%s=true", features.InPlacePodVerticalScaling)); err != nil {
		log.Log(log.ShimPredicates).Fatal("Unable to set InPlacePodVerticalScaling feature gate", zap.Error(err))
	}
	log.Log(log.ShimPredicates).Debug("Enabling InPlacePodVerticalScalingAllocatedStatus feature gate")
	if err := feature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%s=true", features.InPlacePodVerticalScalingAllocatedStatus)); err != nil {
		log.Log(log.ShimPredicates).Fatal("Unable to set InPlacePodVerticalScalingAllocatedStatus feature gate", zap.Error(err))
	}
}

func NewPredicateManager(handle framework.Handle) PredicateManager {
	/*
		Default K8S plugins as of 1.32 that implement PreFilter:
			NodeAffinity
			NodePorts
			NodeResourcesFit
			VolumeRestrictions
			NodeVolumeLimits
			VolumeBinding
			VolumeZone
			PodTopologySpread
			InterPodAffinity
	*/

	// run only the simpler PreFilter plugins during reservation phase
	reservationPreFilters := map[string]bool{
		names.NodeAffinity:      true,
		names.NodePorts:         true,
		names.PodTopologySpread: true,
		names.InterPodAffinity:  true,
		// NodeResourcesFit : skip because during reservation, node resources are not enough
		// NodeVolumeLimits
		// VolumeRestrictions
		// VolumeBinding
		// VolumeZone
	}

	// run all PreFilter plugins during allocation phase
	allocationPreFilters := map[string]bool{
		"*": true,
	}

	/*
		Default K8S plugins as of 1.32 that implement Filter:
		    NodeUnschedulable
			NodeName
			TaintToleration
			NodeAffinity
			NodePorts
			NodeResourcesFit
			VolumeRestrictions
			NodeVolumeLimits
			VolumeBinding
			VolumeZone
			PodTopologySpread
			InterPodAffinity
	*/

	// run only the simpler Filter plugins during reservation phase
	reservationFilters := map[string]bool{
		names.NodeUnschedulable: true,
		names.NodeName:          true,
		names.TaintToleration:   true,
		names.NodeAffinity:      true,
		names.NodePorts:         true,
		names.PodTopologySpread: true,
		names.InterPodAffinity:  true,
		// NodeResourcesFit : skip because during reservation, node resources are not enough
		// VolumeRestrictions
		// NodeVolumeLimits
		// VolumeBinding
		// VolumeZone
	}

	// run all Filter plugins during allocation phase
	allocationFilters := map[string]bool{
		"*": true,
	}

	return newPredicateManagerInternal(handle, reservationPreFilters, allocationPreFilters, reservationFilters, allocationFilters)
}

func newPredicateManagerInternal(
	handle framework.Handle,
	reservationPreFilters map[string]bool,
	allocationPreFilters map[string]bool,
	reservationFilters map[string]bool,
	allocationFilters map[string]bool) *predicateManagerImpl {
	// ensure K8s scheduler metrics have been initialized in YK standalone mode to avoid SIGSEGV
	if metrics.Goroutines == nil {
		metrics.InitMetrics()
	}

	pluginRegistry := plugins.NewInTreeRegistry()

	cfg, err := defaultConfig() // latest.Default()
	if err != nil {
		log.Log(log.ShimPredicates).Fatal("Unable to get default predicate config", zap.Error(err))
	}

	profile := cfg.Profiles[0] // first profile is default
	registeredPlugins := profile.Plugins
	createdPlugins := make([]framework.Plugin, 0)

	// As of SchedulerConfiguration v1, all plugins implement MultiPoint, therefore we need to instantiate each one and
	// check to see what interfaces it implements dynamically
	createPlugins(handle, pluginRegistry, &registeredPlugins.MultiPoint, &createdPlugins)

	resPre := make([]framework.Plugin, 0)
	allocPre := make([]framework.Plugin, 0)
	resFilt := make([]framework.Plugin, 0)
	allocFilt := make([]framework.Plugin, 0)

	addPlugins("PreFilter", createdPlugins, &resPre, reservationPreFilters)
	addPlugins("PreFilter", createdPlugins, &allocPre, allocationPreFilters)
	addPlugins("Filter", createdPlugins, &resFilt, reservationFilters)
	addPlugins("Filter", createdPlugins, &allocFilt, allocationFilters)

	pm := &predicateManagerImpl{
		reservationPreFilters: preFilterPlugins(resPre),
		allocationPreFilters:  preFilterPlugins(allocPre),
		reservationFilters:    filterPlugins(resFilt),
		allocationFilters:     filterPlugins(allocFilt),
		klogger:               klog.NewKlogr(),
	}

	return pm
}

func preFilterPlugins(plugins []framework.Plugin) *[]framework.PreFilterPlugin {
	result := make([]framework.PreFilterPlugin, 0)
	for _, plugin := range plugins {
		prefilter, ok := plugin.(framework.PreFilterPlugin)
		if ok {
			result = append(result, prefilter)
		}
	}
	return &result
}

func filterPlugins(plugins []framework.Plugin) *[]framework.FilterPlugin {
	result := make([]framework.FilterPlugin, 0)
	for _, plugin := range plugins {
		filter, ok := plugin.(framework.FilterPlugin)
		if ok {
			result = append(result, filter)
		}
	}
	return &result
}

func defaultConfig() (*apiConfig.KubeSchedulerConfiguration, error) {
	versionedCfg := schedConfig.KubeSchedulerConfiguration{}
	versionedCfg.DebuggingConfiguration = *v1alpha1.NewRecommendedDebuggingConfiguration()

	scheme.Scheme.Default(&versionedCfg)
	cfg := apiConfig.KubeSchedulerConfiguration{}
	if err := scheme.Scheme.Convert(&versionedCfg, &cfg, nil); err != nil {
		return nil, err
	}

	// We don't set this field in pkg/scheduler/apis/config/{version}/conversion.go
	// because the field will be cleared later by API machinery during
	// conversion. See KubeSchedulerConfiguration internal type definition for
	// more details.
	cfg.TypeMeta.APIVersion = schedConfig.SchemeGroupVersion.String()

	// Disable some plugins we don't want for YuniKorn
	removePlugin(&cfg, names.DefaultPreemption) // we do our own preemption algorithm
	removePlugin(&cfg, names.SchedulingGates)   // we want PreEnqueue, but not the default SchedulingGates behavior

	return &cfg, nil
}

func removePlugin(config *apiConfig.KubeSchedulerConfiguration, name string) {
	for _, profile := range config.Profiles {
		enabled := make([]apiConfig.Plugin, 0)

		for _, plugin := range profile.Plugins.MultiPoint.Enabled {
			if plugin.Name == name {
				profile.Plugins.MultiPoint.Disabled = append(profile.Plugins.MultiPoint.Disabled, plugin)
			} else {
				enabled = append(enabled, plugin)
			}
		}
		profile.Plugins.MultiPoint.Enabled = enabled
	}
}

func addPlugins(phase string, createdPlugins []framework.Plugin, pluginList *[]framework.Plugin, pluginFilter map[string]bool) {
	for _, plugin := range createdPlugins {
		name := plugin.Name()
		enabled, ok := pluginFilter[name]
		if !ok {
			enabled, ok = pluginFilter["*"] // check for wildcard
			if !ok {
				continue
			}
		}
		if enabled {
			log.Log(log.ShimPredicates).Debug("adding plugin", zap.String("phase", phase), zap.String("pluginName", name))
			*pluginList = append(*pluginList, plugin)
		}
	}
}

func createPlugins(handle framework.Handle, registry fwruntime.Registry, plugins *apiConfig.PluginSet, createdPlugins *[]framework.Plugin) {
	pluginConfig := make(map[string]runtime.Object)

	for _, p := range plugins.Enabled {
		cfg, err := getPluginArgsOrDefault(pluginConfig, p.Name)
		if err != nil {
			log.Log(log.ShimPredicates).Error("failed to create plugin config", zap.String("pluginName", p.Name), zap.Error(err))
			continue
		}
		log.Log(log.ShimPredicates).Debug("plugin config created", zap.String("pluginName", p.Name), zap.Any("cfg", cfg))

		factory := registry[p.Name]
		plugin, err := factory(context.Background(), cfg, handle)
		if err != nil {
			log.Log(log.ShimPredicates).Error("failed to create plugin", zap.String("pluginName", p.Name), zap.Error(err))
			continue
		}
		log.Log(log.ShimPredicates).Debug("plugin created", zap.String("pluginName", p.Name))
		*createdPlugins = append(*createdPlugins, plugin)
	}
}

// getPluginArgsOrDefault returns a configuration provided by the user or builds
// a default from the scheme. Returns `nil, nil` if the plugin does not have a
// defined arg types, such as in-tree plugins that don't require configuration
// or out-of-tree plugins.
func getPluginArgsOrDefault(pluginConfig map[string]runtime.Object, name string) (runtime.Object, error) {
	res, ok := pluginConfig[name]
	if ok {
		return res, nil
	}
	// Use defaults from latest config API version.
	gvk := schedConfig.SchemeGroupVersion.WithKind(name + "Args")
	obj, _, err := configDecoder.Decode(nil, &gvk, nil)
	if runtime.IsNotRegisteredError(err) {
		// This plugin doesn't require configuration.
		return nil, nil
	}
	return obj, err
}
