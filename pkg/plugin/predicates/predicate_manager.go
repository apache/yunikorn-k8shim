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
	"fmt"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-base/config/v1alpha1"
	"k8s.io/kube-scheduler/config/v1beta2"
	apiConfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/podtopologyspread"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	fwruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	"github.com/apache/yunikorn-core/pkg/log"
)

type PredicateManager interface {
	Predicates(pod *v1.Pod, node *framework.NodeInfo, allocate bool) (plugin string, error error)
}

var _ PredicateManager = &predicateManagerImpl{}

var configDecoder = scheme.Codecs.UniversalDecoder()

type predicateManagerImpl struct {
	reservationPreFilters *[]framework.PreFilterPlugin
	allocationPreFilters  *[]framework.PreFilterPlugin
	reservationFilters    *[]framework.FilterPlugin
	allocationFilters     *[]framework.FilterPlugin
}

func (p *predicateManagerImpl) Predicates(pod *v1.Pod, node *framework.NodeInfo, allocate bool) (plugin string, error error) {
	if allocate {
		return p.predicatesAllocate(pod, node)
	}
	return p.predicatesReserve(pod, node)
}

func (p *predicateManagerImpl) predicatesReserve(pod *v1.Pod, node *framework.NodeInfo) (plugin string, error error) {
	ctx := context.Background()
	state := framework.NewCycleState()
	return p.podFitsNode(ctx, state, *p.reservationPreFilters, *p.reservationFilters, pod, node)
}

func (p *predicateManagerImpl) predicatesAllocate(pod *v1.Pod, node *framework.NodeInfo) (plugin string, error error) {
	ctx := context.Background()
	state := framework.NewCycleState()
	return p.podFitsNode(ctx, state, *p.allocationPreFilters, *p.allocationFilters, pod, node)
}

func (p *predicateManagerImpl) podFitsNode(ctx context.Context, state *framework.CycleState, preFilters []framework.PreFilterPlugin, filters []framework.FilterPlugin, pod *v1.Pod, node *framework.NodeInfo) (plugin string, error error) {
	// Run "prefilter" plugins.
	s, plugin := p.runPreFilterPlugins(ctx, state, preFilters, pod)
	if !s.IsSuccess() {
		return plugin, s.AsError()
	}

	// Run "filter" plugins on node
	statuses, plugin := p.runFilterPlugins(ctx, filters, state, pod, node)
	s = statuses.Merge()
	if !s.IsSuccess() {
		return plugin, s.AsError()
	}
	return "", nil
}

func (p *predicateManagerImpl) runPreFilterPlugins(ctx context.Context, state *framework.CycleState, plugins []framework.PreFilterPlugin, pod *v1.Pod) (status *framework.Status, plugin string) {
	for _, pl := range plugins {
		status = p.runPreFilterPlugin(ctx, pl, state, pod)
		if !status.IsSuccess() {
			if status.IsUnschedulable() {
				return status, plugin
			}
			err := status.AsError()
			log.Logger().Error("failed running PreFilter plugin",
				zap.String("pluginName", pl.Name()),
				zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
				zap.Error(err))
			return framework.AsStatus(fmt.Errorf("running PreFilter plugin %q: %w", pl.Name(), err)), plugin
		}
	}

	return nil, ""
}

func (p *predicateManagerImpl) runPreFilterPlugin(ctx context.Context, pl framework.PreFilterPlugin, state *framework.CycleState, pod *v1.Pod) *framework.Status {
	return pl.PreFilter(ctx, state, pod)
}

func (p *predicateManagerImpl) runFilterPlugins(ctx context.Context, plugins []framework.FilterPlugin, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) (status framework.PluginToStatus, plugin string) {
	statuses := make(framework.PluginToStatus)
	plugin = ""
	for _, pl := range plugins {
		pluginStatus := p.runFilterPlugin(ctx, pl, state, pod, nodeInfo)
		if !pluginStatus.IsSuccess() {
			if plugin == "" {
				plugin = pl.Name()
			}
			if !pluginStatus.IsUnschedulable() {
				// Filter plugins are not supposed to return any status other than
				// Success or Unschedulable.
				errStatus := framework.NewStatus(framework.Error, fmt.Sprintf("running %q filter plugin for pod %q: %v", pl.Name(), pod.Name, pluginStatus.Message()))
				log.Logger().Error("failed running Filter plugin",
					zap.String("pluginName", pl.Name()),
					zap.String("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)),
					zap.String("message", pluginStatus.Message()))
				return map[string]*framework.Status{pl.Name(): errStatus}, pl.Name()
			}
			statuses[pl.Name()] = pluginStatus
		}
	}
	return statuses, plugin
}

func (p *predicateManagerImpl) runFilterPlugin(ctx context.Context, pl framework.FilterPlugin, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	return pl.Filter(ctx, state, pod, nodeInfo)
}

func NewPredicateManager(handle framework.Handle) PredicateManager {
	/*
		Default K8S plugins as of 1.23 that implement PreFilter:
			NodeAffinity
			NodePorts
			NodeResourcesFit
			VolumeRestrictions
			VolumeBinding
			PodTopologySpread
			InterPodAffinity
	*/

	// run only the simpler PreFilter plugins during reservation phase
	reservationPreFilters := map[string]bool{
		nodeaffinity.Name:      true,
		nodeports.Name:         true,
		podtopologyspread.Name: true,
		interpodaffinity.Name:  true,
		// NodeResourcesFit : skip because during reservation, node resources are not enough
		// VolumeRestrictions
		// VolumeBinding
	}

	// run all PreFilter plugins during allocation phase
	allocationPreFilters := map[string]bool{
		"*": true,
	}

	/*
		Default K8S plugins as of 1.23 that implement Filter:
		    NodeUnschedulable
			NodeName
			TaintToleration
			NodeAffinity
			NodePorts
			NodeResourcesFit
			VolumeRestrictions
			EBSLimits
			GCEPDLimits
			NodeVolumeLimits
			AzureDiskLimits
			VolumeBinding
			VolumeZone
			PodTopologySpread
			InterPodAffinity
	*/

	// run only the simpler Filter plugins during reservation phase
	reservationFilters := map[string]bool{
		nodeunschedulable.Name: true,
		nodename.Name:          true,
		tainttoleration.Name:   true,
		nodeaffinity.Name:      true,
		nodeports.Name:         true,
		podtopologyspread.Name: true,
		interpodaffinity.Name:  true,
		// NodeResourcesFit : skip because during reservation, node resources are not enough
		// VolumeRestrictions
		// EBSLimits
		// GCEPDLimits
		// NodeVolumeLimits
		// AzureDiskLimits
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
	pluginRegistry := plugins.NewInTreeRegistry()

	cfg, err := defaultConfig() // latest.Default()
	if err != nil {
		log.Logger().Fatal("Unable to get default predicate config", zap.Error(err))
	}

	profile := cfg.Profiles[0] // first profile is default
	registeredPlugins := profile.Plugins
	createdPlugins := make(map[string]framework.Plugin)
	reservationPreFilterPlugins := &apiConfig.PluginSet{}
	allocationPreFilterPlugins := &apiConfig.PluginSet{}
	reservationFilterPlugins := &apiConfig.PluginSet{}
	allocationFilterPlugins := &apiConfig.PluginSet{}

	addPlugins("PreFilter", &registeredPlugins.PreFilter, reservationPreFilterPlugins, reservationPreFilters)
	addPlugins("PreFilter", &registeredPlugins.PreFilter, allocationPreFilterPlugins, allocationPreFilters)
	addPlugins("Filter", &registeredPlugins.Filter, reservationFilterPlugins, reservationFilters)
	addPlugins("Filter", &registeredPlugins.Filter, allocationFilterPlugins, allocationFilters)

	createPlugins(handle, pluginRegistry, reservationPreFilterPlugins, createdPlugins)
	createPlugins(handle, pluginRegistry, allocationPreFilterPlugins, createdPlugins)
	createPlugins(handle, pluginRegistry, reservationFilterPlugins, createdPlugins)
	createPlugins(handle, pluginRegistry, allocationFilterPlugins, createdPlugins)

	// assign reservation PreFilter plugins
	resPre := make([]framework.PreFilterPlugin, 0)
	for _, v := range reservationPreFilterPlugins.Enabled {
		plugin := createdPlugins[v.Name]
		if plugin == nil {
			log.Logger().Warn("plugin not found", zap.String("pluginName", v.Name))
			continue
		}
		pfPlugin, ok := plugin.(framework.PreFilterPlugin)
		if !ok {
			log.Logger().Warn("plugin does not implement PreFilterPlugin", zap.String("pluginName", v.Name))
			continue
		}
		resPre = append(resPre, pfPlugin)
		log.Logger().Debug("Registered reservation PreFilter plugin", zap.String("pluginName", plugin.Name()))
	}

	// assign allocation PreFilter plugins
	allocPre := make([]framework.PreFilterPlugin, 0)
	for _, v := range allocationPreFilterPlugins.Enabled {
		plugin := createdPlugins[v.Name]
		if plugin == nil {
			log.Logger().Warn("plugin not found", zap.String("pluginName", v.Name))
			continue
		}
		pfPlugin, ok := plugin.(framework.PreFilterPlugin)
		if !ok {
			log.Logger().Warn("plugin does not implement PreFilterPlugin", zap.String("pluginName", v.Name))
			continue
		}
		allocPre = append(allocPre, pfPlugin)
		log.Logger().Debug("Registered allocation PreFilter plugin", zap.String("pluginName", plugin.Name()))
	}

	// assign reservation Filter plugins
	resFilt := make([]framework.FilterPlugin, 0)
	for _, v := range reservationFilterPlugins.Enabled {
		plugin := createdPlugins[v.Name]
		if plugin == nil {
			log.Logger().Warn("plugin not found", zap.String("pluginName", v.Name))
			continue
		}
		fPlugin, ok := plugin.(framework.FilterPlugin)
		if !ok {
			log.Logger().Warn("plugin does not implement FilterPlugin", zap.String("pluginName", v.Name))
			continue
		}
		resFilt = append(resFilt, fPlugin)
		log.Logger().Debug("Registered reservation Filter plugin", zap.String("pluginName", plugin.Name()))
	}

	// assign allocation Filter plugins
	allocFilt := make([]framework.FilterPlugin, 0)
	for _, v := range allocationFilterPlugins.Enabled {
		plugin := createdPlugins[v.Name]
		if plugin == nil {
			log.Logger().Warn("plugin not found", zap.String("pluginName", v.Name))
			continue
		}
		fPlugin, ok := plugin.(framework.FilterPlugin)
		if !ok {
			log.Logger().Warn("plugin does not implement FilterPlugin", zap.String("pluginName", v.Name))
			continue
		}
		allocFilt = append(allocFilt, fPlugin)
		log.Logger().Debug("Registered allocation Filter plugin", zap.String("pluginName", plugin.Name()))
	}

	pm := &predicateManagerImpl{
		reservationPreFilters: &resPre,
		allocationPreFilters:  &allocPre,
		reservationFilters:    &resFilt,
		allocationFilters:     &allocFilt,
	}

	return pm
}

func defaultConfig() (*apiConfig.KubeSchedulerConfiguration, error) {
	versionedCfg := v1beta2.KubeSchedulerConfiguration{}
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
	cfg.TypeMeta.APIVersion = v1beta2.SchemeGroupVersion.String()
	return &cfg, nil
}

func addPlugins(phase string, source *apiConfig.PluginSet, dest *apiConfig.PluginSet, pluginFilter map[string]bool) {
	for _, p := range source.Enabled {
		enabled, ok := pluginFilter[p.Name]
		if !ok {
			enabled, ok = pluginFilter["*"] // check for wildcard
			if !ok {
				continue
			}
		}
		if enabled {
			log.Logger().Debug("adding plugin", zap.String("phase", phase), zap.String("pluginName", p.Name))
			dest.Enabled = append(dest.Enabled, p)
		}
	}
}

func createPlugins(handle framework.Handle, registry fwruntime.Registry, plugins *apiConfig.PluginSet, createdPlugins map[string]framework.Plugin) {
	pluginConfig := make(map[string]runtime.Object)

	for _, p := range plugins.Enabled {
		if _, ok := createdPlugins[p.Name]; ok {
			// already exists
			continue
		}
		cfg, err := getPluginArgsOrDefault(pluginConfig, p.Name)
		if err != nil {
			log.Logger().Error("failed to create plugin config", zap.String("pluginName", p.Name), zap.Error(err))
			continue
		}
		log.Logger().Debug("plugin config created", zap.String("pluginName", p.Name), zap.Any("cfg", cfg))

		factory := registry[p.Name]
		plugin, err := factory(cfg, handle)
		if err != nil {
			log.Logger().Error("failed to create plugin", zap.String("pluginName", p.Name), zap.Error(err))
			continue
		}
		log.Logger().Debug("plugin created", zap.String("pluginName", p.Name))
		createdPlugins[p.Name] = plugin
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
	gvk := v1beta2.SchemeGroupVersion.WithKind(name + "Args")
	obj, _, err := configDecoder.Decode(nil, &gvk, nil)
	if runtime.IsNotRegisteredError(err) {
		// This plugin doesn't require configuration.
		return nil, nil
	}
	return obj, err
}
