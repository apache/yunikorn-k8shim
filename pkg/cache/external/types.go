/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

package external

import "k8s.io/kubernetes/pkg/scheduler/factory"

// PluginFactoryArgs are the arguments required for plugin (predicate/priority) functions.
//
// -- Predicates --
// PodLister -> MatchInterPodAffinityPred
// NodeInfo -> MatchInterPodAffinityPred
// PVInfo -> All volume related predicates
// PVCInfo -> All volume related predicates
// StorageClassInfo -> Volume related predicates
// VolumeBinder -> Volume bind predicate
//
// -- Priorities ---
// ServiceLister/ControllerLister/ReplicaSetLister/StatefulSetLister are required by PriorityMetadataProducer
// NodeLister -> InterPodAffinityPriority
//
// see more at how these args are used in predictor.go
var pluginArgs *factory.PluginFactoryArgs

// initially args are empty, these args will be initialized by one or more provides,
// from whom implemented PluginFactoryArgsProvider interface.
func init() {
	pluginArgs = &factory.PluginFactoryArgs{}
}

func GetPluginArgs() *factory.PluginFactoryArgs {
	return pluginArgs
}

// see e.g SchedulerCache
type PluginFactoryArgsProvider interface {
	// a plugin factory args provider implemented one or more interfaces defined in
	// such as PodLister, NodeLister, etc.
	assignArgs(args *factory.PluginFactoryArgs)
}
