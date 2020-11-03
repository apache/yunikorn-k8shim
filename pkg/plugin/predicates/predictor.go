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
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/factory"
	deschedulernode "k8s.io/kubernetes/pkg/scheduler/nodeinfo"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

// this policy defines a configurable set of supported predicates.
// this should be configurable, and can be dumped as part of the
// scheduler configuration, which can be used to explicitly advertise
// what are supported.
var defaultSchedulerPolicy = schedulerapi.Policy{
	Predicates: []schedulerapi.PredicatePolicy{
		{Name: predicates.NoVolumeZoneConflictPred},
		{Name: predicates.MaxCSIVolumeCountPred},
		{Name: predicates.MatchInterPodAffinityPred},
		{Name: predicates.NoDiskConflictPred},
		{Name: predicates.PodToleratesNodeTaintsPred},
		{Name: predicates.CheckNodeUnschedulablePred},
		// If replacing the default scheduler you must have the volume predicate included:
		// https://docs.okd.io/latest/admin_guide/scheduling/scheduler.html#static-predicates
		{Name: predicates.CheckVolumeBindingPred},
		//PodFitsResourcesPred, PodFitsHostPortsPred, MatchNodeSelectorPred and HostNamePred forms the GeneralPredicate
		{Name: predicates.PodFitsResourcesPred},
		{Name: predicates.HostNamePred},
		{Name: predicates.PodFitsHostPortsPred},
		{Name: predicates.MatchNodeSelectorPred},
	},
}

// This is a hardcoded list of predicates to run when checking reservation proposals.
// This list must be a subset of the default policy or the configured policy.
// It should not include resources used as the pod is just reserved which means the node is
// out of resources. At least one pod should stop before we can proceed with allocation and
// that could change the resources used (including disk mount, pids, ports etc)
var reservationPredicates = []string{
	predicates.PodToleratesNodeTaintsPred, // taint check
	predicates.MatchInterPodAffinityPred,  // affinity check
	predicates.CheckNodeUnschedulablePred, // unschedulable node are filtered
	predicates.MatchNodeSelectorPred,      // node selector check
}

type Predictor struct {
	fitPredicateMap              map[string]factory.FitPredicateFactory
	fitPredicateFunctions        map[string]predicates.FitPredicate
	predicateMetaProducerFactory factory.PredicateMetadataProducerFactory
	predicateMetaProducer        predicates.PredicateMetadataProducer
	mandatoryFitPredicates       sets.String
	schedulerPolicy              schedulerapi.Policy

	sync.RWMutex
}

func NewPredictor(args *factory.PluginFactoryArgs, testMode bool) *Predictor {
	if testMode {
		// in test mode, disable all the predicates
		return newPredictorInternal(args, schedulerapi.Policy{
			Predicates: []schedulerapi.PredicatePolicy{},
		})
	}
	schedulerPolicy, err := parseConfiguredSchedulerPolicy()
	if err != nil {
		log.Logger().Fatal(err.Error())
	}
	if schedulerPolicy == nil {
		schedulerPolicy = &defaultSchedulerPolicy
	}
	return newPredictorInternal(args, *schedulerPolicy)
}

func newPredictorInternal(args *factory.PluginFactoryArgs, schedulerPolicy schedulerapi.Policy) *Predictor {
	p := &Predictor{
		fitPredicateMap:        make(map[string]factory.FitPredicateFactory),
		fitPredicateFunctions:  make(map[string]predicates.FitPredicate),
		mandatoryFitPredicates: sets.NewString(),
		schedulerPolicy:        schedulerPolicy,
	}
	// init all predicates
	p.init()
	// generate predicate functions
	p.populatePredicateFunc(*args)
	// generate predicate meta producer
	p.populatePredicateMetaProducer(*args)
	return p
}

// a complete list of all supported predicates,
// see more at "kubernetes/pkg/scheduler/algorithmprovider/defaults/register_predicates.go"
func (p *Predictor) init() {
	// Register functions that extract metadata used by predicates computations.
	p.registerPredicateMetadataProducerFactory(
		func(args factory.PluginFactoryArgs) predicates.PredicateMetadataProducer {
			return predicates.NewPredicateMetadataFactory(args.PodLister)
		})

	// IMPORTANT NOTES for predicate developers:
	// Registers predicates and priorities that are not enabled by default, but user can pick when creating their
	// own set of priorities/predicates.

	// PodFitsPorts has been replaced by PodFitsHostPorts for better user understanding.
	// For backwards compatibility with 1.0, PodFitsPorts is registered as well.
	p.registerFitPredicate("PodFitsPorts", predicates.PodFitsHostPorts)
	// Fit is defined based on the absence of port conflicts.
	// This predicate is actually a default predicate, because it is invoked from
	// predicates.GeneralPredicates()
	p.registerFitPredicate(predicates.PodFitsHostPortsPred, predicates.PodFitsHostPorts)
	// Fit is determined by resource availability.
	// This predicate is actually a default predicate, because it is invoked from
	// predicates.GeneralPredicates()
	p.registerFitPredicate(predicates.PodFitsResourcesPred, predicates.PodFitsResources)
	// Fit is determined by the presence of the Host parameter and a string match
	// This predicate is actually a default predicate, because it is invoked from
	// predicates.GeneralPredicates()
	p.registerFitPredicate(predicates.HostNamePred, predicates.PodFitsHost)
	// Fit is determined by node selector query.
	p.registerFitPredicate(predicates.MatchNodeSelectorPred, predicates.PodMatchNodeSelector)

	// Fit is determined by volume zone requirements.
	p.registerFitPredicateFactory(
		predicates.NoVolumeZoneConflictPred,
		func(args factory.PluginFactoryArgs) predicates.FitPredicate {
			return predicates.NewVolumeZonePredicate(args.PVInfo, args.PVCInfo, args.StorageClassInfo)
		},
	)

	// Fit is determined by inter-pod affinity.
	p.registerFitPredicateFactory(
		predicates.MatchInterPodAffinityPred,
		func(args factory.PluginFactoryArgs) predicates.FitPredicate {
			return predicates.NewPodAffinityPredicate(args.NodeInfo, args.PodLister)
		},
	)

	// Fit is determined by non-conflicting disk volumes.
	p.registerFitPredicate(predicates.NoDiskConflictPred, predicates.NoDiskConflict)

	// GeneralPredicates are the predicates that are enforced by all Kubernetes components
	// (e.g. kubelet and all schedulers)
	p.registerFitPredicate(predicates.GeneralPred, predicates.GeneralPredicates)

	// Fit is determined based on whether a pod can tolerate all of the node's taints
	p.registerMandatoryFitPredicate(predicates.PodToleratesNodeTaintsPred, predicates.PodToleratesNodeTaints)

	// Fit is determined based on whether a pod can tolerate unschedulable of node
	p.registerMandatoryFitPredicate(predicates.CheckNodeUnschedulablePred, predicates.CheckNodeUnschedulablePredicate)

	// Fit is determined by volume topology requirements.
	p.registerFitPredicateFactory(
		predicates.CheckVolumeBindingPred,
		func(args factory.PluginFactoryArgs) predicates.FitPredicate {
			return predicates.NewVolumeBindingPredicate(args.VolumeBinder)
		},
	)
}

// From: k8s.io/kubernetes/pkg/scheduler/factory/plugins.go
// RegisterFitPredicate registers a fit predicate with the algorithm
// registry. Returns the name with which the predicate was registered.
func (p *Predictor) registerFitPredicate(name string, predicate predicates.FitPredicate) string {
	return p.registerFitPredicateFactory(name,
		func(factory.PluginFactoryArgs) predicates.FitPredicate { return predicate })
}

// From: k8s.io/kubernetes/pkg/scheduler/factory/plugins.go
// RegisterMandatoryFitPredicate registers a fit predicate with the algorithm registry, the predicate is used by
// kubelet, DaemonSet; it is always included in configuration. Returns the name with which the predicate was
// registered.
func (p *Predictor) registerMandatoryFitPredicate(name string, predicate predicates.FitPredicate) string {
	p.Lock()
	defer p.Unlock()
	p.fitPredicateMap[name] = func(factory.PluginFactoryArgs) predicates.FitPredicate { return predicate }
	p.mandatoryFitPredicates.Insert(name)
	return name
}

// From: k8s.io/kubernetes/pkg/scheduler/factory/plugins.go
// RegisterFitPredicateFactory registers a fit predicate factory with the
// algorithm registry. Returns the name with which the predicate was registered.
func (p *Predictor) registerFitPredicateFactory(name string, predicateFactory factory.FitPredicateFactory) string {
	p.Lock()
	defer p.Unlock()
	p.fitPredicateMap[name] = predicateFactory
	return name
}

// RegisterPredicateMetadataProducerFactory registers a PredicateMetadataProducerFactory.
func (p *Predictor) registerPredicateMetadataProducerFactory(factory factory.PredicateMetadataProducerFactory) {
	p.Lock()
	defer p.Unlock()
	p.predicateMetaProducerFactory = factory
}

func (p *Predictor) populatePredicateFunc(args factory.PluginFactoryArgs) {
	for _, predicate := range p.schedulerPolicy.Predicates {
		if preFactory, ok := p.fitPredicateMap[predicate.Name]; ok {
			p.fitPredicateFunctions[predicate.Name] = preFactory(args)
		}
	}
}

func (p *Predictor) populatePredicateMetaProducer(args factory.PluginFactoryArgs) {
	if p.predicateMetaProducerFactory != nil {
		p.predicateMetaProducer = p.predicateMetaProducerFactory(args)
	}
}

func (p *Predictor) GetPredicateMeta(pod *v1.Pod,
	nodeNameToInfo map[string]*deschedulernode.NodeInfo) predicates.PredicateMetadata {
	return p.predicateMetaProducer(pod, nodeNameToInfo)
}

func (p *Predictor) Enabled() bool {
	return len(p.schedulerPolicy.Predicates) > 0
}

func (p *Predictor) Predicates(pod *v1.Pod, meta predicates.PredicateMetadata, node *deschedulernode.NodeInfo, allocate bool) error {
	if allocate {
		return p.predicatesAllocate(pod, meta, node)
	}
	return p.predicatesReserve(pod, meta, node)
}

func (p *Predictor) predicatesReserve(pod *v1.Pod, meta predicates.PredicateMetadata, node *deschedulernode.NodeInfo) error {
	for _, predicateKey := range reservationPredicates {
		if predicateFn, exist := p.fitPredicateFunctions[predicateKey]; exist {
			fit, reasons, err := predicateFn(pod, meta, node)
			if !fit {
				err = fmt.Errorf("predicate %s fit failed, reason(s) %v", predicateKey, reasons)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Predictor) predicatesAllocate(pod *v1.Pod, meta predicates.PredicateMetadata, node *deschedulernode.NodeInfo) error {
	// honor the ordering...
	for _, predicateKey := range predicates.Ordering() {
		if predicateFn, exist := p.fitPredicateFunctions[predicateKey]; exist {
			fit, reasons, err := predicateFn(pod, meta, node)
			if err != nil {
				events.GetRecorder().Eventf(pod, v1.EventTypeWarning,
					"FailedScheduling", "predicate is not satisfied, error: %s", err.Error())
				return err
			}

			if !fit {
				events.GetRecorder().Eventf(pod, v1.EventTypeWarning,
					"FailedScheduling", "%v", reasons)
				return fmt.Errorf("predicate %s cannot be satisfied, reason: %v", predicateKey, reasons)
			}
		}
	}
	return nil
}

// parse configured scheduler policy from scheduler conf, currently only depends on predicates.
func parseConfiguredSchedulerPolicy() (*schedulerapi.Policy, error) {
	configuredPredicates := conf.GetSchedulerConf().Predicates
	if configuredPredicates != "" {
		validPredicates := make(map[string]bool)
		for _, validPredicate := range predicates.Ordering() {
			validPredicates[validPredicate] = true
		}
		parsedPredicates := strings.Split(configuredPredicates, ",")
		predicatePolicies := make([]schedulerapi.PredicatePolicy, len(parsedPredicates))
		// validate parsed predicates and update predicate policies
		for i, parsedPredicate := range parsedPredicates {
			if _, ok := validPredicates[parsedPredicate]; ok {
				predicatePolicies[i] = schedulerapi.PredicatePolicy{Name: parsedPredicate}
			} else {
				// return error if there's invalid predicate
				return nil, fmt.Errorf("configured predicate '%s' is invalid, valid predicates are: %v",
					parsedPredicate, predicates.Ordering())
			}
		}
		log.Logger().Info("use configured predicates",
			zap.Any("predicates", predicatePolicies))
		return &schedulerapi.Policy{Predicates: predicatePolicies}, nil
	}
	return nil, nil
}
