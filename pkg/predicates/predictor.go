package predicates

import (
	"fmt"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	deschedulernode "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type Predictor struct {
	fitPredicateMap map[string]predicates.FitPredicate
	mandatoryFitPredicates sets.String
}

func NewPredictor() *Predictor{
	p := &Predictor {
		fitPredicateMap:  make(map[string]predicates.FitPredicate),
		mandatoryFitPredicates: sets.NewString(),
	}
	p.init()
	return p
}

func (p *Predictor) RegisterFitPredicate(name string, predicate predicates.FitPredicate) {
	p.fitPredicateMap[name]=predicate
}

func (p *Predictor) RegisterMandatoryFitPredicate(name string, predicate predicates.FitPredicate) {
	p.fitPredicateMap[name] = predicate
	p.mandatoryFitPredicates.Insert(name)
}

func (p *Predictor) init() {
	p.RegisterFitPredicate("PodFitsPorts", predicates.PodFitsHostPorts)
	p.RegisterFitPredicate(predicates.PodFitsHostPortsPred, predicates.PodFitsHostPorts)
	p.RegisterFitPredicate(predicates.PodFitsResourcesPred, predicates.PodFitsResources)
	p.RegisterFitPredicate(predicates.HostNamePred, predicates.PodFitsHost)
	p.RegisterFitPredicate(predicates.MatchNodeSelectorPred, predicates.PodMatchNodeSelector)
	p.RegisterFitPredicate(predicates.NoDiskConflictPred, predicates.NoDiskConflict)
	p.RegisterFitPredicate(predicates.GeneralPred, predicates.GeneralPredicates)
	p.RegisterFitPredicate(predicates.CheckNodeMemoryPressurePred, predicates.CheckNodeMemoryPressurePredicate)
	p.RegisterFitPredicate(predicates.CheckNodeDiskPressurePred, predicates.CheckNodeDiskPressurePredicate)
	p.RegisterFitPredicate(predicates.CheckNodePIDPressurePred, predicates.CheckNodePIDPressurePredicate)
	p.RegisterMandatoryFitPredicate(predicates.CheckNodeConditionPred, predicates.CheckNodeConditionPredicate)
	p.RegisterFitPredicate(predicates.PodToleratesNodeTaintsPred, predicates.PodToleratesNodeTaints)
}

func (p *Predictor) Predicates(pod *v1.Pod, node *deschedulernode.NodeInfo) error {
	glog.V(4).Infof(">>>> Calling predicates")
	// honor the ordering...
	for _, predicateKey := range predicates.Ordering() {
		var (
			fit     bool
			reasons []predicates.PredicateFailureReason
			err     error
		)
		if predicate, exist := p.fitPredicateMap[predicateKey]; exist {
			// TODO meta???
			fit, reasons, err = predicate(pod, nil, node)
			glog.V(4).Infof(">>>> predicate %s, result %v", predicateKey, fit)
			if err != nil {
				glog.V(0).Infof("evaluating predicate, key=%s, result=%s, reason: %v",
					predicateKey, fit, reasons)
				return err
			}

			if !fit {
				glog.V(0).Infof("evaluating predicate, key=%s, result=%s, reason: %v",
					predicateKey, fit, reasons)
				return fmt.Errorf("predicate %s cannot be satisified, reason %v", predicateKey, reasons)
			}
		}
	}
	return nil
}