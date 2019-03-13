package fsm

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/api"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/unityscheduler/rmproxy"
	"k8s.io/apimachinery/pkg/util/wait"
	"sync"
)

// shim scheduler watches api server and interacts with unity scheduler to allocate pods
type ShimScheduler struct {
	rmProxy *rmproxy.RMProxy
	context *state.Context
	callback api.ResourceManagerCallback
	sm *fsm.FSM
	events *SchedulerEvents
	states *SchedulerStates
	lock *sync.Mutex
}

func NewShimScheduler(p *rmproxy.RMProxy, ctx *state.Context, cb api.ResourceManagerCallback) *ShimScheduler {
	var events = InitiateEvents()
	var states =  InitiateStates()
	ss := &ShimScheduler{
		rmProxy: p,
		context: ctx,
		callback: cb,
		events:  events,
		states: states,
	}

	ss.sm = fsm.NewFSM(
		ss.states.NEW.state,
		fsm.Events{
			{ Name: events.REGISTER.event,
				Src: []string{states.NEW.state},
				Dst: states.REGISTERED.state},
			//{Name: "reject", Src: []string{"new"}, Dst: "rejected"},
		},
		fsm.Callbacks{
			events.REGISTER.event: ss.register(),
		},
	)
	return ss
}

func (ss *ShimScheduler) register() func(e *fsm.Event) {
	return func(e *fsm.Event) {
		if err := ss.registerShimLayer(); err != nil {
			panic(fmt.Sprintf("failed to register to the unity scheduler, error %s", err.Error()))
		}
	}
}

func (ss *ShimScheduler) registerShimLayer() error {
	glog.V(3).Infof("register RM to the scheduler")
	registerMessage := si.RegisterResourceManagerRequest{
		RmId:    common.ClusterId,
		Version: common.ClusterVersion,
		PolicyGroup: common.DefaultPolicyGroup,
	}

	if _, err := ss.rmProxy.RegisterResourceManager(&registerMessage, ss.callback); err != nil {
		return err
	}

	return nil
}

// event handling
func (ss *ShimScheduler) Handle(se SchedulerEvent) error {
	glog.V(4).Infof("ShimScheduler: preState: %s, coming event: %s", ss.sm.Current(), se.event)
	err := ss.sm.Event(se.event)
	glog.V(4).Infof("ShimScheduler: postState: %s, handled event: %s", ss.sm.Current(), se.event)
	return err
}

// each schedule iteration, we scan all jobs and triggers job state transition
func (ss *ShimScheduler) schedule() {
	jobs := ss.context.SelectJobs(nil)
	for _, job := range jobs {
		for _, pendingPod := range job.GetPendingPods() {
			glog.V(3).Infof("schedule job %s pending pod: %s", job.JobId, pendingPod.Name,)
			switch job.GetJobState() {
			case job.States.NEW.Value():
				ss.context.GetJobController().Submit(job)
			case job.States.ACCEPTED.Value():
				ss.context.GetJobController().Run(job)
			case job.States.RUNNING.Value():
				ss.context.GetJobController().Schedule(job, pendingPod)
			case job.States.FINISHED.Value():
				glog.V(4).Infof("job %s is already at finished state", job.JobId)
			case job.States.REJECTED.Value():
				glog.V(4).Infof("ignoring schedule pod %s for job %s, job is rejected",
					pendingPod.UID, job.JobId)
			}
		}
	}

}

func (ss *ShimScheduler) Run(stopChan chan struct{}) {
	// first register to scheduler
	if err := ss.Handle(ss.events.REGISTER); err != nil {
		panic(fmt.Sprintf("state transition failed, error %s", err.Error()))
	}

	ss.context.Run(stopChan)
	go wait.Until(ss.schedule, ss.context.GetSchedulerConf().GetSchedulingInterval(), stopChan)
}

