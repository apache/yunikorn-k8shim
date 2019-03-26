/*
Copyright 2019 The Unity Scheduler Authors

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

package fsm

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
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
	lock *sync.Mutex
}

func NewShimScheduler(p *rmproxy.RMProxy, ctx *state.Context, cb api.ResourceManagerCallback) *ShimScheduler {
	var events = InitiateEvents()
	ss := &ShimScheduler{
		rmProxy: p,
		context: ctx,
		callback: cb,
		events:  events,
	}

	var states = common.States().Scheduler
	ss.sm = fsm.NewFSM(
		states.New,
		fsm.Events{
			{ Name: events.REGISTER.event,
				Src: []string{states.New},
				Dst: states.Registered},
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

func (ss *ShimScheduler) GetSchedulerState() string {
	return ss.sm.Current()
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
		for _, pendingTask := range job.GetPendingTasks() {
			var states = common.States().Job
			glog.V(3).Infof("schedule job %s pending task: %s", job.JobId, pendingTask.GetTaskPod().Name)
			switch job.GetJobState() {
			case states.New:
				job.Submit()
			case states.Accepted:
				job.Run()
			case states.Running:
				job.ScheduleTask(pendingTask)
			case states.Completed:
				job.IgnoreScheduleTask(pendingTask)
			case states.Rejected:
				job.IgnoreScheduleTask(pendingTask)
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

