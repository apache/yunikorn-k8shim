package shim

import (
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

//----------------------------------------------
// Scheduler events
//----------------------------------------------
type SchedulerEventType int

const (
	RegisterScheduler SchedulerEventType = iota
	RegisterSchedulerSucceed
	RegisterSchedulerFailed
	RecoverScheduler
	RecoverSchedulerSucceed
	RecoverSchedulerFailed
)

func (ae SchedulerEventType) String() string {
	return [...]string{"RegisterScheduler", "RegisterSchedulerSucceed", "RegisterSchedulerFailed", "RecoverScheduler", "RecoverSchedulerSucceed", "RecoverSchedulerFailed"}[ae]
}

type ShimSchedulerEvent struct {
	event SchedulerEventType
}

func (rs ShimSchedulerEvent) GetEvent() string {
	return rs.event.String()
}

func (rs ShimSchedulerEvent) GetArgs() []interface{} {
	return nil
}

// -------------------------------------------------------------------
// event to trigger scheduler registration
// --------------------------------------------------------------------
type RegisterSchedulerEvent struct {
	event SchedulerEventType
}

func newRegisterSchedulerEvent() RegisterSchedulerEvent {
	return RegisterSchedulerEvent{
		event: RegisterScheduler,
	}
}

func (rs RegisterSchedulerEvent) GetEvent() string {
	return rs.event.String()
}

func (rs RegisterSchedulerEvent) GetArgs() []interface{} {
	return nil
}

// ----------------------------------
// Scheduler states
// ----------------------------------
var storeScheduleStates *schedulerStates

type schedulerStates struct {
	New         string
	Registered  string
	Registering string
	Recovering  string
	Running     string
	Draining    string
	Stopped     string
}

func SchedulerStates() *schedulerStates {
	if storeScheduleStates == nil {
		storeScheduleStates = &schedulerStates{
			New:         "New",
			Registered:  "Registered",
			Registering: "Registering",
			Recovering:  "Recovering",
			Running:     "Running",
			Draining:    "Draining",
			Stopped:     "Stopped",
		}
	}
	return storeScheduleStates
}

func newSchedulerState() *fsm.FSM {
	states := SchedulerStates()
	return fsm.NewFSM(
		states.New, fsm.Events{
			{
				Name: RegisterScheduler.String(),
				Src:  []string{states.New},
				Dst:  states.Registering,
			},
			{
				Name: RegisterSchedulerSucceed.String(),
				Src:  []string{states.Registering},
				Dst:  states.Registered,
			},
			{
				Name: RegisterSchedulerFailed.String(),
				Src:  []string{states.Registering},
				Dst:  states.Stopped,
			},
			{
				Name: RecoverScheduler.String(),
				Src:  []string{states.Registered},
				Dst:  states.Recovering,
			},
			{
				Name: RecoverSchedulerSucceed.String(),
				Src:  []string{states.Recovering},
				Dst:  states.Running,
			},
			{
				Name: RecoverSchedulerFailed.String(),
				Src:  []string{states.Recovering},
				Dst:  states.Stopped,
			},
		},
		fsm.Callbacks{
			events.EnterState: func(event *fsm.Event) {
				log.Logger().Debug("scheduler shim state transition",
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
			},
			states.Registered: func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.triggerSchedulerStateRecovery()    // if reaches registered, trigger recovering
			},
			states.Recovering: func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.recoverSchedulerState()            // do recovering
			},
			states.Running: func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.doScheduling()                     // do scheduling
			},
			RegisterScheduler.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.register()                         // trigger registration
			},
			RegisterSchedulerFailed.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.handleSchedulerFailure()           // registration failed, stop the scheduler
			},
			RecoverSchedulerFailed.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.handleSchedulerFailure()           // recovery failed
			},
		},
	)
}
