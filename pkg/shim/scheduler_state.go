package shim

import (
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

//----------------------------------------------
// scheduler events
//----------------------------------------------

type SchedulerEventType events.SchedulerEventType

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
// scheduler states
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
			"enter_state": func(event *fsm.Event) {
					log.Logger().Debug("scheduler shim state transition",
						zap.String("source", event.Src),
						zap.String("destination", event.Dst),
						zap.String("event", event.Event))
			},
			states.Registered: func(event *fsm.Event) {
					scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
					scheduler.triggerSchedulerStateRecovery()
			},
			states.Recovering: func(event *fsm.Event) {
					scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
					scheduler.recoverSchedulerState()
			},
			states.Running: func(event *fsm.Event) {
					scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
					scheduler.doScheduling()
			},
			RegisterScheduler.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.register()
			},
			RegisterSchedulerFailed.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.handleSchedulerFailure()
			},
			RecoverSchedulerFailed.String(): func(event *fsm.Event) {
				scheduler := event.Args[0].(*KubernetesShim) //nolint:errcheck
				scheduler.handleSchedulerFailure()
			},
		},
	)
}
