package cache

import (
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

//----------------------------------------------
// Application events
//----------------------------------------------
type applicationEvent int

const (
	SubmitApplication applicationEvent = iota
	RecoverApplication
	AcceptApplication
	TryReserve
	UpdateReservation
	RunApplication
	RejectApplication
	CompleteApplication
	FailApplication
	KillApplication
	KilledApplication
	ReleaseAppAllocation
	ReleaseAppAllocationAsk
	AppStateChange
	ResumingApplication
	AppTaskCompleted
)

func (ae applicationEvent) String() string {
	return [...]string{"SubmitApplication", "RecoverApplication", "AcceptApplication", "TryReserve", "UpdateReservation", "RunApplication", "RejectApplication",
		"CompleteApplication", "FailApplication", "KillApplication", "KilledApplication", "ReleaseAppAllocation", "ReleaseAppAllocationAsk", "AppStateChange", "ResumingApplication", "AppTaskCompleted"}[ae]
}

// ----------------------------------
// application states
// ----------------------------------
type applicationState int

const (
	New applicationState = iota
	Recovering
	Submitted
	Accepted
	Reserving
	Running
	Rejected
	Completed
	Killing
	Killed
	Failing
	Failed
	Resuming
)

func (as applicationState) String() string {
	return [...]string{"New", "Recovering", "Submitted", "Accepted", "Reserving", "Running", "Rejected", "Completed", "Killing", "Killed", "Failing", "Failed", "Resuming"}[as]
}

func NewAppState() *fsm.FSM {
	return fsm.NewFSM(
		New.String(), fsm.Events{
			{
				Name: SubmitApplication.String(),
				Src:  []string{New.String()},
				Dst:  Submitted.String(),
			},
			{
				Name: RecoverApplication.String(),
				Src:  []string{New.String()},
				Dst:  Recovering.String(),
			},
			{
				Name: AcceptApplication.String(),
				Src:  []string{Submitted.String(), Recovering.String()},
				Dst:  Accepted.String(),
			},
			{
				Name: TryReserve.String(),
				Src:  []string{Accepted.String()},
				Dst:  Reserving.String(),
			},
			{
				Name: UpdateReservation.String(),
				Src:  []string{Reserving.String()},
				Dst:  Reserving.String(),
			},
			{
				Name: ResumingApplication.String(),
				Src:  []string{Reserving.String()},
				Dst:  Resuming.String(),
			},
			{
				Name: AppTaskCompleted.String(),
				Src:  []string{Resuming.String()},
				Dst:  Resuming.String(),
			},
			{
				Name: RunApplication.String(),
				Src:  []string{Accepted.String(), Reserving.String(), Resuming.String(), Running.String()},
				Dst:  Running.String(),
			},
			{
				Name: ReleaseAppAllocation.String(),
				Src:  []string{Running.String()},
				Dst:  Running.String(),
			},
			{
				Name: ReleaseAppAllocation.String(),
				Src:  []string{Failing.String()},
				Dst:  Failing.String(),
			},
			{
				Name: ReleaseAppAllocation.String(),
				Src:  []string{Resuming.String()},
				Dst:  Resuming.String(),
			},
			{
				Name: ReleaseAppAllocationAsk.String(),
				Src:  []string{Running.String(), Accepted.String(), Reserving.String()},
				Dst:  Running.String(),
			},
			{
				Name: ReleaseAppAllocationAsk.String(),
				Src:  []string{Failing.String()},
				Dst:  Failing.String(),
			},
			{
				Name: ReleaseAppAllocationAsk.String(),
				Src:  []string{Resuming.String()},
				Dst:  Resuming.String(),
			},
			{
				Name: CompleteApplication.String(),
				Src:  []string{Running.String()},
				Dst:  Completed.String(),
			},
			{
				Name: RejectApplication.String(),
				Src:  []string{Submitted.String()},
				Dst:  Rejected.String(),
			},
			{
				Name: FailApplication.String(),
				Src:  []string{Submitted.String(), Accepted.String(), Running.String(), Reserving.String()},
				Dst:  Failing.String(),
			},
			{
				Name: FailApplication.String(),
				Src:  []string{Failing.String(), Rejected.String()},
				Dst:  Failed.String(),
			},
			{
				Name: KillApplication.String(),
				Src:  []string{Accepted.String(), Running.String(), Reserving.String()},
				Dst:  Killing.String(),
			},
			{
				Name: KilledApplication.String(),
				Src:  []string{Killing.String()},
				Dst:  Killed.String(),
			},
		},
		fsm.Callbacks{
			"enter_state": func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				log.Logger().Debug("shim app state transition",
					zap.String("app", app.applicationID),
					zap.String("source", event.Src),
					zap.String("destination", event.Dst),
					zap.String("event", event.Event))
				if len(event.Args) == 2 {
					eventInfo := event.Args[1].(string) //nolint:errcheck
					app.OnStateChange(event, eventInfo)
				} else {
					app.OnStateChange(event, "")
				}
			},
			// called after enter state
			Reserving.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			// called after event
			SubmitApplication.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.handleSubmitApplicationEvent()
			},
			RecoverApplication.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
				app.handleRecoverApplicationEvent()
			},
			RejectApplication.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			CompleteApplication.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			FailApplication.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			UpdateReservation.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			ReleaseAppAllocation.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			ReleaseAppAllocationAsk.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
			AppTaskCompleted.String(): func(event *fsm.Event) {
				app := event.Args[0].(*Application) //nolint:errcheck
			},
		},
	)
}
