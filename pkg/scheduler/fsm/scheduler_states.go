package fsm

type SchedulerStates struct {
	NEW SchedulerState
	REGISTERED SchedulerState
	SYNCING SchedulerState
	RUNNING SchedulerState
	DRAINING SchedulerState
	STOPPING SchedulerState
	STOPPED SchedulerState
}

type SchedulerState struct {
	state string
}

func InitiateStates() *SchedulerStates {
	return &SchedulerStates {
		NEW: SchedulerState{"NEW"},
		REGISTERED: SchedulerState{"REGISTERED"},
	}
}
