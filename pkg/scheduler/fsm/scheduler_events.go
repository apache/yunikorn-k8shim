package fsm

type SchedulerEvents struct {
	REGISTER SchedulerEvent
}

type SchedulerEvent struct {
	event string
}

func InitiateEvents() *SchedulerEvents {
	return &SchedulerEvents {
		REGISTER: SchedulerEvent{"REGISTER"},
	}
}