package common

type JobEvents struct {
	SUBMIT JobEvent
	ACCEPT JobEvent
	RUN JobEvent
	REJECT JobEvent
}

type JobEvent struct {
	event string
}

func InitiateJobEvents() *JobEvents {
	return &JobEvents {
		SUBMIT: JobEvent{"SUBMIT"},
		ACCEPT: JobEvent{"ACCEPT"},
		RUN: JobEvent{"RUN"},
		REJECT: JobEvent{"REJECT"},
	}
}