package common

type JobEvents struct {
	SUBMIT JobEvent
	ACCEPT JobEvent
	RUN JobEvent
	REJECT JobEvent
	COMPLETE JobEvent
	FAIL JobEvent
	KILL JobEvent
	KILLED JobEvent
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
		COMPLETE: JobEvent{"COMPLETE"},
		FAIL: JobEvent{"FAIL"},
		KILL: JobEvent{"KILL"},
		KILLED: JobEvent{"KILLED"},
	}
}