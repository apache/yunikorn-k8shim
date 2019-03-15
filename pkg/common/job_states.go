package common

type JobStates struct {
	NEW JobState
	SUBMITTED JobState
	ACCEPTED JobState
	RUNNING JobState
	REJECTED JobState
	COMPLETED JobState
	KILLING JobState
	KILLED JobState
	FAILED JobState
}

type JobState struct {
	state string
	Value func() string
}

func newJobState(str string) JobState{
	return JobState{
		state: str,
		Value: func() string { return str }}
}

func InitiateJobStates() *JobStates {
	return &JobStates {
		NEW: newJobState("NEW"),
		SUBMITTED: newJobState("SUBMITTED"),
		ACCEPTED: newJobState("ACCEPTED"),
		RUNNING: newJobState("RUNNING"),
		REJECTED: newJobState("REJECTED"),
		COMPLETED: newJobState("COMPLETED"),
		KILLING: newJobState("KILLING"),
		KILLED: newJobState("KILLED"),
		FAILED: newJobState("FAILED"),
	}
}