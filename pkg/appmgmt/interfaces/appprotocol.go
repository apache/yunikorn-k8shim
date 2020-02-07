package interfaces

import v1 "k8s.io/api/core/v1"

type ManagedApp interface {
	GetApplicationID() string
	GetTask(taskID string) (ManagedTask, error)
	GetApplicationState() string
	GetQueue() string
	GetUser() string
	SetState(state string)
}

type ManagedTask interface {
	GetTaskID() string
	GetTaskState() string
	GetTaskPod() *v1.Pod
}