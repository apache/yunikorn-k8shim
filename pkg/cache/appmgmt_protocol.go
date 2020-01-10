package cache

import (
	v1 "k8s.io/api/core/v1"
)

type AddApplicationRequest struct {
	ApplicationID string   //required
	QueueName     string   //
	User          string
	Tags          map[string]string
	Recovery      bool
}

type AddTaskRequest struct {
	ApplicationID string
	TaskID        string
	Pod           *v1.Pod
	Recovery      bool
}

type ApplicationManagementProtocol interface {
	GetApplication(appID string) (*Application, bool)
	AddApplication(request *AddApplicationRequest) *Application
	AddTask(request *AddTaskRequest)
	NotifyApplicationComplete(appID string)
}
