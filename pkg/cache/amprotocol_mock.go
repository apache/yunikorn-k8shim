package cache

import (
	"fmt"

	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
)

// implements ApplicationManagementProtocol
type MockedAMProtocol struct {
	applications map[string]*Application
}

func NewMockedAMProtocol() *MockedAMProtocol {
	return &MockedAMProtocol{
		applications: make(map[string]*Application)}
}

func (m *MockedAMProtocol) GetApplication(appID string) (*Application, bool) {
	if app, ok := m.applications[appID]; ok {
		return app, true
	}
	return nil, false
}

func (m *MockedAMProtocol) AddApplication(request *AddApplicationRequest) (*Application, bool) {
	if app, ok := m.GetApplication(request.Metadata.ApplicationID); ok {
		return app, false
	}

	app := NewApplication(
		request.Metadata.ApplicationID,
		request.Metadata.QueueName,
		request.Metadata.User,
		request.Metadata.Tags,
		nil)

	// add into cache
	m.applications[app.GetApplicationID()] = app

	switch request.Recovery {
	case true:
		app.SetState(events.States().Application.Recovering)
	case false:
		app.SetState(events.States().Application.New)
	}

	return app, true
}

func (m *MockedAMProtocol) RemoveApplication(appID string) error {
	if _, ok := m.GetApplication(appID); ok {
		delete(m.applications, appID)
		return nil
	}
	return fmt.Errorf("application doesn't exist")
}

func (m *MockedAMProtocol) AddTask(request *AddTaskRequest) (*Task, bool) {
	if app, ok := m.applications[request.Metadata.ApplicationID]; ok {
		if existingTask, err := app.GetTask(request.Metadata.TaskID); err != nil {
			task := NewTask(request.Metadata.TaskID, app, nil, request.Metadata.Pod)
			app.addTask(&task)
			return &task, true
		} else {
			return existingTask, false
		}
	} else {
		return nil, false
	}
}

func (m *MockedAMProtocol) RemoveTask(appID, taskID string) error {
	if app, ok := m.applications[appID]; ok {
		return app.removeTask(taskID)
	} else {
		return fmt.Errorf("app not found")
	}
}

func (m *MockedAMProtocol) NotifyApplicationComplete(appID string) {

}

