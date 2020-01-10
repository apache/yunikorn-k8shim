package cache

import (
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

func (m *MockedAMProtocol) AddApplication(request *AddApplicationRequest) *Application {
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
	return nil
}

func (m *MockedAMProtocol) AddTask(request *AddTaskRequest) {
	if app, ok := m.applications[request.Metadata.ApplicationID]; ok {
		if _, err := app.GetTask(request.Metadata.TaskID); err != nil {
			task := NewTask(request.Metadata.TaskID, app, nil, request.Metadata.Pod)
			app.AddTask(&task)
		}
	}
}

func (m *MockedAMProtocol) NotifyApplicationComplete(appID string) {

}

