/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package cache

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// implements ApplicationManagementProtocol
type MockedAMProtocol struct {
	applications map[string]*Application
	addTaskFn    func(request *interfaces.AddTaskRequest)
}

func NewMockedAMProtocol() *MockedAMProtocol {
	return &MockedAMProtocol{
		applications: make(map[string]*Application)}
}

func (m *MockedAMProtocol) GetApplication(appID string) interfaces.ManagedApp {
	if app, ok := m.applications[appID]; ok {
		return app
	}
	return nil
}

func (m *MockedAMProtocol) AddApplication(request *interfaces.AddApplicationRequest) interfaces.ManagedApp {
	if app := m.GetApplication(request.Metadata.ApplicationID); app != nil {
		return app
	}

	app := NewApplication(
		request.Metadata.ApplicationID,
		request.Metadata.QueueName,
		request.Metadata.User,
		request.Metadata.Groups,
		request.Metadata.Tags,
		test.NewSchedulerAPIMock())
	app.setPlaceholderOwnerReferences(request.Metadata.OwnerReferences)

	// add into cache
	m.applications[app.GetApplicationID()] = app

	return app
}

func (m *MockedAMProtocol) RemoveApplication(appID string) error {
	if app := m.GetApplication(appID); app != nil {
		delete(m.applications, appID)
		return nil
	}
	return fmt.Errorf("application doesn't exist")
}

func (m *MockedAMProtocol) AddTask(request *interfaces.AddTaskRequest) interfaces.ManagedTask {
	if m.addTaskFn != nil {
		m.addTaskFn(request)
	}
	if app, ok := m.applications[request.Metadata.ApplicationID]; ok {
		existingTask, err := app.GetTask(request.Metadata.TaskID)
		if err != nil {
			var originator bool

			// Is this task the originator of the application?
			// If yes, then make it as "first pod/owner/driver" of the application and set the task as originator
			if app.GetOriginatingTask() == nil {
				for _, ownerReference := range app.getPlaceholderOwnerReferences() {
					referenceID := string(ownerReference.UID)
					if request.Metadata.TaskID == referenceID {
						originator = true
						break
					}
				}
			}
			task := NewFromTaskMeta(request.Metadata.TaskID, app, nil, request.Metadata, originator)
			app.addTask(task)
			log.Logger().Info("task added",
				zap.String("appID", app.applicationID),
				zap.String("taskID", task.taskID),
				zap.String("taskState", task.GetTaskState()))
			if originator {
				if app.GetOriginatingTask() != nil {
					log.Logger().Error("Inconsistent state - found another originator task for an application",
						zap.String("taskId", task.GetTaskID()))
				}
				app.setOriginatingTask(task)
				log.Logger().Info("app request originating pod added",
					zap.String("appID", app.applicationID),
					zap.String("original task", task.GetTaskID()))
			}
			return task
		}
		return existingTask
	}
	return nil
}

func (m *MockedAMProtocol) RemoveTask(appID, taskID string) {
	if app, ok := m.applications[appID]; ok {
		app.removeTask(taskID)
	}
}

func (m *MockedAMProtocol) NotifyApplicationComplete(appID string) {
	if app := m.GetApplication(appID); app != nil {
		if p, valid := app.(*Application); valid {
			p.SetState(ApplicationStates().Completed)
		}
	}
}

func (m *MockedAMProtocol) NotifyApplicationFail(appID string) {
	if app := m.GetApplication(appID); app != nil {
		if p, valid := app.(*Application); valid {
			p.SetState(ApplicationStates().Failed)
		}
	}
}

func (m *MockedAMProtocol) NotifyTaskComplete(appID, taskID string) {
	if app := m.GetApplication(appID); app != nil {
		if task, err := app.GetTask(taskID); err == nil {
			if t, ok := task.(*Task); ok {
				t.sm.SetState(TaskStates().Completed)
			}
		}
	}
}

func (m *MockedAMProtocol) UseAddTaskFn(fn func(request *interfaces.AddTaskRequest)) {
	m.addTaskFn = fn
}
