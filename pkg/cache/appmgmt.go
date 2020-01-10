/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	v1 "k8s.io/api/core/v1"
)

// app management protocol defines all the APIs needed for app management,
// this is the protocol between scheduler cache and app management plugins
type ApplicationManagementProtocol interface {
	GetApplication(appID string) (*Application, bool)
	AddApplication(request *AddApplicationRequest) *Application
	AddTask(request *AddTaskRequest)
	NotifyApplicationComplete(appID string)
}

type AddApplicationRequest struct {
	Metadata ApplicationMetadata
	Recovery bool
}

type AddTaskRequest struct {
	Metadata TaskMetadata
	Recovery bool
}

type ApplicationMetadata struct {
	ApplicationID string
	QueueName     string
	User          string
	Tags          map[string]string
}

type TaskMetadata struct {
	ApplicationID string
	TaskID        string
	Pod           *v1.Pod
}