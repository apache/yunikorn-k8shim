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

package interfaces

import (
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
)

// app management protocol defines all the APIs needed for app management,
// this is the protocol between scheduler cache and app management plugins
type ApplicationManagementProtocol interface {
	// returns app that already existed in the cache,
	// or nil, false if app with the given appID is not found
	GetApplication(appID string) ManagedApp

	// add app to the context, app manager needs to provide all
	// necessary app metadata through this call. If this a existing app
	// for recovery, the AddApplicationRequest#Recovery must be true.
	AddApplication(request *AddApplicationRequest) ManagedApp

	// remove application from the context
	// returns an error if for some reason the app cannot be removed,
	// e.g the given app is not found in current context.
	RemoveApplication(appID string) error

	// add task to the context, if add is successful,
	AddTask(request *AddTaskRequest) ManagedTask

	// remove task from the app
	// return an error if for some reason the task cannot be removed
	// e.g app that owns this task is not found in context.
	RemoveTask(appID, taskID string) error

	// notify the context that an app is completed,
	// this will trigger some consequent operations for the given app
	NotifyApplicationComplete(appID string)

	// notify the context that an task is completed,
	// this will trigger some consequent operations for a given task,
	// e.g release the allocations that assigned for this task.
	NotifyTaskComplete(appID, taskID string)
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
	TaskGroups    []v1alpha1.TaskGroup
}

type TaskMetadata struct {
	ApplicationID string
	TaskID        string
	Pod           *v1.Pod
	Placeholder   bool
	TaskGroupName string
}
