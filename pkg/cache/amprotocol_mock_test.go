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
	"testing"
	"time"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestAddApplication(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user",
		Groups:        []string{"test-group"},
		Tags: map[string]string{
			constants.AppTagNamespace:        constants.DefaultAppNamespace,
			constants.AppTagImagePullSecrets: "secret1,secret2",
			siCommon.AppTagStateAwareDisable: "true",
		},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: metav1.SchemeGroupVersion.String(),
				Name:       "owner ref",
			},
		},
		CreationTime: time.Now().Unix(),
	}

	amProtocol := NewMockedAMProtocol()

	amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})

	ManagedApp := amProtocol.GetApplication("app0001")
	assert.Equal(t, ManagedApp.GetApplicationID(), "app0001")
	assert.Equal(t, ManagedApp.GetQueue(), "root")
	assert.Equal(t, ManagedApp.GetUser(), "test-user")

	app := amProtocol.applications["app0001"]
	assert.Equal(t, app.groups[0], "test-group")
	assert.Equal(t, app.GetTags()[constants.AppTagNamespace], constants.DefaultAppNamespace)
	assert.Equal(t, app.GetTags()[constants.AppTagImagePullSecrets], "secret1,secret2")
	assert.Equal(t, app.GetTags()[siCommon.AppTagStateAwareDisable], "true")
	assert.Equal(t, app.getPlaceholderOwnerReferences()[0].Name, "owner ref")
}

func TestRemoveApplications(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user1",
	}

	amProtocol := NewMockedAMProtocol()
	amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})

	// remove application
	amProtocol.RemoveApplication("app0001")
	app := amProtocol.GetApplication("app0001")
	assert.Equal(t, app == nil, true)

	// remove application which doesn't exist
	err := amProtocol.RemoveApplication("app0001")
	assert.Error(t, err, "application doesn't exist")
}

func TestAddTasks(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user",
		Tags: map[string]string{
			constants.AppTagNamespace: constants.DefaultAppNamespace,
		},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: metav1.SchemeGroupVersion.String(),
				Name:       "ownerref",
				UID:        types.UID("ownerref"),
			},
		},
	}
	taskMeta := interfaces.TaskMetadata{
		ApplicationID: "app0001",
		TaskID:        "task0001",
		Pod:           newPodHelper("test-pod", "default", "task0001", "fake-node", v1.PodRunning),
	}

	amProtocol := NewMockedAMProtocol()
	app := amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})

	amProtocol.AddTask(&interfaces.AddTaskRequest{
		Metadata: taskMeta,
	})

	task, err := app.GetTask("task0001")
	assert.NilError(t, err, "Get failed")
	assert.Equal(t, task.GetTaskPod().Name, "test-pod")
}

func TestRemoveTasks(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user",
		Tags: map[string]string{
			constants.AppTagNamespace: constants.DefaultAppNamespace,
		},
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: metav1.SchemeGroupVersion.String(),
				Name:       "ownerref",
				UID:        types.UID("ownerref"),
			},
		},
	}
	taskMeta := interfaces.TaskMetadata{
		ApplicationID: "app0001",
		TaskID:        "task0001",
		Pod:           newPodHelper("test-pod", "default", "task0001", "fake-node", v1.PodRunning),
	}

	amProtocol := NewMockedAMProtocol()
	app := amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})

	amProtocol.AddTask(&interfaces.AddTaskRequest{
		Metadata: taskMeta,
	})
	_, err := app.GetTask("task0001")
	assert.NilError(t, err, "Get failed")

	amProtocol.RemoveTask("app0001", "task0001")
	_, err = app.GetTask("task0001")
	assert.Error(t, err, "task task0001 doesn't exist in application app0001")
}

func TestNotifyApplicationComplete(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user1",
	}

	amProtocol := NewMockedAMProtocol()
	amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})
	app := amProtocol.applications["app0001"]
	amProtocol.NotifyApplicationComplete("app0001")

	assert.Equal(t, app.GetApplicationState(), "Completed")
}

func TestNotifyApplicationFail(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user1",
	}

	amProtocol := NewMockedAMProtocol()
	amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})
	app := amProtocol.applications["app0001"]
	amProtocol.NotifyApplicationFail("app0001")

	assert.Equal(t, app.GetApplicationState(), "Failed")
}

func TestNotifyTaskComplete(t *testing.T) {
	appMeta := interfaces.ApplicationMetadata{
		ApplicationID: "app0001",
		QueueName:     "root",
		User:          "test-user",
	}
	taskMeta := interfaces.TaskMetadata{
		ApplicationID: "app0001",
		TaskID:        "task0001",
		Pod:           newPodHelper("test-pod", "default", "task0001", "fake-node", v1.PodRunning),
	}

	amProtocol := NewMockedAMProtocol()
	app := amProtocol.AddApplication(&interfaces.AddApplicationRequest{
		Metadata: appMeta,
	})
	amProtocol.AddTask(&interfaces.AddTaskRequest{
		Metadata: taskMeta,
	})
	amProtocol.NotifyTaskComplete("app0001", "task0001")
	task, err := app.GetTask("task0001")

	assert.NilError(t, err, "Get failed")
	assert.Equal(t, task.GetTaskState(), "Completed")
}
