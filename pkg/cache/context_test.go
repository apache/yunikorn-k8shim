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
	"testing"

	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/test"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
)

const fakeClusterID = "test-cluster"
const fakeClusterVersion = "0.1.0"
const fakeClusterSchedulerName = "yunikorn-test"
const fakeClusterSchedulingInterval = 1

func initContextForTest() *Context {
	configs := conf.SchedulerConf{
		ClusterID:      fakeClusterID,
		ClusterVersion: fakeClusterVersion,
		SchedulerName:  fakeClusterSchedulerName,
		Interval:       fakeClusterSchedulingInterval,
		KubeConfig:     "",
		TestMode:       true,
	}

	conf.Set(&configs)

	context := NewContextInternal(test.NewMockedAPIProvider(), true)
	return context
}
func TestAddApplications(t *testing.T) {
	context := initContextForTest()
	context.AddApplication(&AddApplicationRequest{
		Metadata: ApplicationMetadata{
			ApplicationID: "app00001",
			QueueName:     "root.a",
			User:          "test-user",
			Tags:          nil,
		},
		Recovery: false,
	})
	assert.Equal(t, len(context.applications), 1)
	assert.Assert(t, context.applications["app00001"] != nil)
	assert.Equal(t, context.applications["app00001"].GetApplicationState(), events.States().Application.New)
	assert.Equal(t, len(context.applications["app00001"].GetPendingTasks()), 0)

	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00001",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})

	context.AddTask(&AddTaskRequest{
		Metadata: TaskMetadata{
			ApplicationID: "app00001",
			TaskID:        "task00002",
			Pod:           &v1.Pod{},
		},
		Recovery: false,
	})

	assert.Equal(t, len(context.applications["app00001"].GetNewTasks()), 2)
}
