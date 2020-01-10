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

package appmgmt

import (
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	v1 "k8s.io/api/core/v1"
)

// a common interface for app management service
// an app management service monitors the lifecycle of applications,
// it is responsible for reporting application status to the scheduler,
// that helps the scheduler to manage the application lifecycle natively.
type AppManager interface {
	// the name of this application service
	// this info is exposed to the scheduler so we know what kind of apps
	// the scheduler is able to supervise.
	Name() string

	// if the service needs to init any objects, this is the place
	// the initialization of the service must not start any of go routines,
	// this will be called before starting the service.
	ServiceInit() error

	// if the service has some internal stuff to run, this is the place to run them
	// usually if an application is defined as K8s CRD, the operator service needs
	// to watch on these CRD events. the CRD informers can be launched here.
	// some implementation may not need to implement this.
	Start() error

	// if there is some go routines running in start, properly stop them while
	// the stop() function is called.
	Stop() error

	// why we need a recoveryApplication API here?
	// the scheduler is stateless, all states are maintained just in memory,
	// so each time when scheduler restarts, it needs to recover apps and nodes states from scratch.
	// nodes state will be taken care of by the scheduler itself, however for apps state recovery,
	// the scheduler will need to call this function for every allocated pod, app management service
	// needs to implement this accordingly.
	GetAppMetadata(pod *v1.Pod) (cache.ApplicationMetadata, bool)

	GetTaskMetadata(pod *v1.Pod) (cache.TaskMetadata, bool)
}