/*
Copyright 2019 The Unity Scheduler Authors

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

package common

import (
	"fmt"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"k8s.io/api/core/v1"
)

type Task struct {
	taskId        string
	applicationId string
	application   *Application
	resource      *si.Resource
	pod           *v1.Pod
	kubeClient    client.KubeClient
	schedulerApi  api.SchedulerApi
	sm            *fsm.FSM
	lock          *sync.RWMutex
}

func newTask(tid string, app *Application, client client.KubeClient, schedulerApi api.SchedulerApi, pod *v1.Pod) Task {
	taskResource := GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, client, schedulerApi)
}

func CreateTaskForTest(tid string, app *Application, resource *si.Resource,
	client client.KubeClient, schedulerApi api.SchedulerApi) Task {
	return createTaskInternal(tid, app, resource, &v1.Pod{}, client, schedulerApi)
}

func createTaskInternal(tid string, app *Application, resource *si.Resource,
	pod *v1.Pod, client client.KubeClient, schedulerApi api.SchedulerApi) Task {
	task := Task{
		taskId:        tid,
		applicationId: app.applicationId,
		application:   app,
		pod:           pod,
		resource:      resource,
		kubeClient:    client,
		schedulerApi:  schedulerApi,
		lock:          &sync.RWMutex{},
	}

	var states = States().Task
	task.sm = fsm.NewFSM(
		states.Pending,
		fsm.Events{
			{Name: string(Submit),
				Src: []string{states.Pending},
				Dst: states.Scheduling},
			{Name: string(Allocated),
				Src: []string{states.Scheduling},
				Dst: states.Allocated},
			{Name: string(Bound),
				Src: []string{states.Allocated},
				Dst: states.Bound},
			{Name: string(Complete),
				Src: []string{states.Bound},
				Dst: states.Completed},
			{Name: string(Kill),
				Src: []string{states.Pending, states.Scheduling, states.Allocated, states.Bound},
				Dst: states.Killing},
			{Name: string(Killed),
				Src: []string{states.Killing},
				Dst: states.Killed},
			{Name: string(Rejected),
				Src: []string{states.Pending, states.Scheduling},
				Dst: states.Rejected},
			{Name: string(Fail),
				Src: []string{states.Rejected},
				Dst: states.Failed},
		},
		fsm.Callbacks{
			string(Submit):         task.handleSubmitTaskEvent,
			string(Fail):           task.handleFailEvent,
			states.Allocated:       task.postTaskAllocated,
			states.Rejected:        task.postTaskRejected,
			"enter_state":          task.onStateChange,
		},
	)

	return task
}

func CreateTaskFromPod(app *Application, client client.KubeClient, scheduler api.SchedulerApi, pod *v1.Pod) *Task {
	task := newTask(string(pod.UID), app, client, scheduler, pod)
	return &task
}

func (task *Task) GetTaskPod() *v1.Pod {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.pod
}

func (task *Task) GetTaskId() string {
	return task.taskId
}

func (task *Task) GetTaskState() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.sm.Current()
}

func (task *Task) IsPending() bool {
	return strings.Compare(task.GetTaskState(), States().Task.Pending) == 0
}

func (task *Task) handleFailEvent(event *fsm.Event) {
	task.lock.Lock()
	defer task.lock.Unlock()

	if len(event.Args) != 1 {
		glog.V(1).Infof("invalid number of arguments, expecting 1 but get %d",
			len(event.Args))
		return
	}

	glog.V(1).Infof("Task failed, applicationId=%s, taskId=%s, error message: %s",
		task.applicationId, task.taskId, event.Args[0])
}

func (task *Task) onStateChange(event *fsm.Event) {
	glog.V(4).Infof("Enqueue task on state change," +
		" taskId=%s, preState=%s, postState=%s",
		task.taskId, event.Src, event.Dst)
	task.application.workChan <- task
}

func (task *Task) handleSubmitTaskEvent(event *fsm.Event) {
	glog.V(4).Infof("Trying to schedule pod: %s", task.GetTaskPod().Name)
	// convert the request
	//rr := ConvertRequest(task.applicationId, task.GetTaskPod())
	appQueue := task.application.queue
	if queueName, ok := task.pod.Labels[LabelQueueName]; ok {
		appQueue = queueName
	}
	rr := CreateUpdateRequestForTask(task.applicationId, task.taskId, appQueue, task.resource)
	glog.V(4).Infof("send update request %s", rr.String())
	if err := task.schedulerApi.Update(&rr); err != nil {
		glog.V(2).Infof("failed to send scheduling request to scheduler, error: %v", err)
		return
	}
}

// this is called after task reaches ALLOCATED state,
// we run this in a go routine to bind pod to the allocated node,
// if successful, we move task to next state BOUND,
// otherwise we fail the task
func (task *Task) postTaskAllocated(event *fsm.Event) {
	go func(event *fsm.Event) {
		task.lock.Lock()
		defer task.lock.Unlock()

		var errorMessage string
		// once allocated, task state transits to ALLOCATED,
		// now trigger BIND pod to a specific node
		if len(event.Args) != 1 {
			errorMessage = fmt.Sprintf("invalid number of arguments, expecting 1 but get %d", len(event.Args))
			glog.V(1).Infof(errorMessage)
			task.Handle(NewFailTaskEvent(errorMessage))
		}

		nodeId := fmt.Sprint(event.Args[0])
		glog.V(4).Infof("bind pod target: name: %s, uid: %s", task.pod.Name, task.pod.UID)
		if err := task.kubeClient.Bind(task.pod, nodeId); err != nil {
			errorMessage = fmt.Sprintf("bind pod failed, name: %s, uid: %s, %#v",
				task.pod.Name, task.pod.UID, err)
			glog.V(1).Info(errorMessage)
			task.Handle(NewFailTaskEvent(errorMessage))
			return
		}

		glog.V(3).Infof("Successfully bound pod %s", task.pod.Name)
		task.Handle(NewBindTaskEvent())
	}(event)
}

func (task *Task) postTaskRejected(event *fsm.Event) {
	// currently, once task is rejected by scheduler, we directly move task to failed state.
	// so this function simply triggers the state transition when it is rejected.
	// but further, we can introduce retry mechanism if necessary.
	go func() {
		task.Handle(NewFailTaskEvent(
			fmt.Sprintf("task %s failed because it is rejected by scheduler",
				task.taskId)))
	}()
}

// event handling
func (task *Task) Handle(te TaskEvent) error {
	glog.V(4).Infof("Task(%s): preState: %s, coming event: %s",
		task.taskId, task.sm.Current(), te.getEvent())
	err := task.sm.Event(string(te.getEvent()), te.getArgs())
	glog.V(4).Infof("Task(%s): postState: %s, handled event: %s",
		task.taskId, task.sm.Current(), te.getEvent())
	return err
}
