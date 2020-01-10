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
	"fmt"
	"sync"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"

	"github.com/looplab/fsm"
	v1 "k8s.io/api/core/v1"
)

type Task struct {
	taskID         string
	applicationID  string
	application    *Application
	allocationUUID string
	resource       *si.Resource
	pod            *v1.Pod
	context        *Context
	nodeName       string
	sm             *fsm.FSM
	lock           *sync.RWMutex
}

func newTask(tid string, app *Application, ctx *Context, pod *v1.Pod) Task {
	taskResource := common.GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, ctx)
}

// test only
func CreateTaskForTest(tid string, app *Application, resource *si.Resource, ctx *Context) Task {
	// for testing purpose, the pod name is same as the taskID
	taskPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: tid,
		},
	}
	return createTaskInternal(tid, app, resource, taskPod, ctx)
}

func createTaskInternal(tid string, app *Application, resource *si.Resource,
	pod *v1.Pod, ctx *Context) Task {
	task := Task{
		taskID:        tid,
		applicationID: app.GetApplicationID(),
		application:   app,
		pod:           pod,
		resource:      resource,
		context:       ctx,
		lock:          &sync.RWMutex{},
	}

	var states = events.States().Task
	task.sm = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: string(events.InitTask),
				Src: []string{states.New},
				Dst: states.Pending},
			{Name: string(events.SubmitTask),
				Src: []string{states.Pending},
				Dst: states.Scheduling},
			{Name: string(events.TaskAllocated),
				Src: []string{states.Scheduling},
				Dst: states.Allocated},
			{Name: string(events.TaskBound),
				Src: []string{states.Allocated},
				Dst: states.Bound},
			{Name: string(events.CompleteTask),
				Src: []string{states.Bound},
				Dst: states.Completed},
			{Name: string(events.KillTask),
				Src: []string{states.Pending, states.Scheduling, states.Allocated, states.Bound},
				Dst: states.Killing},
			{Name: string(events.TaskKilled),
				Src: []string{states.Killing},
				Dst: states.Killed},
			{Name: string(events.TaskRejected),
				Src: []string{states.New, states.Pending, states.Scheduling},
				Dst: states.Rejected},
			{Name: string(events.TaskFail),
				Src: []string{states.Rejected, states.Allocated},
				Dst: states.Failed},
		},
		fsm.Callbacks{
			string(events.SubmitTask): task.handleSubmitTaskEvent,
			string(events.TaskFail):   task.handleFailEvent,
			states.Pending:            task.postTaskPending,
			states.Allocated:          task.postTaskAllocated,
			states.Rejected:           task.postTaskRejected,
			states.Completed:          task.postTaskCompleted,
			states.Failed:             task.postTaskFailed,
		},
	)

	return task
}

// event handling
func (task *Task) handle(te events.TaskEvent) error {
	task.lock.Lock()
	defer task.lock.Unlock()
	log.Logger.Debug("task state transition",
		zap.String("taskID", task.taskID),
		zap.String("preState", task.sm.Current()),
		zap.String("pendingEvent", string(te.GetEvent())))
	err := task.sm.Event(string(te.GetEvent()), te.GetArgs()...)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	log.Logger.Debug("task state transition",
		zap.String("taskID", task.taskID),
		zap.String("postState", task.sm.Current()))
	return nil
}

func (task *Task) canHandle(te events.TaskEvent) bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.sm.Can(string(te.GetEvent()))
}

func (task *Task) GetTaskPod() *v1.Pod {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.pod
}

func (task *Task) GetTaskState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return task.sm.Current()
}

func (task *Task) setAllocated(nodeName string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.nodeName = nodeName
	task.sm.SetState(events.States().Task.Allocated)
}

func (task *Task) handleFailEvent(event *fsm.Event) {
	eventArgs := make([]string, 1)
	if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
		dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, err.Error()))
		return
	}

	log.Logger.Error("task failed",
		zap.String("appID", task.applicationID),
		zap.String("taskID", task.taskID),
		zap.String("reason", eventArgs[0]))
}

func (task *Task) handleSubmitTaskEvent(event *fsm.Event) {
	log.Logger.Debug("scheduling pod",
		zap.String("podName", task.pod.Name))
	// convert the request
	rr := common.CreateUpdateRequestForTask(task.applicationID, task.taskID, task.resource)
	log.Logger.Debug("send update request", zap.String("request", rr.String()))
	if err := task.context.apiProvider.GetClientSet().SchedulerAPI.Update(&rr); err != nil {
		log.Logger.Debug("failed to send scheduling request to scheduler", zap.Error(err))
		return
	}

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeNormal, "TaskSubmitted",
		"application \"%s\" task \"%s\" is submitted to the scheduler",
		task.applicationID, task.taskID)
}

// this is called after task reaches PENDING state,
// submit the resource asks from this task to the scheduler core
func (task *Task) postTaskPending(event *fsm.Event) {
	dispatcher.Dispatch(NewSubmitTaskEvent(task.applicationID, task.taskID))
}

// this is called after task reaches ALLOCATED state,
// we run this in a go routine to bind pod to the allocated node,
// if successful, we move task to next state BOUND,
// otherwise we fail the task
func (task *Task) postTaskAllocated(event *fsm.Event) {
	// delay binding task
	// this calls K8s api to bind a pod to the assigned node, this may need some time,
	// so we do a delay binding to avoid blocking main process. we tracks the result
	// of the binding and properly handle failures.
	go func(event *fsm.Event) {
		// we need to obtain task's lock first,
		// this ensures no other threads modifying task state at the time being
		task.lock.Lock()
		defer task.lock.Unlock()

		var errorMessage string
		eventArgs := make([]string, 2)
		if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
			errorMessage = err.Error()
			log.Logger.Error("error", zap.Error(err))
			dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
			return
		}

		allocUUID := eventArgs[0]
		nodeID := eventArgs[1]

		// task allocation UID is assigned once we get allocation decision from scheduler core
		task.allocationUUID = allocUUID

		// before binding pod to node, first bind volumes to pod
		log.Logger.Debug("bind pod volumes",
			zap.String("podName", task.pod.Name),
			zap.String("podUID", string(task.pod.UID)))
		if task.context.apiProvider.GetClientSet().VolumeBinder != nil {
			if err := task.context.bindPodVolumes(task.pod); err != nil {
				errorMessage = fmt.Sprintf("bind pod volumes failed, name: %s, uid: %s, %#v",
					task.pod.Name, task.pod.UID, err)
				dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
				events.GetRecorder().Eventf(task.pod,
					v1.EventTypeWarning, "PodVolumesBindFailure", errorMessage)
				return
			}
		}

		log.Logger.Debug("bind pod",
			zap.String("podName", task.pod.Name),
			zap.String("podUID", string(task.pod.UID)))

		if err := task.context.apiProvider.GetClientSet().KubeClient.Bind(task.pod, nodeID); err != nil {
			errorMessage = fmt.Sprintf("bind pod failed, name: %s, uid: %s, %#v",
				task.pod.Name, task.pod.UID, err)
			log.Logger.Error(errorMessage)
			dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
			events.GetRecorder().Eventf(task.pod,
				v1.EventTypeWarning, "PodBindFailure", errorMessage)
			return
		}

		log.Logger.Info("successfully bound pod", zap.String("podName", task.pod.Name))
		dispatcher.Dispatch(NewBindTaskEvent(task.applicationID, task.taskID))
		events.GetRecorder().Eventf(task.pod,
			v1.EventTypeNormal, "PodBindSuccessful",
			"pod \"%s\" successfully bound to node \"%s\"", task.pod.Name, nodeID)
	}(event)
}

func (task *Task) postTaskRejected(event *fsm.Event) {
	// currently, once task is rejected by scheduler, we directly move task to failed state.
	// so this function simply triggers the state transition when it is rejected.
	// but further, we can introduce retry mechanism if necessary.
	dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID,
		fmt.Sprintf("task %s failed because it is rejected by scheduler", task.taskID)))

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeWarning, "TaskRejected",
		"application \"%s\" task \"%s\" is rejected by the scheduler", task.applicationID, task.taskID)
}

func (task *Task) postTaskFailed(event *fsm.Event) {
	// when task is failed, we need to do the cleanup,
	// we need to release the allocation from scheduler core
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeWarning, "TaskFailed",
		"application \"%s\" task \"%s\" is failed", task.applicationID, task.taskID)
}

func (task *Task) postTaskCompleted(event *fsm.Event) {
	// when task is completed, release its allocation from scheduler core
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeNormal, "TaskCompleted",
		"application \"%s\" task \"%s\" is completed", task.applicationID, task.taskID)
}

func (task *Task) releaseAllocation() {
	// when task is completed, we notify the scheduler to release allocations
	go func() {
		// scheduler api might be nil in some tests
		if task.context.apiProvider.GetClientSet().SchedulerAPI != nil {
			releaseRequest := common.CreateReleaseAllocationRequestForTask(
				task.applicationID, task.allocationUUID, task.application.partition)

			log.Logger.Debug("send release request",
				zap.String("releaseRequest", releaseRequest.String()))
			if err := task.context.apiProvider.GetClientSet().SchedulerAPI.Update(&releaseRequest); err != nil {
				log.Logger.Debug("failed to send scheduling request to scheduler", zap.Error(err))
			}
		}
	}()
}
