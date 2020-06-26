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
	"sync"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"

	"github.com/looplab/fsm"
	v1 "k8s.io/api/core/v1"
)

type Task struct {
	taskID         string
	alias          string
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

func NewTask(tid string, app *Application, ctx *Context, pod *v1.Pod) *Task {
	taskResource := common.GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, ctx)
}

// test only
func CreateTaskForTest(tid string, app *Application, resource *si.Resource, ctx *Context) *Task {
	// for testing purpose, the pod name is same as the taskID
	taskPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: tid,
		},
	}
	return createTaskInternal(tid, app, resource, taskPod, ctx)
}

func createTaskInternal(tid string, app *Application, resource *si.Resource,
	pod *v1.Pod, ctx *Context) *Task {
	task := &Task{
		taskID:        tid,
		alias:         fmt.Sprintf("%s/%s", pod.Namespace, pod.Name),
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
				Src: states.Any,
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
			string(events.SubmitTask):       task.handleSubmitTaskEvent,
			string(events.TaskFail):         task.handleFailEvent,
			states.Pending:                  task.postTaskPending,
			states.Allocated:                task.postTaskAllocated,
			states.Rejected:                 task.postTaskRejected,
			beforeHook(events.CompleteTask): task.beforeTaskCompleted,
			states.Failed:                   task.postTaskFailed,
			events.EnterState:               task.enterState,
		},
	)

	return task
}

func beforeHook(event events.TaskEventType) string {
	return fmt.Sprintf("before_%s", string(event))
}

// event handling
func (task *Task) handle(te events.TaskEvent) error {
	task.lock.Lock()
	defer task.lock.Unlock()
	err := task.sm.Event(string(te.GetEvent()), te.GetArgs()...)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
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

func (task *Task) GetTaskID() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.taskID
}

func (task *Task) GetTaskState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return task.sm.Current()
}

func (task *Task) getTaskAllocationUUID() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.allocationUUID
}

func (task *Task) setAllocated(nodeName, allocationUUID string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.allocationUUID = allocationUUID
	task.nodeName = nodeName
	task.sm.SetState(events.States().Task.Allocated)
}

func (task *Task) handleFailEvent(event *fsm.Event) {
	eventArgs := make([]string, 1)
	if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
		dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, err.Error()))
		events.GetRecorder().Eventf(task.pod, v1.EventTypeWarning, "SchedulingFailed",
			"%s scheduling failed, reason: %s", task.alias, err.Error())
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
	rr := common.CreateUpdateRequestForTask(task.applicationID, task.taskID, task.resource, task.pod)
	log.Logger.Debug("send update request", zap.String("request", rr.String()))
	if err := task.context.apiProvider.GetAPIs().SchedulerAPI.Update(&rr); err != nil {
		log.Logger.Debug("failed to send scheduling request to scheduler", zap.Error(err))
		return
	}

	events.GetRecorder().Eventf(task.pod, v1.EventTypeNormal, "Scheduling",
		"%s is queued and waiting for allocation", task.alias)

	// after a small amount of time, if the task is still not able to get allocation,
	// put the pod to unscheduable state. This will trigger the cluster-auto-scaler to launch
	// the auto-scaling process.
	// TODO improve this
	// Note, this approach is suboptimal, when a task cannot be allocated, it doesn't always
	// mean it needs the nodes to scale up. E.g task runs out of max capacity of the queue,
	// user/app runs out of the limit etc. Ideally, we should add more interactions between
	// core and shim to negotiate on when to set the state to unscheduable and trigger the
	// auto-scaling appropriately.
	time.AfterFunc(3*time.Second, func() {
		if task.GetTaskState() == events.States().Task.Scheduling {
			log.Logger.Debug("updating pod state ",
				zap.String("appID", task.applicationID),
				zap.String("taskID", task.taskID),
				zap.String("podName", task.alias),
				zap.String("state", "Unscheduable"))
			// if task state is still pending after 5s,
			// move task to un-schedule-able state.
			if err := task.context.updatePodCondition(task.pod,
				&v1.PodCondition{
					Type:    v1.PodScheduled,
					Status:  v1.ConditionFalse,
					Reason:  v1.PodReasonUnschedulable,
					Message: "pod is unable to be scheduled due to lack of resources",
				}); err != nil {
				log.Logger.Error("update pod condition failed",
					zap.Error(err))
			}
			events.GetRecorder().Eventf(task.pod,
				v1.EventTypeNormal, "PodUnscheduable",
				"Task %s state changes to Unscheduable", task.alias)
		}
	})
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

		// post a message to indicate the pod gets its allocation
		events.GetRecorder().Eventf(task.pod,
			v1.EventTypeNormal, "Scheduled",
			"Successfully assigned %s to node %s", task.alias, nodeID)

		// task allocation UID is assigned once we get allocation decision from scheduler core
		task.allocationUUID = allocUUID

		// before binding pod to node, first bind volumes to pod
		log.Logger.Debug("bind pod volumes",
			zap.String("podName", task.pod.Name),
			zap.String("podUID", string(task.pod.UID)))
		if task.context.apiProvider.GetAPIs().VolumeBinder != nil {
			if err := task.context.bindPodVolumes(task.pod); err != nil {
				errorMessage = fmt.Sprintf("bind pod volumes failed, name: %s, %s", task.alias, err.Error())
				dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
				events.GetRecorder().Eventf(task.pod,
					v1.EventTypeWarning, "PodVolumesBindFailure", errorMessage)
				return
			}
		}

		log.Logger.Debug("bind pod",
			zap.String("podName", task.pod.Name),
			zap.String("podUID", string(task.pod.UID)))

		if err := task.context.apiProvider.GetAPIs().KubeClient.Bind(task.pod, nodeID); err != nil {
			errorMessage = fmt.Sprintf("bind pod volumes failed, name: %s, %s", task.alias, err.Error())
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
			"Pod %s is successfully bound to node %s", task.alias, nodeID)
	}(event)
}

func (task *Task) postTaskRejected(event *fsm.Event) {
	// currently, once task is rejected by scheduler, we directly move task to failed state.
	// so this function simply triggers the state transition when it is rejected.
	// but further, we can introduce retry mechanism if necessary.
	dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID,
		fmt.Sprintf("task %s failed because it is rejected by scheduler", task.alias)))

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeWarning, "TaskRejected",
		"Task %s is rejected by the scheduler", task.alias)
}

func (task *Task) postTaskFailed(event *fsm.Event) {
	// when task is failed, we need to do the cleanup,
	// we need to release the allocation from scheduler core
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeNormal, "TaskFailed",
		"Task %s is failed", task.alias)
}

func (task *Task) beforeTaskCompleted(event *fsm.Event) {
	// before task transits to completed, release its allocation from scheduler core
	// this is done as a before hook because the releaseAllocation() call needs to
	// send different requests to scheduler-core, depending on current task state
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod,
		v1.EventTypeNormal, "TaskCompleted",
		"Task %s is completed", task.alias)
}

func (task *Task) releaseAllocation() {
	// scheduler api might be nil in some tests
	if task.context.apiProvider.GetAPIs().SchedulerAPI != nil {
		log.Logger.Debug("prepare to send release request",
			zap.String("applicationID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("taskAlias", task.alias),
			zap.String("allocationUUID", task.allocationUUID),
			zap.String("task", task.GetTaskState()))

		// depends on current task state, generate requests accordingly.
		// if task is already allocated, which means the scheduler core already,
		// places an allocation for it, we need to send AllocationReleaseRequest,
		// if task is not allocated yet, we need to send AllocationAskReleaseRequest
		var releaseRequest si.UpdateRequest
		s := events.States().Task
		switch task.GetTaskState() {
		case s.New, s.Pending, s.Scheduling:
			releaseRequest = common.CreateReleaseAskRequestForTask(
				task.applicationID, task.taskID, task.application.partition)
		default:
			// sending empty allocation UUID back to scheduler-core is dangerous
			// log a warning and skip the release request. this may leak some resource
			// in the scheduler, collect logs and check why this happens.
			if task.allocationUUID == "" {
				log.Logger.Warn("task allocation UUID is empty, sending this release request "+
					"to yunikorn-core could cause all allocations of this app get released. skip this "+
					"request, this may cause some resource leak. check the logs for more info!",
					zap.String("applicationID", task.applicationID),
					zap.String("taskID", task.taskID),
					zap.String("taskAlias", task.alias),
					zap.String("allocationUUID", task.allocationUUID),
					zap.String("task", task.GetTaskState()))
				return
			}
			releaseRequest = common.CreateReleaseAllocationRequestForTask(
				task.applicationID, task.allocationUUID, task.application.partition)
		}

		if releaseRequest.Releases != nil {
			log.Logger.Info("releasing allocations",
				zap.Int("numOfAsksToRelease", len(releaseRequest.Releases.AllocationAsksToRelease)),
				zap.Int("numOfAllocationsToRelease", len(releaseRequest.Releases.AllocationsToRelease)))
		}
		if err := task.context.apiProvider.GetAPIs().SchedulerAPI.Update(&releaseRequest); err != nil {
			log.Logger.Debug("failed to send scheduling request to scheduler", zap.Error(err))
		}
	}
}

// some sanity checks before sending task for scheduling,
// this reduces the scheduling overhead by blocking such
// request away from the core scheduler.
func (task *Task) sanityCheckBeforeScheduling() error {
	// Check PVCs used by the pod
	namespace := task.pod.Namespace
	manifest := &(task.pod.Spec)
	for i := range manifest.Volumes {
		volume := &manifest.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			// Volume is not a PVC, ignore
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		log.Logger.Debug("checking PVC", zap.String("name", pvcName))
		pvc, err := task.context.apiProvider.GetAPIs().PVCInformer.Lister().PersistentVolumeClaims(namespace).Get(pvcName)
		if err != nil {
			return err
		}
		if pvc.DeletionTimestamp != nil {
			return fmt.Errorf("persistentvolumeclaim %q is being deleted", pvc.Name)
		}
	}
	return nil
}

func (task *Task) enterState(event *fsm.Event) {
	log.Logger.Debug("shim task state transition",
		zap.String("app", task.applicationID),
		zap.String("task", task.taskID),
		zap.String("taskAlias", task.alias),
		zap.String("source", event.Src),
		zap.String("destination", event.Dst),
		zap.String("event", event.Event))
}
