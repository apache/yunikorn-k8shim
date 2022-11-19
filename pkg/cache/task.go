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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"

	"github.com/looplab/fsm"
	v1 "k8s.io/api/core/v1"
)

type Task struct {
	taskID          string
	alias           string
	applicationID   string
	application     *Application
	allocationUUID  string
	resource        *si.Resource
	pod             *v1.Pod
	context         *Context
	nodeName        string
	createTime      time.Time
	taskGroupName   string
	placeholder     bool
	terminationType string
	pluginMode      bool
	originator      bool
	sm              *fsm.FSM
	lock            *sync.RWMutex
}

func NewTask(tid string, app *Application, ctx *Context, pod *v1.Pod) *Task {
	taskResource := common.GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, false, "", ctx, false)
}

func NewTaskPlaceholder(tid string, app *Application, ctx *Context, pod *v1.Pod) *Task {
	taskResource := common.GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, true, "", ctx, false)
}

func NewFromTaskMeta(tid string, app *Application, ctx *Context, metadata interfaces.TaskMetadata, originator bool) *Task {
	taskPod := metadata.Pod
	taskResource := common.GetPodResource(taskPod)
	return createTaskInternal(
		tid,
		app,
		taskResource,
		metadata.Pod,
		metadata.Placeholder,
		metadata.TaskGroupName,
		ctx,
		originator)
}

func createTaskInternal(tid string, app *Application, resource *si.Resource,
	pod *v1.Pod, placeholder bool, taskGroupName string, ctx *Context, originator bool) *Task {
	var pluginMode bool
	if ctx != nil {
		pluginMode = ctx.IsPluginMode()
	}
	task := &Task{
		taskID:        tid,
		alias:         fmt.Sprintf("%s/%s", pod.Namespace, pod.Name),
		applicationID: app.GetApplicationID(),
		application:   app,
		pod:           pod,
		resource:      resource,
		createTime:    pod.GetCreationTimestamp().Time,
		placeholder:   placeholder,
		taskGroupName: taskGroupName,
		pluginMode:    pluginMode,
		originator:    originator,
		context:       ctx,
		sm:            newTaskState(),
		lock:          &sync.RWMutex{},
	}
	if tgName := utils.GetTaskGroupFromPodSpec(pod); tgName != "" {
		task.taskGroupName = tgName
	}
	task.initialize()
	return task
}

func beforeHook(event TaskEventType) string {
	return fmt.Sprintf("before_%s", event)
}

// event handling
func (task *Task) handle(te events.TaskEvent) error {
	task.lock.Lock()
	defer task.lock.Unlock()
	err := task.sm.Event(te.GetEvent(), task, te.GetArgs())
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	return nil
}

func (task *Task) canHandle(te events.TaskEvent) bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.sm.Can(te.GetEvent())
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

func (task *Task) IsPlaceholder() bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.placeholder
}

func (task *Task) GetTaskState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return task.sm.Current()
}

func (task *Task) setTaskGroupName(groupName string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.taskGroupName = groupName
}

func (task *Task) setTaskTerminationType(terminationTyp string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.terminationType = terminationTyp
}

func (task *Task) getTaskGroupName() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.taskGroupName
}

func (task *Task) getTaskAllocationUUID() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.allocationUUID
}

func (task *Task) DeleteTaskPod(pod *v1.Pod) error {
	return task.context.apiProvider.GetAPIs().KubeClient.Delete(task.pod)
}

func (task *Task) UpdateTaskPodStatus(pod *v1.Pod) (*v1.Pod, error) {
	return task.context.apiProvider.GetAPIs().KubeClient.UpdateStatus(pod)
}

func (task *Task) UpdateTaskPod(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error) {
	return task.context.apiProvider.GetAPIs().KubeClient.UpdatePod(pod, podMutator)
}

func (task *Task) isTerminated() bool {
	for _, states := range TaskStates().Terminated {
		if task.GetTaskState() == states {
			return true
		}
	}
	return false
}

// task object initialization
// normally when task is added, the task state is New
// but during recovery, we need to init the task state according to
// the task pod status. if the pod is already terminated,
// we should mark the task as completed according.
func (task *Task) initialize() {
	task.lock.Lock()
	defer task.lock.Unlock()

	// task needs recovery means the task has already been
	// scheduled by us with an allocation, instead of starting
	// from New, directly set the task to Allocated.
	if utils.NeedRecovery(task.pod) {
		task.allocationUUID = string(task.pod.UID)
		task.nodeName = task.pod.Spec.NodeName
		task.sm.SetState(TaskStates().Allocated)
		log.Logger().Info("set task as Allocated",
			zap.String("appID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("allocationUUID", task.allocationUUID),
			zap.String("nodeName", task.nodeName))
	}

	// task already terminated, succeed or failed
	// that means the task was already allocated and completed
	// the resources were already released, instead of starting
	// from New, directly set the task to Completed
	if utils.IsPodTerminated(task.pod) {
		task.allocationUUID = string(task.pod.UID)
		task.nodeName = task.pod.Spec.NodeName
		task.sm.SetState(TaskStates().Completed)
		log.Logger().Info("set task as Completed",
			zap.String("appID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("allocationUUID", task.allocationUUID),
			zap.String("nodeName", task.nodeName))
	}
}

func (task *Task) setAllocated(nodeName, allocationUUID string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.allocationUUID = allocationUUID
	task.nodeName = nodeName
	task.sm.SetState(TaskStates().Allocated)
}

func (task *Task) IsOriginator() bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.originator
}

func (task *Task) handleFailEvent(reason string, err bool) {
	if err {
		dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, reason))
		events.GetRecorder().Eventf(task.pod.DeepCopy(), nil, v1.EventTypeWarning, "SchedulingFailed", "SchedulingFailed",
			"%s scheduling failed, reason: %s", task.alias, reason)
		return
	}
	log.Logger().Error("task failed",
		zap.String("appID", task.applicationID),
		zap.String("taskID", task.taskID),
		zap.String("reason", reason))
}

func (task *Task) handleSubmitTaskEvent() {
	log.Logger().Debug("scheduling pod",
		zap.String("podName", task.pod.Name))
	// convert the request
	rr := common.CreateAllocationRequestForTask(
		task.applicationID,
		task.taskID,
		task.resource,
		task.placeholder,
		task.taskGroupName,
		task.pod,
		task.originator)
	log.Logger().Debug("send update request", zap.String("request", rr.String()))
	if err := task.context.apiProvider.GetAPIs().SchedulerAPI.UpdateAllocation(&rr); err != nil {
		log.Logger().Debug("failed to send scheduling request to scheduler", zap.Error(err))
		return
	}

	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil, v1.EventTypeNormal, "Scheduling", "Scheduling",
		"%s is queued and waiting for allocation", task.alias)
	// if this task belongs to a task group, that means the app has gang scheduling enabled
	// in this case, post an event to indicate the task is being gang scheduled
	if !task.placeholder && task.taskGroupName != "" {
		events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
			v1.EventTypeNormal, "GangScheduling", "GangScheduling",
			"Pod belongs to the taskGroup %s, it will be scheduled as a gang member", task.taskGroupName)
	}
}

// this is called after task reaches PENDING state,
// submit the resource asks from this task to the scheduler core
func (task *Task) postTaskPending() {
	dispatcher.Dispatch(NewSubmitTaskEvent(task.applicationID, task.taskID))
}

// this is called after task reaches ALLOCATED state,
// we run this in a go routine to bind pod to the allocated node,
// if successful, we move task to next state BOUND,
// otherwise we fail the task
func (task *Task) postTaskAllocated(allocUUID string, nodeID string) {
	// delay binding task
	// this calls K8s api to bind a pod to the assigned node, this may need some time,
	// so we do a delay binding to avoid blocking main process. we tracks the result
	// of the binding and properly handle failures.
	go func() {
		// we need to obtain task's lock first,
		// this ensures no other threads modifying task state at the time being
		task.lock.Lock()
		defer task.lock.Unlock()
		var errorMessage string
		// task allocation UID is assigned once we get allocation decision from scheduler core
		task.allocationUUID = allocUUID

		// plugin mode means we delegate this work to the default scheduler
		if task.pluginMode {
			log.Logger().Debug("allocating pod",
				zap.String("podName", task.pod.Name),
				zap.String("podUID", string(task.pod.UID)))

			task.context.AddPendingPodAllocation(string(task.pod.UID), nodeID)

			dispatcher.Dispatch(NewBindTaskEvent(task.applicationID, task.taskID))
			events.GetRecorder().Eventf(task.pod.DeepCopy(),
				nil, v1.EventTypeNormal, "QuotaApproved", "QuotaApproved",
				"Pod %s is ready for scheduling on node %s", task.alias, nodeID)
		} else {
			// post a message to indicate the pod gets its allocation
			events.GetRecorder().Eventf(task.pod.DeepCopy(),
				nil, v1.EventTypeNormal, "Scheduled", "Scheduled",
				"Successfully assigned %s to node %s", task.alias, nodeID)

			// before binding pod to node, first bind volumes to pod
			log.Logger().Debug("bind pod volumes",
				zap.String("podName", task.pod.Name),
				zap.String("podUID", string(task.pod.UID)))
			if task.context.apiProvider.GetAPIs().VolumeBinder != nil {
				if err := task.context.bindPodVolumes(task.pod); err != nil {
					errorMessage = fmt.Sprintf("bind pod volumes failed, name: %s, %s", task.alias, err.Error())
					dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
					events.GetRecorder().Eventf(task.pod.DeepCopy(),
						nil, v1.EventTypeWarning, "PodVolumesBindFailure", "PodVolumesBindFailure", errorMessage)
					return
				}
			}

			log.Logger().Debug("bind pod",
				zap.String("podName", task.pod.Name),
				zap.String("podUID", string(task.pod.UID)))

			if err := task.context.apiProvider.GetAPIs().KubeClient.Bind(task.pod, nodeID); err != nil {
				errorMessage = fmt.Sprintf("bind pod volumes failed, name: %s, %s", task.alias, err.Error())
				log.Logger().Error(errorMessage)
				dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
				events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
					v1.EventTypeWarning, "PodBindFailure", "PodBindFailure", errorMessage)
				return
			}

			log.Logger().Info("successfully bound pod", zap.String("podName", task.pod.Name))
			dispatcher.Dispatch(NewBindTaskEvent(task.applicationID, task.taskID))
			events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
				v1.EventTypeNormal, "PodBindSuccessful", "PodBindSuccessful",
				"Pod %s is successfully bound to node %s", task.alias, nodeID)
		}
	}()
}

// this callback is called before handling the TaskAllocated event,
// when we receive the new allocation from the core, normally the task
// should be in Scheduling state and waiting for the allocation to come.
// but in some cases, the task could be canceled (e.g pod deleted) before
// getting the response. Such task transited to the Completed state.
// If we find the task is already in Completed state while handling TaskAllocated
// event, we need to explicitly release this allocation because it is no
// longer valid.
func (task *Task) beforeTaskAllocated(eventSrc string, allocUUID string, nodeID string) {
	// if task is already completed and we got a TaskAllocated event
	// that means this allocation is no longer valid, we should
	// notify the core to release this allocation to avoid resource leak
	if eventSrc == TaskStates().Completed {
		log.Logger().Info("task is already completed, invalidate the allocation",
			zap.String("currentTaskState", eventSrc),
			zap.String("allocUUID", allocUUID),
			zap.String("allocatedNode", nodeID))
		task.allocationUUID = allocUUID
		task.releaseAllocation()
	}
}

func (task *Task) postTaskBound() {
	if task.pluginMode {
		// when the pod is scheduling by yunikorn, it is moved to the default-scheduler's
		// unschedulable queue, if nothing changes, the pod will be staying in the unschedulable
		// queue for unschedulableQTimeInterval long (default 1 minute). hence, we are updating
		// the pod status explicitly, when there is a status change, the default scheduler will
		// move the pod back to the active queue immediately.
		podCopy := task.pod.DeepCopy()
		if _, err := task.UpdateTaskPod(podCopy, func(pod *v1.Pod) {
			pod.Status = v1.PodStatus{
				Phase:   podCopy.Status.Phase,
				Reason:  "QuotaApproved",
				Message: "pod fits into the queue quota and it is ready for scheduling",
			}

			// this is a bit of a hack, but ensures that the default scheduler detects the pod as having changed
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations["yunikorn.apache.org/scheduled-at"] = strconv.FormatInt(time.Now().UnixNano(), 10)
		}); err != nil {
			log.Logger().Warn("failed to update pod status", zap.Error(err))
		}
	}

	if task.placeholder {
		log.Logger().Info("placeholder is bound",
			zap.String("appID", task.applicationID),
			zap.String("taskName", task.alias),
			zap.String("taskGroupName", task.taskGroupName))
		dispatcher.Dispatch(NewUpdateApplicationReservationEvent(task.applicationID))
	}
}

func (task *Task) postTaskRejected() {
	// currently, once task is rejected by scheduler, we directly move task to failed state.
	// so this function simply triggers the state transition when it is rejected.
	// but further, we can introduce retry mechanism if necessary.
	dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID,
		fmt.Sprintf("task %s failed because it is rejected by scheduler", task.alias)))

	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeWarning, "TaskRejected", "TaskRejected",
		"Task %s is rejected by the scheduler", task.alias)
}

func (task *Task) postTaskFailed() {
	// when task is failed, we need to do the cleanup,
	// we need to release the allocation from scheduler core
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeNormal, "TaskFailed", "TaskFailed",
		"Task %s is failed", task.alias)
}

func (task *Task) beforeTaskCompleted() {
	// before task transits to completed, release its allocation from scheduler core
	// this is done as a before hook because the releaseAllocation() call needs to
	// send different requests to scheduler-core, depending on current task state
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeNormal, "TaskCompleted", "TaskCompleted",
		"Task %s is completed", task.alias)
}

func (task *Task) releaseAllocation() {
	// scheduler api might be nil in some tests
	if task.context.apiProvider.GetAPIs().SchedulerAPI != nil {
		log.Logger().Debug("prepare to send release request",
			zap.String("applicationID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("taskAlias", task.alias),
			zap.String("allocationUUID", task.allocationUUID),
			zap.String("task", task.GetTaskState()),
			zap.String("terminationType", task.terminationType))

		// depends on current task state, generate requests accordingly.
		// if task is already allocated, which means the scheduler core already,
		// places an allocation for it, we need to send AllocationReleaseRequest,
		// if task is not allocated yet, we need to send AllocationAskReleaseRequest
		var releaseRequest si.AllocationRequest
		s := TaskStates()
		switch task.GetTaskState() {
		case s.New, s.Pending, s.Scheduling:
			releaseRequest = common.CreateReleaseAskRequestForTask(
				task.applicationID, task.taskID, task.application.partition)
		default:
			// sending empty allocation UUID back to scheduler-core is dangerous
			// log a warning and skip the release request. this may leak some resource
			// in the scheduler, collect logs and check why this happens.
			if task.allocationUUID == "" {
				log.Logger().Warn("task allocation UUID is empty, sending this release request "+
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
				task.applicationID, task.allocationUUID, task.application.partition, task.terminationType)
		}

		if releaseRequest.Releases != nil {
			log.Logger().Info("releasing allocations",
				zap.Int("numOfAsksToRelease", len(releaseRequest.Releases.AllocationAsksToRelease)),
				zap.Int("numOfAllocationsToRelease", len(releaseRequest.Releases.AllocationsToRelease)))
		}
		if err := task.context.apiProvider.GetAPIs().SchedulerAPI.UpdateAllocation(&releaseRequest); err != nil {
			log.Logger().Debug("failed to send scheduling request to scheduler", zap.Error(err))
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
		log.Logger().Debug("checking PVC", zap.String("name", pvcName))
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
	log.Logger().Debug("shim task state transition",
		zap.String("app", task.applicationID),
		zap.String("task", task.taskID),
		zap.String("taskAlias", task.alias),
		zap.String("source", event.Src),
		zap.String("destination", event.Dst),
		zap.String("event", event.Event))
}
