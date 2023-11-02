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
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type Task struct {
	taskID          string
	alias           string
	applicationID   string
	application     *Application
	allocationUUID  string
	resource        *si.Resource
	pod             *v1.Pod
	podStatus       v1.PodStatus // pod status, maintained separately for efficiency reasons
	context         *Context
	nodeName        string
	createTime      time.Time
	taskGroupName   string
	placeholder     bool
	terminationType string
	originator      bool
	schedulingState interfaces.TaskSchedulingState
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
	task := &Task{
		taskID:          tid,
		alias:           fmt.Sprintf("%s/%s", pod.Namespace, pod.Name),
		applicationID:   app.GetApplicationID(),
		application:     app,
		pod:             pod,
		podStatus:       *pod.Status.DeepCopy(),
		resource:        resource,
		createTime:      pod.GetCreationTimestamp().Time,
		placeholder:     placeholder,
		taskGroupName:   taskGroupName,
		originator:      originator,
		context:         ctx,
		sm:              newTaskState(),
		schedulingState: interfaces.TaskSchedPending,
		lock:            &sync.RWMutex{},
	}
	if tgName := utils.GetTaskGroupFromPodSpec(pod); tgName != "" {
		task.taskGroupName = tgName
	}
	task.initialize()
	return task
}

// event handling
func (task *Task) handle(te events.TaskEvent) error {
	task.lock.Lock()
	defer task.lock.Unlock()
	err := task.sm.Event(context.Background(), te.GetEvent(), task, te.GetArgs())
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
	// from New, directly set the task to Bound.
	if utils.NeedRecovery(task.pod) {
		task.allocationUUID = string(task.pod.UID)
		task.nodeName = task.pod.Spec.NodeName
		task.sm.SetState(TaskStates().Bound)
		log.Log(log.ShimCacheTask).Info("set task as Bound",
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
		log.Log(log.ShimCacheTask).Info("set task as Completed",
			zap.String("appID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("allocationUUID", task.allocationUUID),
			zap.String("nodeName", task.nodeName))
	}
}

func (task *Task) IsOriginator() bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.originator
}

func (task *Task) isPreemptSelfAllowed() bool {
	value := utils.GetPodAnnotationValue(task.pod, constants.AnnotationAllowPreemption)
	switch value {
	case constants.True:
		return true
	case constants.False:
		return false
	default:
		return task.context.IsPreemptSelfAllowed(task.pod.Spec.PriorityClassName)
	}
}

func (task *Task) isPreemptOtherAllowed() bool {
	policy := task.pod.Spec.PreemptionPolicy
	if policy == nil {
		return true
	}
	switch *policy {
	case v1.PreemptNever:
		return false
	case v1.PreemptLowerPriority:
		return true
	default:
		return true
	}
}

func (task *Task) SetTaskSchedulingState(state interfaces.TaskSchedulingState) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.schedulingState = state
}

func (task *Task) GetTaskSchedulingState() interfaces.TaskSchedulingState {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.schedulingState
}

func (task *Task) handleSubmitTaskEvent() {
	log.Log(log.ShimCacheTask).Debug("scheduling pod",
		zap.String("podName", task.pod.Name))

	// build preemption policy
	preemptionPolicy := &si.PreemptionPolicy{
		AllowPreemptSelf:  task.isPreemptSelfAllowed(),
		AllowPreemptOther: task.isPreemptOtherAllowed(),
	}

	// convert the request
	rr := common.CreateAllocationRequestForTask(
		task.applicationID,
		task.taskID,
		task.resource,
		task.placeholder,
		task.taskGroupName,
		task.pod,
		task.originator,
		preemptionPolicy)
	log.Log(log.ShimCacheTask).Debug("send update request", zap.Stringer("request", rr))
	if err := task.context.apiProvider.GetAPIs().SchedulerAPI.UpdateAllocation(rr); err != nil {
		log.Log(log.ShimCacheTask).Debug("failed to send scheduling request to scheduler", zap.Error(err))
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

// postTaskAllocated is called after task reaches ALLOCATED state.
// This routine binds the pod to the allocated node.
// It calls K8s api to bind a pod to the assigned node, this may need some time,
// so we do a delay binding, background process, to avoid blocking main process.
// The result of the binding is tracked and failures are properly handled.
// If successful, we move task to next state BOUND, otherwise we fail the task
func (task *Task) postTaskAllocated() {
	go func() {
		// we need to obtain task's lock first,
		// this ensures no other threads modifying task state at the time being
		task.lock.Lock()
		defer task.lock.Unlock()

		// plugin mode means we delegate this work to the default scheduler
		if utils.IsPluginMode() {
			log.Log(log.ShimCacheTask).Debug("allocating pod",
				zap.String("podName", task.pod.Name),
				zap.String("podUID", string(task.pod.UID)))

			task.context.AddPendingPodAllocation(string(task.pod.UID), task.nodeName)

			dispatcher.Dispatch(NewBindTaskEvent(task.applicationID, task.taskID))
			events.GetRecorder().Eventf(task.pod.DeepCopy(),
				nil, v1.EventTypeNormal, "QuotaApproved", "QuotaApproved",
				"Pod %s is ready for scheduling on node %s", task.alias, task.nodeName)
		} else {
			// post a message to indicate the pod gets its allocation
			events.GetRecorder().Eventf(task.pod.DeepCopy(),
				nil, v1.EventTypeNormal, "Scheduled", "Scheduled",
				"Successfully assigned %s to node %s", task.alias, task.nodeName)

			// before binding pod to node, first bind volumes to pod
			log.Log(log.ShimCacheTask).Debug("bind pod volumes",
				zap.String("podName", task.pod.Name),
				zap.String("podUID", string(task.pod.UID)))
			if task.context.apiProvider.GetAPIs().VolumeBinder != nil {
				if err := task.context.bindPodVolumes(task.pod); err != nil {
					errorMessage := fmt.Sprintf("bind volumes to pod failed, name: %s, %s", task.alias, err.Error())
					dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
					events.GetRecorder().Eventf(task.pod.DeepCopy(),
						nil, v1.EventTypeWarning, "PodVolumesBindFailure", "PodVolumesBindFailure", errorMessage)
					return
				}
			}

			log.Log(log.ShimCacheTask).Debug("bind pod",
				zap.String("podName", task.pod.Name),
				zap.String("podUID", string(task.pod.UID)))

			if err := task.context.apiProvider.GetAPIs().KubeClient.Bind(task.pod, task.nodeName); err != nil {
				errorMessage := fmt.Sprintf("bind pod to node failed, name: %s, %s", task.alias, err.Error())
				log.Log(log.ShimCacheTask).Error(errorMessage)
				dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
				events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
					v1.EventTypeWarning, "PodBindFailure", "PodBindFailure", errorMessage)
				return
			}

			log.Log(log.ShimCacheTask).Info("successfully bound pod", zap.String("podName", task.pod.Name))
			dispatcher.Dispatch(NewBindTaskEvent(task.applicationID, task.taskID))
			events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
				v1.EventTypeNormal, "PodBindSuccessful", "PodBindSuccessful",
				"Pod %s is successfully bound to node %s", task.alias, task.nodeName)
		}

		task.schedulingState = interfaces.TaskSchedAllocated
	}()
}

// beforeTaskAllocated is called before handling the TaskAllocated event.
// This sets the allocation information returned by the core in the task.
// In some cases, the task is canceled (e.g. pod deleted) before we process the allocation
// from the core. Those task will already be in the Completed state.
// If we find the task is already in Completed state while handling TaskAllocated
// event, we need to explicitly release this allocation because it is no
// longer valid.
func (task *Task) beforeTaskAllocated(eventSrc string, allocUUID string, nodeID string) {
	// task is allocated on a node with a UUID set the details in the task here to allow referencing later.
	task.allocationUUID = allocUUID
	task.nodeName = nodeID
	// If the task is Completed the pod was deleted on K8s but the core was not aware yet.
	// Notify the core to release this allocation to avoid resource leak.
	// The ask is not relevant at this point.
	if eventSrc == TaskStates().Completed {
		log.Log(log.ShimCacheTask).Info("task is already completed, invalidate the allocation",
			zap.String("currentTaskState", eventSrc),
			zap.String("allocUUID", allocUUID),
			zap.String("allocatedNode", nodeID))
		task.releaseAllocation()
	}
}

func (task *Task) postTaskBound() {
	if utils.IsPluginMode() {
		// When the pod is actively scheduled by YuniKorn, it can be  moved to the default-scheduler's
		// UnschedulablePods structure. If the pod does not change, the pod will stay in the UnschedulablePods
		// structure for podMaxInUnschedulablePodsDuration (default 5 minutes). Here we update the pod
		// explicitly to move it back to the active queue.
		// See: pkg/scheduler/internal/queue/scheduling_queue.go:isPodUpdated() for what is considered updated.
		podCopy := task.pod.DeepCopy()
		if _, err := task.UpdateTaskPod(podCopy, func(pod *v1.Pod) {
			// Ensure that the default scheduler detects the pod as having changed
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations[constants.DomainYuniKorn+"scheduled-at"] = strconv.FormatInt(time.Now().UnixNano(), 10)
		}); err != nil {
			log.Log(log.ShimCacheTask).Warn("failed to update pod status", zap.Error(err))
		}
	}

	if task.placeholder {
		log.Log(log.ShimCacheTask).Info("placeholder is bound",
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

// beforeTaskFail releases the allocation or ask from scheduler core
// this is done as a before hook because the releaseAllocation() call needs to
// send different requests to scheduler-core, depending on current task state
func (task *Task) beforeTaskFail() {
	task.releaseAllocation()
}

func (task *Task) postTaskFailed(reason string) {
	log.Log(log.ShimCacheTask).Error("task failed",
		zap.String("appID", task.applicationID),
		zap.String("taskID", task.taskID),
		zap.String("reason", reason))
	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeNormal, "TaskFailed", "TaskFailed",
		"Task %s is failed", task.alias)
}

// beforeTaskCompleted releases the allocation or ask from scheduler core
// this is done as a before hook because the releaseAllocation() call needs to
// send different requests to scheduler-core, depending on current task state
func (task *Task) beforeTaskCompleted() {
	task.releaseAllocation()

	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeNormal, "TaskCompleted", "TaskCompleted",
		"Task %s is completed", task.alias)
}

// releaseAllocation sends the release request for the Allocation or the AllocationAsk to the core.
func (task *Task) releaseAllocation() {
	// scheduler api might be nil in some tests
	if task.context.apiProvider.GetAPIs().SchedulerAPI != nil {
		log.Log(log.ShimCacheTask).Debug("prepare to send release request",
			zap.String("applicationID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("taskAlias", task.alias),
			zap.String("allocationUUID", task.allocationUUID),
			zap.String("task", task.GetTaskState()),
			zap.String("terminationType", task.terminationType))

		// The message depends on current task state, generate requests accordingly.
		// If allocated send an AllocationReleaseRequest,
		// If not allocated yet send an AllocationAskReleaseRequest
		var releaseRequest *si.AllocationRequest
		s := TaskStates()
		switch task.GetTaskState() {
		case s.New, s.Pending, s.Scheduling, s.Rejected:
			releaseRequest = common.CreateReleaseAskRequestForTask(
				task.applicationID, task.taskID, task.application.partition)
		default:
			if task.allocationUUID == "" {
				log.Log(log.ShimCacheTask).Warn("BUG: task allocation UUID is empty on release",
					zap.String("applicationID", task.applicationID),
					zap.String("taskID", task.taskID),
					zap.String("taskAlias", task.alias),
					zap.String("task", task.GetTaskState()))
				return
			}
			releaseRequest = common.CreateReleaseAllocationRequestForTask(
				task.applicationID, task.allocationUUID, task.application.partition, task.terminationType)
		}

		if releaseRequest.Releases != nil {
			log.Log(log.ShimCacheTask).Info("releasing allocations",
				zap.Int("numOfAsksToRelease", len(releaseRequest.Releases.AllocationAsksToRelease)),
				zap.Int("numOfAllocationsToRelease", len(releaseRequest.Releases.AllocationsToRelease)))
		}
		if err := task.context.apiProvider.GetAPIs().SchedulerAPI.UpdateAllocation(releaseRequest); err != nil {
			log.Log(log.ShimCacheTask).Debug("failed to send scheduling request to scheduler", zap.Error(err))
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
		log.Log(log.ShimCacheTask).Debug("checking PVC", zap.String("name", pvcName))
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

func (task *Task) UpdatePodCondition(podCondition *v1.PodCondition) (bool, *v1.Pod) {
	task.lock.Lock()
	defer task.lock.Unlock()

	status := task.podStatus.DeepCopy()
	pod := task.pod.DeepCopy()
	pod.Status = *status
	if !utils.PodUnderCondition(pod, podCondition) {
		log.Log(log.ShimContext).Debug("updating pod condition",
			zap.String("namespace", task.pod.Namespace),
			zap.String("name", task.pod.Name),
			zap.Any("podCondition", podCondition))
		if podutil.UpdatePodCondition(&task.podStatus, podCondition) {
			status = task.podStatus.DeepCopy()
			pod.Status = *status
			return true, pod
		}
	}

	return false, pod
}
