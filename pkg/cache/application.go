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
	"sort"
	"strings"
	"sync"

	"github.com/looplab/fsm"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type Application struct {
	applicationID              string
	queue                      string
	partition                  string
	user                       string
	taskMap                    map[string]*Task
	tags                       map[string]string
	schedulingPolicy           v1alpha1.SchedulingPolicy
	taskGroups                 []v1alpha1.TaskGroup
	taskGroupsDefinition       string
	schedulingParamsDefinition string
	placeholderOwnerReferences []metav1.OwnerReference
	stateMachine               *fsm.FSM
	lock                       *sync.RWMutex
	schedulerAPI               api.SchedulerAPI
	placeholderAsk             *si.Resource // total placeholder request for the app (all task groups)
	placeholderTimeoutInSec    int64
	schedulingStyle            string
}

func (app *Application) String() string {
	return fmt.Sprintf("applicationID: %s, queue: %s, partition: %s,"+
		" totalNumOfTasks: %d, currentState: %s",
		app.applicationID, app.queue, app.partition, len(app.taskMap), app.GetApplicationState())
}

func NewApplication(appID, queueName, user string, tags map[string]string, scheduler api.SchedulerAPI) *Application {
	taskMap := make(map[string]*Task)
	app := &Application{
		applicationID:           appID,
		queue:                   queueName,
		partition:               constants.DefaultPartition,
		user:                    user,
		taskMap:                 taskMap,
		tags:                    tags,
		schedulingPolicy:        v1alpha1.SchedulingPolicy{},
		stateMachine:            NewAppState(),
		taskGroups:              make([]v1alpha1.TaskGroup, 0),
		lock:                    &sync.RWMutex{},
		schedulerAPI:            scheduler,
		placeholderTimeoutInSec: 0,
		schedulingStyle:         constants.SchedulingPolicyStyleParamDefault,
	}
	return app
}

func (app *Application) handleApplicationEvent(event applicationEvent) error {
	// Locking mechanism:
	// 1) when handle event transitions, we first obtain the object's lock,
	//    this helps us to place a pre-check before entering here, in case
	//    we receive some invalidate events. If this introduces performance
	//    regression, a possible way to optimize is to use a separate lock
	//    to protect the transition phase.
	// 2) Note, state machine calls those callbacks here, we must ensure
	//    they are lock-free calls. Otherwise the callback will be blocked
	//    because the lock is already held here.
	app.lock.Lock()
	defer app.lock.Unlock()
	err := app.stateMachine.Event(event.String(), app)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	return nil
}

func (app *Application) handleApplicationEventWithInfo(event applicationEvent, eventnfo []string) error {
	// Locking mechanism:
	// 1) when handle event transitions, we first obtain the object's lock,
	//    this helps us to place a pre-check before entering here, in case
	//    we receive some invalidate events. If this introduces performance
	//    regression, a possible way to optimize is to use a separate lock
	//    to protect the transition phase.
	// 2) Note, state machine calls those callbacks here, we must ensure
	//    they are lock-free calls. Otherwise the callback will be blocked
	//    because the lock is already held here.
	app.lock.Lock()
	defer app.lock.Unlock()
	err := app.stateMachine.Event(event.String(), app, eventnfo)
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	return nil
}

func (app *Application) canHandle(ev events.ApplicationEvent) bool {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.stateMachine.Can(string(ev.GetEvent()))
}

func (app *Application) GetTask(taskID string) (interfaces.ManagedTask, error) {
	app.lock.RLock()
	defer app.lock.RUnlock()
	if task, ok := app.taskMap[taskID]; ok {
		return task, nil
	}
	return nil, fmt.Errorf("task %s doesn't exist in application %s",
		taskID, app.applicationID)
}

func (app *Application) GetApplicationID() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.applicationID
}

func (app *Application) GetQueue() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.queue
}

func (app *Application) GetUser() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.user
}

func (app *Application) setSchedulingPolicy(policy v1alpha1.SchedulingPolicy) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.schedulingPolicy = policy
}

func (app *Application) getSchedulingPolicy() v1alpha1.SchedulingPolicy {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.schedulingPolicy
}

func (app *Application) setTaskGroupsDefinition(taskGroupsDef string) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.taskGroupsDefinition = taskGroupsDef
}

func (app *Application) GetTaskGroupsDefinition() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.taskGroupsDefinition
}

func (app *Application) setSchedulingParamsDefinition(schedParamsDef string) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.schedulingParamsDefinition = schedParamsDef
}

func (app *Application) GetSchedulingParamsDefinition() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.schedulingParamsDefinition
}

func (app *Application) setTaskGroups(taskGroups []v1alpha1.TaskGroup) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.taskGroups = taskGroups
	for _, taskGroup := range app.taskGroups {
		app.placeholderAsk = common.Add(app.placeholderAsk, common.GetTGResource(taskGroup.MinResource, int64(taskGroup.MinMember)))
	}
}

func (app *Application) getPlaceholderAsk() *si.Resource {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.placeholderAsk
}

func (app *Application) getTaskGroups() []v1alpha1.TaskGroup {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.taskGroups
}

func (app *Application) setOwnReferences(ref []metav1.OwnerReference) {
	app.lock.RLock()
	defer app.lock.RUnlock()
	app.placeholderOwnerReferences = ref
}

func (app *Application) setSchedulingStyle(schedulingStyle string) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.schedulingStyle = schedulingStyle
}

func (app *Application) addTask(task *Task) {
	app.lock.Lock()
	defer app.lock.Unlock()
	if _, ok := app.taskMap[task.taskID]; ok {
		// skip adding duplicate task
		return
	}
	app.taskMap[task.taskID] = task
}

func (app *Application) removeTask(taskID string) {
	app.lock.Lock()
	defer app.lock.Unlock()
	if _, ok := app.taskMap[taskID]; !ok {
		log.Logger().Debug("Attempted to remove non-existent task", zap.String("taskID", taskID))
		return
	}
	delete(app.taskMap, taskID)
	log.Logger().Info("task removed",
		zap.String("appID", app.applicationID),
		zap.String("taskID", taskID))
}

func (app *Application) GetApplicationState() string {
	return app.stateMachine.Current()
}

func (app *Application) GetPendingTasks() []*Task {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.getTasks(events.States().Task.Pending)
}

func (app *Application) GetNewTasks() []*Task {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.getTasks(events.States().Task.New)
}

func (app *Application) GetAllocatedTasks() []*Task {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.getTasks(events.States().Task.Allocated)
}

func (app *Application) getTasks(state string) []*Task {
	taskList := make([]*Task, 0)
	if len(app.taskMap) > 0 {
		for _, task := range app.taskMap {
			if task.GetTaskState() == state {
				taskList = append(taskList, task)
			}
		}
	}

	// sort the task based on creation time
	sort.Slice(taskList, func(i, j int) bool {
		l := taskList[i]
		r := taskList[j]
		return l.createTime.Before(r.createTime)
	})

	return taskList
}

func (app *Application) GetTags() map[string]string {
	return app.tags
}

func (app *Application) getNonTerminatedTaskAlias() []string {
	var nonTerminatedTaskAlias []string
	for _, task := range app.taskMap {
		if !task.isTerminated() {
			nonTerminatedTaskAlias = append(nonTerminatedTaskAlias, task.alias)
		}
	}
	return nonTerminatedTaskAlias
}

// SetState is only for testing
// this is just used for testing, it is not supposed to change state like this
func (app *Application) SetState(state string) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.stateMachine.SetState(state)
}

func (app *Application) TriggerAppRecovery() error {
	return app.handleApplicationEvent(RecoverApplication)
}

// Schedule is called in every scheduling interval,
// we are not using dispatcher here because we want to
// make state transition in sync mode in order to prevent
// generating too many duplicate events. However, it must
// ensure non of these calls is expensive, usually, they
// do nothing more than just triggering the state transition.
// return true if the app needs scheduling or false if not
func (app *Application) Schedule() bool {
	switch app.GetApplicationState() {
	case New.String():
		if err := app.handleApplicationEvent(SubmitApplication); err != nil {
			log.Logger().Warn("failed to handle SUBMIT app event",
				zap.Error(err))
		}
	case Accepted.String():
		// once the app is accepted by the scheduler core,
		// the next step is to send requests for scheduling
		// the app state could be transited to Reserving or Running
		// depends on if the app has gang members
		app.postAppAccepted()
	case Reserving.String():
		// during the Reserving state, only the placeholders
		// can be scheduled
		app.scheduleTasks(func(t *Task) bool {
			return t.placeholder
		})
		if len(app.GetNewTasks()) == 0 {
			return false
		}
	case Running.String():
		// during the Running state, only the regular pods
		// can be scheduled
		app.scheduleTasks(func(t *Task) bool {
			return !t.placeholder
		})
		if len(app.GetNewTasks()) == 0 {
			return false
		}
	default:
		log.Logger().Debug("skipping scheduling application",
			zap.String("appID", app.GetApplicationID()),
			zap.String("appState", app.GetApplicationState()))
		return false
	}
	return true
}

func (app *Application) scheduleTasks(taskScheduleCondition func(t *Task) bool) {
	for _, task := range app.GetNewTasks() {
		if taskScheduleCondition(task) {
			// for each new task, we do a sanity check before moving the state to Pending_Schedule
			if err := task.sanityCheckBeforeScheduling(); err == nil {
				// note, if we directly trigger submit task event, it may spawn too many duplicate
				// events, because a task might be submitted multiple times before its state transits to PENDING.
				if handleErr := task.handle(
					NewSimpleTaskEvent(task.applicationID, task.taskID, events.InitTask)); handleErr != nil {
					// something goes wrong when transit task to PENDING state,
					// this should not happen because we already checked the state
					// before calling the transition. Nowhere to go, just log the error.
					log.Logger().Warn("init task failed", zap.Error(err))
				}
			} else {
				events.GetRecorder().Eventf(task.GetTaskPod(), nil, v1.EventTypeWarning, "FailedScheduling", "FailedScheduling", err.Error())
				log.Logger().Debug("task is not ready for scheduling",
					zap.String("appID", task.applicationID),
					zap.String("taskID", task.taskID),
					zap.Error(err))
			}
		}
	}
}

func (app *Application) handleSubmitApplicationEvent() {
	log.Logger().Info("handle app submission",
		zap.String("app", app.String()),
		zap.String("clusterID", conf.GetSchedulerConf().ClusterID))
	err := app.schedulerAPI.UpdateApplication(
		&si.ApplicationRequest{
			New: []*si.AddApplicationRequest{
				{
					ApplicationID: app.applicationID,
					QueueName:     app.queue,
					PartitionName: app.partition,
					Ugi: &si.UserGroupInformation{
						User: app.user,
					},
					Tags:                         app.tags,
					PlaceholderAsk:               app.placeholderAsk,
					ExecutionTimeoutMilliSeconds: app.placeholderTimeoutInSec * 1000,
					GangSchedulingStyle:          app.schedulingStyle,
				},
			},
			RmID: conf.GetSchedulerConf().ClusterID,
		})

	if err != nil {
		// submission failed
		log.Logger().Warn("failed to submit app", zap.Error(err))
		dispatcher.Dispatch(NewFailApplicationEvent(app.applicationID, err.Error()))
	}
}

func (app *Application) handleRecoverApplicationEvent() {
	log.Logger().Info("handle app recovering",
		zap.String("app", app.String()),
		zap.String("clusterID", conf.GetSchedulerConf().ClusterID))
	err := app.schedulerAPI.UpdateApplication(
		&si.ApplicationRequest{
			New: []*si.AddApplicationRequest{
				{
					ApplicationID: app.applicationID,
					QueueName:     app.queue,
					PartitionName: app.partition,
					Ugi: &si.UserGroupInformation{
						User: app.user,
					},
					Tags:                         app.tags,
					PlaceholderAsk:               app.placeholderAsk,
					ExecutionTimeoutMilliSeconds: app.placeholderTimeoutInSec * 1000,
					GangSchedulingStyle:          app.schedulingStyle,
				},
			},
			RmID: conf.GetSchedulerConf().ClusterID,
		})

	if err != nil {
		// recovery failed
		log.Logger().Warn("failed to recover app", zap.Error(err))
		dispatcher.Dispatch(NewFailApplicationEvent(app.applicationID, err.Error()))
	}
}

func (app *Application) skipReservationStage() bool {
	// no task groups defined, skip reservation
	if len(app.taskGroups) == 0 {
		log.Logger().Debug("Skip reservation stage: no task groups defined",
			zap.String("appID", app.applicationID))
		return true
	}

	// if there is any task already passed New state,
	// that means the scheduler has already tried to schedule it
	// in this case, we should skip the reservation stage
	if len(app.taskMap) > 0 {
		for _, task := range app.taskMap {
			if task.GetTaskState() != events.States().Task.New {
				log.Logger().Debug("Skip reservation stage: found task already has been scheduled before.",
					zap.String("appID", app.applicationID),
					zap.String("taskID", task.GetTaskID()),
					zap.String("taskState", task.GetTaskState()))
				return true
			}
		}
	}
	return false
}

func (app *Application) postAppAccepted() {
	// if app has taskGroups defined, and it has no allocated tasks,
	// it goes to the Reserving state before getting to Running.
	// app could have allocated tasks upon a recovery, and in that case,
	// the reserving phase has already passed, no need to trigger that again.
	var ev events.SchedulingEvent
	log.Logger().Debug("postAppAccepted on cached app",
		zap.String("appID", app.applicationID),
		zap.Int("numTaskGroups", len(app.taskGroups)),
		zap.Int("numAllocatedTasks", len(app.getTasks(events.States().Task.Allocated))))
	if app.skipReservationStage() {
		ev = NewRunApplicationEvent(app.applicationID)
		log.Logger().Info("Skip the reservation stage",
			zap.String("appID", app.applicationID))
	} else {
		ev = NewSimpleApplicationEvent(app.applicationID, TryReserve.String())
		log.Logger().Info("app has taskGroups defined, trying to reserve resources for gang members",
			zap.String("appID", app.applicationID))
	}
	dispatcher.Dispatch(ev)
}

func (app *Application) onReserving(event *fsm.Event) {
	go func() {
		// while doing reserving
		if err := getPlaceholderManager().createAppPlaceholders(app); err != nil {
			// creating placeholder failed
			// put the app into recycling queue and turn the app to running state
			getPlaceholderManager().cleanUp(app)
			ev := NewRunApplicationEvent(app.applicationID)
			dispatcher.Dispatch(ev)
		}
	}()
}

func (app *Application) onReservationStateChange(event *fsm.Event) {
	// this event is called when there is a add or release of placeholders
	desireCounts := utils.NewTaskGroupInstanceCountMap()
	for _, tg := range app.taskGroups {
		desireCounts.Add(tg.Name, tg.MinMember)
	}

	actualCounts := utils.NewTaskGroupInstanceCountMap()
	for _, t := range app.getTasks(events.States().Task.Bound) {
		if t.placeholder {
			actualCounts.AddOne(t.taskGroupName)
		}
	}

	// min member all satisfied
	if desireCounts.Equals(actualCounts) {
		ev := NewRunApplicationEvent(app.applicationID)
		dispatcher.Dispatch(ev)
	}
}

func (app *Application) handleRejectApplicationEvent(event *fsm.Event) {
	log.Logger().Info("app is rejected by scheduler", zap.String("appID", app.applicationID))
	eventArgs := make([]string, 1)
	if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
		log.Logger().Error("fail to parse event arg", zap.Error(err))
		return
	}
	reason := eventArgs[0]
	// for rejected apps, we directly move them to failed state
	dispatcher.Dispatch(NewFailApplicationEvent(app.applicationID,
		fmt.Sprintf("%s: %s", constants.ApplicationRejectedFailure, reason)))
}

func (app *Application) handleCompleteApplicationEvent(event *fsm.Event) {
	// TODO app lifecycle updates
	go func() {
		getPlaceholderManager().cleanUp(app)
	}()
}

func failTaskPodWithReasonAndMsg(task *Task, reason string, msg string) {
	podCopy := task.GetTaskPod().DeepCopy()
	podCopy.Status = v1.PodStatus{
		Phase:   v1.PodFailed,
		Reason:  reason,
		Message: msg,
	}
	log.Logger().Info("setting pod to failed", zap.String("podName", task.GetTaskPod().Name))
	pod, err := task.UpdateTaskPodStatus(podCopy)
	if err != nil {
		log.Logger().Error("failed to update task pod status", zap.Error(err))
	} else {
		log.Logger().Info("new pod status", zap.String("status", string(pod.Status.Phase)))
	}
}

func (app *Application) handleFailApplicationEvent(event *fsm.Event) {
	go func() {
		getPlaceholderManager().cleanUp(app)
	}()
	eventArgs := make([]string, 1)
	if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
		log.Logger().Error("fail to parse event arg", zap.Error(err))
		return
	}
	errMsg := eventArgs[0]
	log.Logger().Info("failApplication reason", zap.String("applicationID", app.applicationID), zap.String("errMsg", errMsg))
	// unallocated task states include New, Pending and Scheduling
	unalloc := app.getTasks(events.States().Task.New)
	unalloc = append(unalloc, app.getTasks(events.States().Task.Pending)...)
	unalloc = append(unalloc, app.getTasks(events.States().Task.Scheduling)...)

	// publish pod level event to unallocated pods
	for _, task := range unalloc {
		// Only need to fail the non-placeholder pod(s)
		if strings.Contains(errMsg, constants.ApplicationInsufficientResourcesFailure) {
			failTaskPodWithReasonAndMsg(task, constants.ApplicationInsufficientResourcesFailure, "Scheduling has timed out due to insufficient resources")
		} else if strings.Contains(errMsg, constants.ApplicationRejectedFailure) {
			errMsgArr := strings.Split(errMsg, ":")
			failTaskPodWithReasonAndMsg(task, constants.ApplicationRejectedFailure, errMsgArr[1])
		}
		events.GetRecorder().Eventf(task.GetTaskPod(), nil, v1.EventTypeWarning, "ApplicationFailed", "ApplicationFailed",
			"Application %s scheduling failed, reason: %s", app.applicationID, errMsg)
	}
}

func (app *Application) handleReleaseAppAllocationEvent(event *fsm.Event) {
	eventArgs := make([]string, 2)
	if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
		log.Logger().Error("fail to parse event arg", zap.Error(err))
		return
	}
	allocUUID := eventArgs[0]
	terminationTypeStr := eventArgs[1]
	log.Logger().Info("try to release pod from application",
		zap.String("appID", app.applicationID),
		zap.String("allocationUUID", allocUUID),
		zap.String("terminationType", terminationTypeStr))

	for _, task := range app.taskMap {
		if task.allocationUUID == allocUUID {
			task.setTaskTerminationType(terminationTypeStr)
			err := task.DeleteTaskPod(task.pod)
			if err != nil {
				log.Logger().Error("failed to release allocation from application", zap.Error(err))
			}
		}
	}
}

func (app *Application) handleReleaseAppAllocationAskEvent(event *fsm.Event) {
	eventArgs := make([]string, 2)
	if err := events.GetEventArgsAsStrings(eventArgs, event.Args); err != nil {
		log.Logger().Error("fail to parse event arg", zap.Error(err))
		return
	}
	taskID := eventArgs[0]
	terminationTypeStr := eventArgs[1]
	log.Logger().Info("try to release pod from application",
		zap.String("appID", app.applicationID),
		zap.String("taskID", taskID),
		zap.String("terminationType", terminationTypeStr))
	if task, ok := app.taskMap[taskID]; ok {
		task.setTaskTerminationType(terminationTypeStr)
		if task.IsPlaceholder() {
			err := task.DeleteTaskPod(task.pod)
			if err != nil {
				log.Logger().Error("failed to release allocation ask from application", zap.Error(err))
			}
		} else {
			log.Logger().Warn("skip to release allocation ask, ask is not a placeholder",
				zap.String("appID", app.applicationID),
				zap.String("taskID", taskID))
		}
	} else {
		log.Logger().Warn("task not found",
			zap.String("appID", app.applicationID),
			zap.String("taskID", taskID))
	}
}

func (app *Application) handleAppTaskCompletedEvent(event *fsm.Event) {
	for _, task := range app.taskMap {
		if task.placeholder && task.GetTaskState() != events.States().Task.Completed {
			return
		}
	}
	log.Logger().Info("Resuming completed, start to run the app",
		zap.String("appID", app.applicationID))
	dispatcher.Dispatch(NewRunApplicationEvent(app.applicationID))
}

func (app *Application) SetPlaceholderTimeout(timeout int64) {
	app.lock.Lock()
	defer app.lock.Unlock()
	app.placeholderTimeoutInSec = timeout
}
