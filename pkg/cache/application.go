/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/common"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/dispatcher"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
)

type Application struct {
	applicationId string
	queue         string
	partition     string
	user          string
	taskMap       map[string]*Task
	tags          map[string]string
	sm            *fsm.FSM
	lock          *sync.RWMutex
	ch            CompletionHandler
	schedulerApi  api.SchedulerApi
}

func (app *Application) String() string {
	return fmt.Sprintf("applicationId: %s, queue: %s, partition: %s,"+
		" totalNumOfTasks: %d, currentState: %s",
		app.applicationId, app.queue, app.partition, len(app.taskMap), app.GetApplicationState())
}

func NewApplication(appId, queueName, user string, tags map[string]string, scheduler api.SchedulerApi) *Application {
	taskMap := make(map[string]*Task)
	app := &Application{
		applicationId: appId,
		queue:         queueName,
		partition:     common.DefaultPartition,
		user:          user,
		taskMap:       taskMap,
		tags:          tags,
		lock:          &sync.RWMutex{},
		ch:            CompletionHandler{running: false},
		schedulerApi:  scheduler,
	}

	var states = events.States().Application
	app.sm = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: string(events.SubmitApplication),
				Src: []string{states.New},
				Dst: states.Submitted},
			{Name: string(events.RecoverApplication),
				Src: []string{states.New},
				Dst: states.Recovering},
			{Name: string(events.AcceptApplication),
				Src: []string{states.Submitted, states.Recovering},
				Dst: states.Accepted},
			{Name: string(events.RunApplication),
				Src: []string{states.Accepted, states.Running},
				Dst: states.Running},
			{Name: string(events.CompleteApplication),
				Src: []string{states.Running},
				Dst: states.Completed},
			{Name: string(events.RejectApplication),
				Src: []string{states.Submitted},
				Dst: states.Rejected},
			{Name: string(events.FailApplication),
				Src: []string{states.Submitted, states.Rejected, states.Accepted, states.Running},
				Dst: states.Failed},
			{Name: string(events.KillApplication),
				Src: []string{states.Accepted, states.Running},
				Dst: states.Killing},
			{Name: string(events.KilledApplication),
				Src: []string{states.Killing},
				Dst: states.Killed},
		},
		fsm.Callbacks{
			//"enter_state":               app.handleTaskStateChange,
			string(events.SubmitApplication):   app.handleSubmitApplicationEvent,
			string(events.RecoverApplication):  app.handleRecoverApplicationEvent,
			string(events.RunApplication):      app.handleRunApplicationEvent,
			string(events.RejectApplication):   app.handleRejectApplicationEvent,
			string(events.CompleteApplication): app.handleCompleteApplicationEvent,
		},
	)

	return app
}

func (app *Application) handle(ev events.ApplicationEvent) error {
	// state machine has its instinct lock, we don't need to hold the app lock here
	// because the callbacks are the places where might modify app state, not here.
	log.Logger.Debug("application state transition",
		zap.String("appId", app.applicationId),
		zap.String("preState", app.sm.Current()),
		zap.String("pendingEvent", string(ev.GetEvent())))
	if err := app.sm.Event(string(ev.GetEvent()), ev.GetArgs()...); err != nil {
		return err
	}
	log.Logger.Debug("application state transition",
		zap.String("appId", app.applicationId),
		zap.String("postState", app.sm.Current()),
		zap.String("handledEvent", string(ev.GetEvent())))
	return nil
}

func (app *Application) GetTask(taskId string) (*Task, error) {
	app.lock.RLock()
	defer app.lock.RUnlock()
	if task, ok := app.taskMap[taskId]; ok {
		return task, nil
	}
	return nil, fmt.Errorf("task %s doesn't exist in application %s",
		taskId, app.applicationId)
}

func (app *Application) GetApplicationId() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.applicationId
}

func (app *Application) GetQueue() string {
	app.lock.RLock()
	defer app.lock.RUnlock()
	return app.queue
}

func (app *Application) AddTask(task *Task) {
	app.lock.Lock()
	defer app.lock.Unlock()
	if _, ok := app.taskMap[task.taskId]; ok {
		// skip adding duplicate task
		return
	}
	app.taskMap[task.taskId] = task
}

func (app *Application) GetApplicationState() string {
	return app.sm.Current()
}

func (app *Application) GetPendingTasks() []*Task {
	return app.getTasks(events.States().Task.Pending)
}

func (app *Application) GetAllocatedTasks() []*Task {
	return app.getTasks(events.States().Task.Allocated)
}

func (app *Application) getTasks(state string) []*Task {
	app.lock.RLock()
	defer app.lock.RUnlock()

	taskList := make([]*Task, 0)
	if len(app.taskMap) > 0 {
		for _, task := range app.taskMap {
			if task.GetTaskState() == state {
				taskList = append(taskList, task)
			}
		}
	}
	return taskList
}

func (app *Application) handleSubmitApplicationEvent(event *fsm.Event) {
	log.Logger.Info("handle app submission",
		zap.String("app", app.String()),
		zap.String("clusterId", conf.GetSchedulerConf().ClusterId))
	err := app.schedulerApi.Update(
		&si.UpdateRequest{
			NewApplications: []*si.AddApplicationRequest{
				{
					ApplicationId: app.applicationId,
					QueueName:     app.queue,
					PartitionName: app.partition,
					Ugi:           &si.UserGroupInformation{User: app.user},
					Tags:          app.tags,
				},
			},
			RmId: conf.GetSchedulerConf().ClusterId,
		})

	if err != nil {
		// submission failed
		log.Logger.Warn("failed to submit app", zap.Error(err))
		dispatcher.Dispatch(NewFailApplicationEvent(app.applicationId))
	}
}

func (app *Application) handleRecoverApplicationEvent(event *fsm.Event) {
	log.Logger.Info("handle app recovering",
		zap.String("app", app.String()),
		zap.String("clusterId", conf.GetSchedulerConf().ClusterId))
	err := app.schedulerApi.Update(
		&si.UpdateRequest{
			NewApplications: []*si.AddApplicationRequest{
				{
					ApplicationId: app.applicationId,
					QueueName:     app.queue,
					PartitionName: app.partition,
				},
			},
			RmId: conf.GetSchedulerConf().ClusterId,
		})

	if err != nil {
		// submission failed
		log.Logger.Warn("failed to submit app", zap.Error(err))
		dispatcher.Dispatch(NewFailApplicationEvent(app.applicationId))
	}
}

// this is called after entering running state
func (app *Application) handleRunApplicationEvent(event *fsm.Event) {
	if event.Args == nil || len(event.Args) != 1 {
		log.Logger.Error("failed to run tasks",
			zap.String("appId", app.applicationId),
			zap.String("reason", " event argument is expected to have only 1 argument"))
		return
	}

	switch t := event.Args[0].(type) {
	case *Task:
		dispatcher.Dispatch(NewSubmitTaskEvent(app.applicationId, t.taskId))
	}
}

func (app *Application) handleRejectApplicationEvent(event *fsm.Event) {
	log.Logger.Info("app is rejected by scheduler", zap.String("appId", app.applicationId))
	// for rejected apps, we directly move them to failed state
	dispatcher.Dispatch(NewFailApplicationEvent(app.applicationId))
}

func (app *Application) handleCompleteApplicationEvent(event *fsm.Event) {
	//// shutdown the working channel
	//close(app.stopChan)
}

// a application can have one and at most one completion handler,
// the completion handler determines when a application is considered as stopped,
// such as for Spark, once driver is succeed, we think this application is completed.
// this interface can be customized for different type of apps.
type CompletionHandler struct {
	running    bool
	completeFn func()
}

func (app *Application) startCompletionHandler(client client.KubeClient, pod *v1.Pod) {
	for name, value := range pod.Labels {
		if name == common.SparkLabelRole && value == common.SparkLabelRoleDriver {
			app.startSparkCompletionHandler(client, pod)
			return
		}
	}
}

func (app *Application) startSparkCompletionHandler(client client.KubeClient, pod *v1.Pod) {
	// spark driver pod
	log.Logger.Info("start app completion handler",
		zap.String("pod", pod.Name),
		zap.String("appId", app.applicationId))
	if app.ch.running {
		return
	}

	app.ch = CompletionHandler{
		completeFn: func() {
			podWatch, err := client.GetClientSet().CoreV1().Pods(pod.Namespace).Watch(metav1.ListOptions{Watch: true})
			if err != nil {
				log.Logger.Info("unable to create Watch for pod",
					zap.String("pod", pod.Name),
					zap.Error(err))
				return
			}

			for {
				select {
				case targetPod, ok := <-podWatch.ResultChan():
					if !ok {
						return
					}
					resp := targetPod.Object.(*v1.Pod)
					if resp.Status.Phase == v1.PodSucceeded && resp.UID == pod.UID {
						log.Logger.Info("spark driver completed, app completed",
							zap.String("pod", resp.Name),
							zap.String("appId", app.applicationId))
						dispatcher.Dispatch(NewSimpleApplicationEvent(app.applicationId, events.CompleteApplication))
						return
					}
				}
			}
		},
	}
	app.ch.start()
}

func (ch CompletionHandler) start() {
	if !ch.running {
		go ch.completeFn()
	}
}
