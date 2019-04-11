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

package state

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/conf"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
)

type Application struct {
	applicationId string
	queue         string
	partition     string
	taskMap       map[string]*Task
	sm            *fsm.FSM
	lock          *sync.RWMutex
	ch            CompletionHandler
	schedulerApi  api.SchedulerApi
}

func (app *Application) String() string  {
	return fmt.Sprintf("applicationId: %s, queue: %s, partition: %s," +
		" totalNumOfTasks: %d, currentState: %s",
		app.applicationId, app.queue, app.partition, len(app.taskMap), app.GetApplicationState())
}

func NewApplication(appId string, queueName string, scheduler api.SchedulerApi) *Application {
	taskMap := make(map[string]*Task)
	app := &Application{
		applicationId: appId,
		taskMap:       taskMap,
		queue:         queueName,
		partition:     conf.DefaultPartition,
		lock:          &sync.RWMutex{},
		ch:            CompletionHandler {running: false},
		schedulerApi:  scheduler,
	}

	var states = States().Application
	app.sm = fsm.NewFSM(
		states.New,
		fsm.Events{
			{Name: string(SubmitApplication),
				Src: []string{states.New},
				Dst: states.Submitted},
			{Name: string(AcceptApplication),
				Src: []string{states.Submitted},
				Dst: states.Accepted},
			{Name: string(RunApplication),
				Src: []string{states.Accepted, states.Running},
				Dst: states.Running},
			{Name: string(CompleteApplication),
				Src: []string{states.Running,},
				Dst: states.Completed},
			{Name: string(RejectApplication),
				Src: []string{states.Submitted},
				Dst: states.Rejected},
			{Name: string(FailApplication),
				Src: []string{states.Submitted, states.Rejected, states.Accepted, states.Running},
				Dst: states.Failed},
			{Name: string(KillApplication),
				Src: []string{states.Accepted, states.Running},
				Dst: states.Killing},
			{Name: string(KilledApplication),
				Src: []string{states.Killing},
				Dst: states.Killed},
		},
		fsm.Callbacks{
			//"enter_state":               app.handleTaskStateChange,
			string(SubmitApplication):   app.handleSubmitApplicationEvent,
			string(RunApplication):      app.handleRunApplicationEvent,
			string(RejectApplication):   app.handleRejectApplicationEvent,
			string(CompleteApplication): app.handleCompleteApplicationEvent,
		},
	)

	return app
}

func (app *Application) handle(ev ApplicationEvent) error {
	// state machine has its instinct lock, we don't need to hold the app lock here
	// because the callbacks are the places where might modify app state, not here.
	glog.V(4).Infof("Application(%s): preState: %s, coming event: %s",
		app.applicationId, app.sm.Current(), string(ev.getEvent()))
	if err := app.sm.Event(string(ev.getEvent()), ev.getArgs()...); err != nil {
		return err
	}
	glog.V(4).Infof("Application(%s): postState: %s, handled event: %s",
		app.applicationId, app.sm.Current(), string(ev.getEvent()))
	return nil
}

func (app *Application) GetTask(taskId string) (*Task, error) {
	app.lock.RLock()
	defer app.lock.RUnlock()
	if task, ok := app.taskMap[taskId]; ok{
		return task, nil
	}
	return nil, errors.New(fmt.Sprintf("task %s doesn't exist in application %s",
		taskId, app.applicationId))
}

func (app *Application) GetApplicationId() string{
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

func GenerateApplicationIdFromPod(pod *v1.Pod) (string, error) {
	for name, value := range pod.Labels {
		// if a pod for spark already provided appId, reuse it
		if name == conf.SparkLabelAppId {
			return value, nil
		}

		// application ID can be defined as a label
		if name == conf.LabelApplicationId {
			return value, nil
		}
	}

	// application ID can be defined in annotations too
	for name, value := range pod.Annotations {
		if name == conf.LabelApplicationId {
			return value, nil
		}
	}
	return "", errors.New(fmt.Sprintf("unable to retrieve application ID from pod spec, %s",
		pod.Spec.String()))
}

func (app *Application) GetApplicationState() string {
	return app.sm.Current()
}

func (app *Application) GetPendingTasks() []*Task {
	app.lock.RLock()
	defer app.lock.RUnlock()

	taskList := make([]*Task, 0)
	if len(app.taskMap) > 0 {
		for _, task := range app.taskMap {
			if task.IsPending() {
				taskList = append(taskList, task)
			}
		}
	}
	return taskList
}

func (app *Application) handleSubmitApplicationEvent(event *fsm.Event) {
	glog.V(3).Infof("submit new app %s to cluster %s", app.String(), conf.GlobalClusterId)
	err := app.schedulerApi.Update(
		&si.UpdateRequest{
			NewApplications: []*si.AddApplicationRequest{
				{
					ApplicationId: app.applicationId,
					QueueName:     app.queue,
					PartitionName: app.partition,
				},
			},
			RmId: conf.GlobalClusterId,
	})

	if err != nil {
		// submission failed
		GetDispatcher().Dispatch(NewFailApplicationEvent(app.applicationId))
	}
}

// this is called after entering running state
func (app *Application) handleRunApplicationEvent(event *fsm.Event) {
	if event.Args == nil || len(event.Args) != 1 {
		glog.V(1).Infof("failed to run application tasks," +
			" event argument is expected to have only 1 argument",
			app.applicationId)
		return
	}

	switch t := event.Args[0].(type) {
	case *Task:
		GetDispatcher().Dispatch(NewSubmitTaskEvent(app.applicationId, t.taskId))
	}
}

func (app *Application) handleRejectApplicationEvent(event *fsm.Event) {
	glog.V(4).Infof("app %s is rejected by scheduler", app.applicationId)
	// for rejected apps, we directly move them to failed state
	GetDispatcher().Dispatch(NewFailApplicationEvent(app.applicationId))
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
	running bool
	completeFn func()
}

func (app *Application) StartCompletionHandler(client client.KubeClient, pod *v1.Pod) {
	for name, value := range pod.Labels {
		if name == conf.SparkLabelRole && value == conf.SparkLabelRoleDriver {
			app.startSparkCompletionHandler(client, pod)
			return
		}
 	}
}

func (app *Application) startSparkCompletionHandler(client client.KubeClient, pod *v1.Pod) {
	// spark driver pod
	glog.V(4).Infof("start app completion handler for pod %s, app %s", pod.Name, app.applicationId)
	if app.ch.running {
		return
	}

	app.ch = CompletionHandler{
		completeFn: func() {
			podWatch, err := client.GetClientSet().CoreV1().Pods(pod.Namespace).Watch(metav1.ListOptions{ Watch: true, })
			if err != nil {
				glog.V(1).Info("Unable to create Watch for pod %s", pod.Name)
				return
			}

			for {
				select {
				case events, ok := <-podWatch.ResultChan():
					if !ok {
						return
					}
					resp := events.Object.(*v1.Pod)
					if resp.Status.Phase == v1.PodSucceeded && resp.UID == pod.UID {
						glog.V(4).Infof("spark driver completed %s, app completed %s", resp.Name, app.applicationId)
						GetDispatcher().Dispatch(NewSimpleApplicationEvent(app.applicationId, CompleteApplication))
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