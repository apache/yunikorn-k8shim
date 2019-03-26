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
	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
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
	workChan      chan *Task
	stopChan      chan struct{}
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
		partition:     DefaultPartition,
		lock:          &sync.RWMutex{},
		ch:            CompletionHandler {running: false},
		workChan:      make(chan *Task, 1024),
		stopChan:      make(chan struct{}),
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
				Src: []string{states.Rejected, states.Accepted, states.Running},
				Dst: states.Failed},
			{Name: string(KillApplication),
				Src: []string{states.Accepted, states.Running},
				Dst: states.Killing},
			{Name: string(KilledApplication),
				Src: []string{states.Killing},
				Dst: states.Killed},
		},
		fsm.Callbacks{
			"enter_state":               app.handleTaskStateChange,
			states.Running:              app.handleRunApplicationEvent,
			string(RejectApplication):   app.handleRejectApplicationEvent,
			string(CompleteApplication): app.handleCompleteApplicationEvent,
		},
	)

	return app
}

func (app *Application) ScheduleTask(task *Task) {
	app.workChan <- task
}

func (app *Application) IgnoreScheduleTask(task *Task) {
	glog.V(1).Infof("app %s is in unexpected state, current state is %s," +
		" task cannot be scheduled if app's state is other than %s",
		app.applicationId, app.GetApplicationState(), States().Application.Running)
}

// submit app to the scheduler
func (app *Application) Submit() {
	glog.V(3).Infof("submit new app %s to cluster %s", app.String(), ClusterId)
	if err := app.schedulerApi.Update(&si.UpdateRequest{
		NewJobs: []*si.AddJobRequest{
			{
				JobId:         app.applicationId,
				QueueName:     app.queue,
				PartitionName: app.partition,
			},
		},
		RmId: ClusterId,
	}); err == nil {
		app.Handle(NewSimpleApplicationEvent(SubmitApplication))
	}
}

func (app *Application) Run() {
	app.Handle(NewSimpleApplicationEvent(RunApplication))
}

func (app *Application) GetTask(taskId string) *Task {
	return app.taskMap[taskId]
}

func (app *Application) GetApplicationId() string{
	return app.applicationId
}

func (app *Application) AddTask(task *Task) {
	if _, ok := app.taskMap[task.taskId]; ok {
		// skip adding duplicate task
		return
	}
	app.taskMap[task.taskId] = task
}

func GetApplicationId(pod *v1.Pod) string {
	for name, value := range pod.Labels {
		if name == SparkLabelAppId {
			return value
		}
		if name == LabelApplicationId {
			return value
		}
	}
	return ""
}


func (app *Application) RemoveTask(task Task) {
	app.lock.Lock()
	defer app.lock.Unlock()
	delete(app.taskMap, string(task.taskId))
}

func (app *Application) IsPendingTask(task *Task) bool {
	app.lock.RLock()
	defer app.lock.RUnlock()

	if task := app.taskMap[task.taskId]; task != nil {
		return task.IsPending()
	}
	return false
}

func (app *Application) GetApplicationState() string {
	app.lock.RLock()
	defer app.lock.RUnlock()

	return app.sm.Current()
}

func (app *Application) PrintApplicationState() {
	app.lock.RLock()
	defer app.lock.RUnlock()

	var allTaskNames = make([]string, len(app.taskMap))
	var idx int64 = 0
	for _, task := range app.taskMap {
		allTaskNames[idx] = fmt.Sprintf("%s(%s)", task.GetTaskPod().Name, task.GetTaskState())
		idx++
	}
	glog.V(4).Infof("app state of %s", app.applicationId)
	glog.V(4).Infof(" - state: %s", app.GetApplicationState())
	glog.V(4).Infof(" - totalNumOfPods: %d, names: %s", len(app.taskMap), allTaskNames)
}

func (app *Application) handleTaskStateChange(event *fsm.Event) {
	if len(event.Args) == 1 {
		switch event.Args[0].(type) {
		case Task:
			app.workChan <- event.Args[0].(*Task)
		}
	}
}

func (app *Application) handleRejectApplicationEvent(event *fsm.Event) {
	glog.V(4).Infof("app %s is rejected by scheduler", app.applicationId)
}

func (app *Application) handleCompleteApplicationEvent(event *fsm.Event) {
	// shutdown the working channel
	close(app.stopChan)
}

func (app *Application) handleRunApplicationEvent(event *fsm.Event) {
	glog.V(4).Infof("starting internal app working thread")
	go func() {
		for {
			select {
			case task := <-app.workChan:
				glog.V(3).Infof("Scheduling task: task=%s, state=%s", task.taskId, task.GetTaskState())
				switch task.GetTaskState() {
				case States().Task.Pending:
					task.Handle(NewSubmitTaskEvent())
				default:
					// nothing to do here...
				}
			case <-app.stopChan:
				glog.V(3).Infof("stopping app working thread for app %s", app.applicationId)
				return
			}
		}
	}()
}

// event handling
func (app *Application) Handle(se ApplicationEvent) error {
	app.lock.Lock()
	defer app.lock.Unlock()

	glog.V(4).Infof("Application(%s): preState: %s, coming event: %s", app.applicationId, app.sm.Current(), string(se.getEvent()))
	err := app.sm.Event(string(se.getEvent()), se.getArgs())
	glog.V(4).Infof("Application(%s): postState: %s, handled event: %s", app.applicationId, app.sm.Current(), string(se.getEvent()))
	return err
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
		if name == SparkLabelRole && value == SparkLabelRoleDriver {
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
						app.Handle(NewSimpleApplicationEvent(CompleteApplication))
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