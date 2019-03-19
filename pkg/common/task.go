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
	"sync"

	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"k8s.io/api/core/v1"
)

type Task struct {
	taskId        string
	jobId         string
	pod           *v1.Pod
	kubeClient    client.KubeClient
	Events        *TaskEvents
	sm            *fsm.FSM
	lock          sync.RWMutex
}

func newTask(tid string, jid string, client client.KubeClient, pod *v1.Pod) Task {
	task := Task{
		taskId: tid,
		jobId:  jid,
		pod:    pod,
		kubeClient: client,
		lock:   sync.RWMutex{},
	}

	task.sm = fsm.NewFSM(
		PENDING,
		fsm.Events{
			{Name: string(Submit), Src: []string{PENDING}, Dst: SCHEDULING},
			{Name: string(Allocated), Src: []string{SCHEDULING}, Dst: ALLOCATED},
			{Name: string(Bound), Src: []string{ALLOCATED}, Dst: BOUND},
			{Name: string(Complete), Src: []string{BOUND}, Dst: COMPLETED},
			{Name: string(Kill), Src: []string{PENDING, SCHEDULING, ALLOCATED, BOUND}, Dst: KILLING},
			{Name: string(Killed), Src: []string{KILLING}, Dst: KILLED},
			{Name: string(Rejected), Src: []string{SCHEDULING}, Dst: REJECTED},
			{Name: string(Fail), Src: []string{REJECTED}, Dst: FAILED},
		},
		fsm.Callbacks{
			ALLOCATED: task.postTaskAllocated,
			string(Fail): task.handleFailEvent,
		},
	)

	return task
}

func CreateTaskFromPod(jobId string, client client.KubeClient, pod *v1.Pod) *Task {
	task := newTask(string(pod.UID), jobId, client, pod)
	return &task
}

func (task *Task) GetTaskPod() *v1.Pod {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.pod
}

func (task *Task) GetTaskState() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.sm.Current()
}

// task event handlers
func (task *Task) handleFailEvent(event *fsm.Event) {
	task.lock.Lock()
	defer task.lock.Unlock()

	if len(event.Args) != 1 {
		glog.V(1).Infof("invalid number of arguments, expecting 1 but get %d",
			len(event.Args))
		return
	}

	glog.V(1).Infof("Task failed, jobId=%s, taskId=%s, error message: %s",
		task.jobId, task.taskId, event.Args[0])
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

// event handling
func (task *Task) Handle(te TaskEvent) error {
	glog.V(4).Infof("Task(%s): preState: %s, coming event: %s",
		task.taskId, task.sm.Current(), te.getEvent())
	err := task.sm.Event(string(te.getEvent()), te.getArgs())
	glog.V(4).Infof("Task(%s): postState: %s, handled event: %s",
		task.taskId, task.sm.Current(), te.getEvent())
	return err
}
