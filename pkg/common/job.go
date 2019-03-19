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

type Job struct {
	JobId        string
	Queue        string
	Partition    string
	taskMap      map[string]*Task
	jobState     string
	sm           *fsm.FSM
	States       *JobStates
	lock         *sync.RWMutex
	ch           CompletionHandler
	schedulerApi api.SchedulerApi
	workChan     chan *Task
	stopChan     chan struct{}
}

func (job *Job) String() string  {
	return fmt.Sprintf("jobId: %s, queue: %s, partition: %s," +
		" totalNumOfTasks: %d, currentState: %s",
		job.JobId, job.Queue, job.Partition, len(job.taskMap), job.GetJobState())
}

func NewJob(jobId string, scheduler api.SchedulerApi) *Job {
	taskMap := make(map[string]*Task)
	states := InitiateJobStates()
	job := &Job {
		JobId:     jobId,
		taskMap:   taskMap,
		jobState:  states.NEW.state,
		Queue:     JobDefaultQueue,
		Partition: DefaultPartition,
		States:    states,
		lock:      &sync.RWMutex{},
		ch:        CompletionHandler {running: false},
		workChan:  make(chan *Task, 1024),
		stopChan:  make(chan struct{}),
		schedulerApi: scheduler,
	}

	job.sm = fsm.NewFSM(
		states.NEW.state,
		fsm.Events{
			{Name: string(SubmitJob), Src: []string{states.NEW.state}, Dst: states.SUBMITTED.state},
			{Name: string(AcceptJob), Src: []string{states.SUBMITTED.state}, Dst: states.ACCEPTED.state},
			{Name: string(RunJob), Src: []string{states.ACCEPTED.state, states.RUNNING.state}, Dst: states.RUNNING.state},
			{Name: string(CompleteJob), Src: []string{states.RUNNING.state,}, Dst: states.COMPLETED.state},
			{Name: string(RejectJob), Src: []string{states.SUBMITTED.state}, Dst: states.REJECTED.state},
			{Name: string(FailJob), Src: []string{states.REJECTED.state, states.ACCEPTED.state, states.RUNNING.state}, Dst: states.FAILED.state},
			{Name: string(KillJob), Src: []string{states.ACCEPTED.state, states.RUNNING.state}, Dst: states.KILLING.state},
			{Name: string(KilledJob), Src: []string{states.KILLING.state}, Dst: states.KILLED.state},
		},
		fsm.Callbacks{
			"enter_state": 		  job.handleTaskStateChange,
			states.RUNNING.state: job.handleRunJobEvent,
			string(RejectJob):    job.handleRejectJobEvent,
			string(CompleteJob):  job.handleCompleteJobEvent,
		},
	)

	return job
}

func (job *Job) ScheduleTask(task *Task) {
	job.workChan <- task
}

func (job *Job) IgnoreScheduleTask(task *Task) {
	glog.V(1).Infof("job %s is in unexpected state, current state is %s," +
		" task cannot be scheduled if job's state is other than %s",
		job.JobId, job.GetJobState(), job.States.RUNNING.Value())
}

// submit job to the scheduler
func (job *Job) Submit() {
	glog.V(3).Infof("submit new job %s to cluster %s", job.String(), ClusterId)
	if err := job.schedulerApi.Update(&si.UpdateRequest{
		NewJobs: []*si.AddJobRequest{
			{
				JobId:         job.JobId,
				QueueName:     job.Queue,
				PartitionName: job.Partition,
			},
		},
		RmId: ClusterId,
	}); err == nil {
		job.Handle(NewSimpleJobEvent(SubmitJob))
	}
}

func (job *Job) Run() {
	job.Handle(NewSimpleJobEvent(RunJob))
}

func (job *Job) GetTask(taskId string) *Task {
	return job.taskMap[taskId]
}

func (job *Job) AddTask(task *Task) {
	if _, ok := job.taskMap[task.taskId]; ok {
		// skip adding duplicate task
		return
	}
	job.taskMap[task.taskId] = task
}

func GetJobID(pod *v1.Pod) string {
	for name, value := range pod.Labels {
		if name == SparkLabelAppId {
			return value
		}
		if name == LabelJobId {
			return value
		}
	}
	return ""
}


func (job *Job) RemoveTask(task Task) {
	job.lock.Lock()
	defer job.lock.Unlock()
	delete(job.taskMap, string(task.taskId))
}

func (job *Job) IsPendingTask(task *Task) bool {
	job.lock.RLock()
	defer job.lock.RUnlock()

	if task := job.taskMap[task.taskId]; task != nil {
		return task.IsPending()
	}
	return false
}

func (job *Job) GetJobState() string {
	job.lock.RLock()
	defer job.lock.RUnlock()

	return job.sm.Current()
}

func (job *Job) PrintJobState() {
	job.lock.RLock()
	defer job.lock.RUnlock()

	var allTaskNames = make([]string, len(job.taskMap))
	var idx int64 = 0
	for _, task := range job.taskMap {
		allTaskNames[idx] = fmt.Sprintf("%s(%s)", task.GetTaskPod().Name, task.GetTaskState())
		idx++
	}
	glog.V(4).Infof("job state of %s", job.JobId)
	glog.V(4).Infof(" - state: %s", job.GetJobState())
	glog.V(4).Infof(" - totalNumOfPods: %d, names: %s", len(job.taskMap), allTaskNames)
}

func (job *Job) handleTaskStateChange(event *fsm.Event) {
	if len(event.Args) == 1 {
		switch event.Args[0].(type) {
		case Task:
			job.workChan <- event.Args[0].(*Task)
		}
	}
}

func (job *Job) handleRejectJobEvent(event *fsm.Event) {
	glog.V(4).Infof("job %s is rejected by scheduler", job.JobId)
}

func (job *Job) handleCompleteJobEvent(event *fsm.Event) {
	// shutdown the working channel
	close(job.stopChan)
}

func (job *Job) handleRunJobEvent(event *fsm.Event) {
	glog.V(4).Infof("starting internal job working thread")
	go func() {
		for {
			select {
			case task := <-job.workChan:
				glog.V(3).Infof("Scheduling task: task=%s, state=%s", task.taskId, task.GetTaskState())
				switch task.GetTaskState() {
				case task.states.PENDING.state:
					task.Handle(NewSubmitTaskEvent())
				default:
					// nothing to do here...
				}
			case <-job.stopChan:
				glog.V(3).Infof("stopping job working thread for job %s", job.JobId)
				return
			}
		}
	}()
}

// event handling
func (job *Job) Handle(se JobEvent) error {
	job.lock.Lock()
	defer job.lock.Unlock()

	glog.V(4).Infof("Job(%s): preState: %s, coming event: %s", job.JobId, job.sm.Current(), string(se.getEvent()))
	err := job.sm.Event(string(se.getEvent()), se.getArgs())
	glog.V(4).Infof("Job(%s): postState: %s, handled event: %s", job.JobId, job.sm.Current(), string(se.getEvent()))
	return err
}

func (job *Job) GetPendingTasks() []*Task {
	job.lock.RLock()
	defer job.lock.RUnlock()

	taskList := make([]*Task, 0)
	if len(job.taskMap) > 0 {
		for _, task := range job.taskMap {
			if task.IsPending() {
				taskList = append(taskList, task)
			}
		}
	}
	return taskList
}

// a job can have one and at most one completion handler,
// the completion handler determines when a job is considered as stopped,
// such as for Spark, once driver is succeed, we think this job is completed.
// this interface can be customized for different type of jobs.
type CompletionHandler struct {
	running bool
	completeFn func()
}

func (job *Job) StartCompletionHandler(client client.KubeClient, pod *v1.Pod) {
	for name, value := range pod.Labels {
		if name == SparkLabelRole && value == SparkLabelRoleDriver {
			job.startSparkCompletionHandler(client, pod)
			return
		}
 	}
}

func (job *Job) startSparkCompletionHandler(client client.KubeClient, pod *v1.Pod) {
	// spark driver pod
	glog.V(4).Infof("start job completion handler for pod %s, job %s", pod.Name, job.JobId)
	if job.ch.running {
		return
	}

	job.ch = CompletionHandler{
		completeFn: func() {
			// ctx.jobController.Complete(job)
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
						glog.V(4).Infof("spark driver completed %s, job completed %s", resp.Name, job.JobId)
						job.Handle(NewSimpleJobEvent(CompleteJob))
						return
					}
				}
			}
		},
	}
	job.ch.start()
}

func (ch CompletionHandler) start() {
	if !ch.running {
		go ch.completeFn()
	}
}