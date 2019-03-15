package common

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/looplab/fsm"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sync"
)

// k8s doesn't have job notion. Alternatively can use Pod.MetaData.Label to represent a job,
// e.g jobId: job_2019_01_01_00001. Multiple pods, if they are found having same jobId label,
// will be considered as sub-task for a job.

// new job will need be firstly submitted to the scheduler, check its scheduling status
// until the job is scheduled, then we start to do schedule the real pod.
type Job struct {
	JobId string
	Queue string
	Partition string
	PodList []*v1.Pod
	JobState string
	PendingPods []types.UID
	FSM *fsm.FSM
	Events *JobEvents
	States *JobStates
	lock *sync.RWMutex
	ch CompletionHandler
}

func (job *Job) String() string  {
	return fmt.Sprintf("jobId: %s, queue: %s, partition: %s," +
		" totalNumOfPods: %d, pendingPods: %s, currentState: %s",
		job.JobId, job.Queue, job.Partition, len(job.PodList), job.PendingPods, job.GetJobState())
}

func NewJob(jobId string) *Job {
	podList := make([]*v1.Pod, 0)
	events := InitiateJobEvents()
	states := InitiateJobStates()
	job := &Job {
		JobId:     jobId,
		PodList:   podList,
		JobState:  states.NEW.state,
		Queue:     JobDefaultQueue,
		Partition: DefaultPartition,
		PendingPods: make([]types.UID, 0),
		Events: events,
		States: states,
		lock: &sync.RWMutex{},
		ch: CompletionHandler {running: false},
	}

	job.FSM = fsm.NewFSM(
		states.NEW.state,
		fsm.Events{
			// handle submit
			{ Name: events.SUBMIT.event, Src: []string{ states.NEW.state }, Dst: states.SUBMITTED.state },
			// handle accept
			{ Name: events.ACCEPT.event, Src: []string{ states.SUBMITTED.state }, Dst: states.ACCEPTED.state },
			// handle run
			{ Name: events.RUN.event, Src: []string{ states.ACCEPTED.state }, Dst: states.RUNNING.state},
			// handle complete
			{ Name: events.COMPLETE.event, Src: []string{ states.RUNNING.state, }, Dst: states.COMPLETED.state},
			// handle reject
			{ Name: events.REJECT.event, Src: []string{ states.SUBMITTED.state }, Dst: states.REJECTED.state },
			// handle fail
			{ Name: events.FAIL.event, Src: []string{ states.REJECTED.state, states.ACCEPTED.state, states.RUNNING.state }, Dst: states.FAILED.state },
			// handle kill
			{ Name: events.KILL.event, Src: []string{ states.ACCEPTED.state, states.RUNNING.state }, Dst: states.KILLING.state },
			// handle killed
			{ Name: events.KILLED.event, Src: []string{ states.KILLING.state }, Dst: states.KILLED.state },
		},
		fsm.Callbacks{
			events.REJECT.event: func(event *fsm.Event) {
				job.Handle(events.FAIL)
			},
		},
	)

	return job
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

func (job *Job) AddPod(pod *v1.Pod) bool {
	job.lock.Lock()
	defer job.lock.Unlock()

	for _, j := range job.PodList {
		if j.UID == pod.UID {
			// skip duplicated pod
			return false
		}
	}
	job.PodList = append(job.PodList, pod)
	return true
}

func (job *Job) AddPendingPod(pod *v1.Pod) {
	job.lock.Lock()
	defer job.lock.Unlock()

	for _, j := range job.PendingPods {
		if j == pod.UID {
			// skip duplicated pod
			return
		}
	}
	job.PendingPods = append(job.PendingPods, pod.UID)
}

func (job *Job) RemovePod(pod v1.Pod) {
	job.lock.Lock()
	defer job.lock.Unlock()

	for idx, j := range job.PodList {
		if j.UID == pod.UID {
			job.PodList = append(job.PodList[:idx],
				job.PodList[idx+1:]...)
			return
		}
	}
}

func (job *Job) RemovePendingPod(pod *v1.Pod) {
	job.lock.Lock()
	defer job.lock.Unlock()

	for idx, j := range job.PendingPods {
		if j == pod.UID {
			job.PendingPods = append(job.PendingPods[:idx],
				job.PendingPods[idx+1:]...)
			return
		}
	}
	glog.V(4).Infof("job %s, pending pods %d", job.JobId, len(job.PendingPods))
}

func (job *Job) IsPendingPod(pod *v1.Pod) bool {
	job.lock.RLock()
	defer job.lock.RUnlock()

	for _, j := range job.PendingPods {
		if j == pod.UID {
			return true
		}
	}
	return false
}

func (job *Job) GetJobState() string {
	job.lock.RLock()
	defer job.lock.RUnlock()

	return job.FSM.Current()
}

func (job *Job) PrintJobState() {
	job.lock.RLock()
	defer job.lock.RUnlock()

	var allPodNames = make([]string, len(job.PodList))
	for idx1, j := range job.PodList {
		allPodNames[idx1] = j.Name
	}
	glog.V(4).Infof("job state of %s", job.JobId)
	glog.V(4).Infof(" - state: %s", job.GetJobState())
	glog.V(4).Infof(" - totalNumOfPods: %d, names: %s", len(job.PodList), allPodNames)
	glog.V(4).Infof(" - numOfPendingPods: %d, names: %s", len(job.PendingPods), job.PendingPods)
}

// event handling
func (job *Job) Handle(se JobEvent) error {
	job.lock.Lock()
	defer job.lock.Unlock()

	glog.V(4).Infof("Job(%s): preState: %s, coming event: %s", job.JobId, job.FSM.Current(), se.event)
	err := job.FSM.Event(se.event)
	glog.V(4).Infof("Job(%s): postState: %s, handled event: %s", job.JobId, job.FSM.Current(), se.event)
	return err
}

func (job *Job) SelectPods(filter func(pod *v1.Pod) bool) []*v1.Pod {
	job.lock.RLock()
	defer job.lock.RUnlock()

	pods := make([]*v1.Pod, 0)
	for _, pod := range job.PodList {
		if filter != nil && !filter(pod) {
			continue
		}
		// glog.V(4).Infof("pending pod %s to job select result", pod.Name)
		pods = append(pods, pod)
	}

	return pods
}

func (job *Job) GetPendingPods() []*v1.Pod {
	job.lock.RLock()
	defer job.lock.RUnlock()

	pods := make([]*v1.Pod, 0)

	for _, pod := range job.SelectPods(nil) {
		if job.IsPendingPod(pod) {
			pods = append(pods, pod)
		}
	}

	return pods
}

func (job *Job) RemoveAllPendingPods() {
	job.lock.Lock()
	defer job.lock.Unlock()
	job.PendingPods = nil
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
						job.RemoveAllPendingPods()
						job.Handle(job.Events.COMPLETE)
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