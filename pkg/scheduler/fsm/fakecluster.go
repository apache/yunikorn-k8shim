package fsm

import (
	"fmt"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/client"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/conf"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/shim/callback"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/scheduler/state"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	utils "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/entrypoint"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
	"gotest.tools/assert"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"testing"
	"time"
)

const fakeClusterId = "test-cluster"
const fakeClusterVersion = "0.1.0"
const fakeClusterSchedulerName = "yunikorn-test"
const fakeClusterSchedulingInterval = 1

// fake cluster is used for testing
// it uses fake kube client to simulate API calls with k8s, all other code paths are real
type FakeCluster struct {
	context *state.Context
	scheduler *ShimScheduler
	proxy *rmproxy.RMProxy
	client client.KubeClient
	conf string
	bindFn func(pod *v1.Pod, hostId string) error
	deleteFn func(pod *v1.Pod) error
	stopChan chan struct{}
}

// fake client allows us to inject customized bind/delete pod functions
type FakeKubeClient struct {
	bindFn func(pod *v1.Pod, hostId string) error
	deleteFn func(pod *v1.Pod) error
}

func (c *FakeKubeClient) Bind(pod *v1.Pod, hostId string) error {
	return c.bindFn(pod, hostId)
}

func (c *FakeKubeClient) Delete(pod *v1.Pod) error {
	return c.deleteFn(pod)
}

func (c *FakeKubeClient) GetClientSet() *kubernetes.Clientset {
	return nil
}

func (fc *FakeCluster) init(queues string) {
	configs := conf.SchedulerConf{
		ClusterId:      fakeClusterId,
		ClusterVersion: fakeClusterVersion,
		SchedulerName:  fakeClusterSchedulerName,
		Interval:       fakeClusterSchedulingInterval,
		KubeConfig:     "",
	}

	fc.conf = queues
	fc.stopChan = make(chan struct{})
	// default functions for bind and delete, this can be override if necessary
	fc.deleteFn = func(pod *v1.Pod) error {
		fmt.Printf("pod deleted")
		return nil
	}
	fc.bindFn = func(pod *v1.Pod, hostId string) error {
		fmt.Printf("pod bound")
		return nil
	}

	rmProxy, _, _  := entrypoint.StartAllServices()
	utils.MockSchedulerConfigByData([]byte(fc.conf))

	client := &FakeKubeClient{
		bindFn:   fc.bindFn,
		deleteFn: fc.deleteFn,
	}
	context := state.NewContextInternal(rmProxy, &configs, client, true)
	callback := callback.NewSimpleRMCallback(context)
	ss := NewShimScheduler(rmProxy, context, callback)

	fc.context = context
	fc.scheduler = ss
	fc.proxy = rmProxy
	fc.client = client
}

func (fc *FakeCluster) start() {
	fc.scheduler.Run(fc.stopChan)
}

func (fc *FakeCluster) assertSchedulerState(t *testing.T, expectedState string) {
	assert.Equal(t, fc.scheduler.GetSchedulerState(), expectedState)
}

func (fc *FakeCluster) addNode(nodeName string, memory int64, cpu int64) error {
	nodeResource := common.CreateResource(memory, cpu)
	node := common.CreateFromNodeSpec(nodeName, nodeName, &nodeResource)
	request := common.CreateUpdateRequestForNode(node)
	fmt.Printf("report new nodes to scheduler, request: %s", request.String())
	return fc.proxy.Update(&request)
}

func (fc *FakeCluster) addTask(tid string, ask si.Resource, job *common.Job) common.Task{
	task := common.CreateTaskForTest(tid, job, &ask, fc.client, fc.proxy)
	job.AddTask(&task)
	return task
}

func (fc *FakeCluster) waitForSchedulerState(t *testing.T, expectedState string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if fc.scheduler.GetSchedulerState() == expectedState {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("wait for scheduler to reach state %s failed, current state %s",
				expectedState, fc.scheduler.GetSchedulerState() )
		}
	}
}

func (fc *FakeCluster) waitAndAssertJobState(t *testing.T, jobId string, expectedState string) {
	jobList := fc.context.SelectJobs(func(job *common.Job) bool {
		return job.JobId == jobId
	})
	assert.Equal(t, len(jobList), 1)
	assert.Equal(t, jobList[0].JobId, jobId)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if jobList[0].GetJobState() == expectedState {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("job %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				jobId, expectedState, jobList[0].GetJobState())
		}
	}
}

func (fc *FakeCluster) addJob(job *common.Job) {
	fc.context.AddJob(job)
}

func (fc *FakeCluster) newJob(jobId string, queueName string) *common.Job {
	return common.NewJob(jobId, queueName, fc.proxy)
}

func (fc *FakeCluster) waitAndAssertTaskState(t *testing.T, jobId string, taskId string, expectedState string) {
	jobList := fc.context.SelectJobs(func(job *common.Job) bool {
		return job.JobId == jobId
	})
	assert.Equal(t, len(jobList), 1)
	assert.Equal(t, jobList[0].JobId, jobId)
	assert.Assert(t, jobList[0].GetTask(taskId) != nil)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if jobList[0].GetTask(taskId).GetTaskState() == expectedState {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("task %s doesn't reach expected state in given time, expecting: %s, actual: %s",
				taskId, expectedState, jobList[0].GetTask(taskId).GetTaskState())
		}
	}
}

func (fc *FakeCluster) stop() {
	close(fc.stopChan)
}