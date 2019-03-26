package common

import (
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"gotest.tools/assert"
	"testing"
)

func TestNewJob(t *testing.T) {
	job := NewJob("job00001", "root.queue", newMockSchedulerApi())
	assert.Equal(t, job.JobId, "job00001" )
	assert.Equal(t, job.GetJobState(), States().Job.New)
	assert.Equal(t, job.Partition, DefaultPartition)
	assert.Equal(t, len(job.taskMap), 0)
	assert.Equal(t, job.sm.Current(), States().Job.New)
	assert.Equal(t, job.Queue, "root.queue")
}

func TestSubmitJob(t *testing.T) {
	job := NewJob("job00001", "root.abc", newMockSchedulerApi())
	job.Submit()
	assert.Equal(t, job.GetJobState(), States().Job.Submitted)

	// job already submitted
	job.Submit()
	assert.Equal(t, job.GetJobState(), States().Job.Submitted)
}

func TestRunJob(t *testing.T) {
	ms := &MockSchedulerApi{}
	ms.updateFn = func(request *si.UpdateRequest) error {
		assert.Equal(t, len(request.NewJobs), 1)
		assert.Equal(t, request.NewJobs[0].JobId, "job00001")
		assert.Equal(t, request.NewJobs[0].QueueName, "root.abc")
		return nil
	}

	job := NewJob("job00001", "root.abc", ms)

	// job must be submitted before being able to run
	job.Run()
	assert.Equal(t, job.GetJobState(), States().Job.New)

	// submit the job
	job.Submit()
	assert.Equal(t, job.GetJobState(), States().Job.Submitted)

	// job must be accepted first
	job.Run()
	assert.Equal(t, job.GetJobState(), States().Job.Submitted)

}

func newMockSchedulerApi() *MockSchedulerApi {
	return &MockSchedulerApi{
		registerFn: func(request *si.RegisterResourceManagerRequest, callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, nil
		},
		updateFn: func(request *si.UpdateRequest) error {
			return nil
		},
	}
}
type MockSchedulerApi struct {
	callback api.ResourceManagerCallback
	registerFn func(request *si.RegisterResourceManagerRequest,
		callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)
	updateFn func(request *si.UpdateRequest) error
}

func (ms *MockSchedulerApi) RegisterResourceManager(request *si.RegisterResourceManagerRequest,
	callback api.ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error) {
	return ms.registerFn(request, callback)
}

func (ms *MockSchedulerApi) Update(request *si.UpdateRequest) error {
	return ms.updateFn(request)
}