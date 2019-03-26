package common

import (
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
	"gotest.tools/assert"
	"testing"
)

func TestNewApplication(t *testing.T) {
	app := NewApplication("app00001", "root.queue", newMockSchedulerApi())
	assert.Equal(t, app.GetApplicationId(), "app00001" )
	assert.Equal(t, app.GetApplicationState(), States().Application.New)
	assert.Equal(t, app.partition, DefaultPartition)
	assert.Equal(t, len(app.taskMap), 0)
	assert.Equal(t, app.GetApplicationState(), States().Application.New)
	assert.Equal(t, app.queue, "root.queue")
}

func TestSubmitApplication(t *testing.T) {
	app := NewApplication("app00001", "root.abc", newMockSchedulerApi())
	app.Submit()
	assert.Equal(t, app.GetApplicationState(), States().Application.Submitted)

	// app already submitted
	app.Submit()
	assert.Equal(t, app.GetApplicationState(), States().Application.Submitted)
}

func TestRunApplication(t *testing.T) {
	ms := &MockSchedulerApi{}
	ms.updateFn = func(request *si.UpdateRequest) error {
		assert.Equal(t, len(request.NewJobs), 1)
		assert.Equal(t, request.NewJobs[0].JobId, "app00001")
		assert.Equal(t, request.NewJobs[0].QueueName, "root.abc")
		return nil
	}

	app := NewApplication("app00001", "root.abc", ms)

	// app must be submitted before being able to run
	app.Run()
	assert.Equal(t, app.GetApplicationState(), States().Application.New)

	// submit the app
	app.Submit()
	assert.Equal(t, app.GetApplicationState(), States().Application.Submitted)

	// app must be accepted first
	app.Run()
	assert.Equal(t, app.GetApplicationState(), States().Application.Submitted)

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