package test

import (
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	v1 "k8s.io/api/core/v1"
)

type MockedAPIProvider struct {
	clients *client.Clients
}

func NewMockedAPIProvider() *MockedAPIProvider {
	return &MockedAPIProvider{
		clients: &client.Clients{
			Conf:              &conf.SchedulerConf{
				ClusterID:            "yk-test-cluster",
				ClusterVersion:       "0.1",
				SchedulerName:        "yunikorn",
				PolicyGroup:          "queues",
				Interval:             0,
				KubeConfig:           "",
				LoggingLevel:         0,
				LogEncoding:          "",
				LogFile:              "",
				VolumeBindTimeout:    0,
				TestMode:             true,
				EventChannelCapacity: 0,
				DispatchTimeout:      0,
				KubeQPS:              0,
				KubeBurst:            0,
				Predicates:           "",
			},
			KubeClient:        NewKubeClientMock(),
			SchedulerAPI:      NewSchedulerAPIMock(),
			PodInformer:       nil,
			NodeInformer:      nil,
			ConfigMapInformer: nil,
			PVInformer:        nil,
			PVCInformer:       nil,
			StorageInformer:   nil,
			VolumeBinder:      nil,
		},
	}
}

func (m *MockedAPIProvider) MockBindFn(bfn func(pod *v1.Pod, hostID string) error) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.bindFn = bfn
	}
}

func (m *MockedAPIProvider) MockDeleteFn(dfn func(pod *v1.Pod) error) {
	if mock, ok := m.clients.KubeClient.(*KubeClientMock); ok {
		mock.deleteFn = dfn
	}
}

func (m *MockedAPIProvider) GetAPIs() *client.Clients {
	return m.clients
}

func (m *MockedAPIProvider) AddEventHandler (handlers *client.ResourceEventHandlers) {
	// no impl
}

func (m *MockedAPIProvider) Run(stopCh <-chan struct{}) {
	// no impl
}