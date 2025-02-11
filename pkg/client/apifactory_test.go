/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package client

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestInformerTypes(t *testing.T) {
	assert.Equal(t, 15, len(informerTypes), "wrong informerTypes length")

	assert.Equal(t, "Pod", PodInformerHandlers.String())
	assert.Equal(t, "Node", NodeInformerHandlers.String())
	assert.Equal(t, "ConfigMap", ConfigMapInformerHandlers.String())
	assert.Equal(t, "PV", PVInformerHandlers.String())
	assert.Equal(t, "PVC", PVCInformerHandlers.String())
	assert.Equal(t, "Storage", StorageInformerHandlers.String())
	assert.Equal(t, "CSINode", CSINodeInformerHandlers.String())
	assert.Equal(t, "CSIDriver", CSIDriverInformerHandlers.String())
	assert.Equal(t, "CSIStorageCapacity", CSIStorageCapacityInformerHandlers.String())
	assert.Equal(t, "Namespace", NamespaceInformerHandlers.String())
	assert.Equal(t, "PriorityClass", PriorityClassInformerHandlers.String())
	assert.Equal(t, "Service", ServiceInformerHandlers.String())
	assert.Equal(t, "ReplicationController", ReplicationControllerInformerHandlers.String())
	assert.Equal(t, "ReplicaSet", ReplicaSetInformerHandlers.String())
	assert.Equal(t, "StatefulSet", StatefulSetInformerHandlers.String())
}

func TestMockedAPIProvider_GetPodBindStats(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(*MockedAPIProvider, *KubeClientMock)
		want      BindStats
		wantPanic bool
	}{
		{
			name: "successful get bind stats",
			setupMock: func(m *MockedAPIProvider, k *KubeClientMock) {
				m.clients.KubeClient = k
				k.bindStats = BindStats{
					Success: 10,
					Errors:  2,
				}
			},
			want: BindStats{
				Success: 10,
				Errors:  2,
			},
			wantPanic: false,
		},
		{
			name: "panic when kubeClient is not KubeClientMock",
			setupMock: func(m *MockedAPIProvider, k *KubeClientMock) {
				m.clients.KubeClient = nil // Set to non-KubeClientMock type to trigger panic
			},
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MockedAPIProvider{
				clients: &Clients{},
			}
			k := &KubeClientMock{}

			tt.setupMock(m, k)

			if tt.wantPanic {
				assert.Assert(t, panics(func() { m.GetPodBindStats() }))
				return
			}

			got := m.GetPodBindStats()
			assert.DeepEqual(t, tt.want, got)
		})
	}
}

func TestMockedAPIProvider_GetBoundPods(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(*MockedAPIProvider, *KubeClientMock)
		clear     bool
		want      []BoundPod
		wantPanic bool
	}{
		{
			name: "successful get bound pods with clear",
			setupMock: func(m *MockedAPIProvider, k *KubeClientMock) {
				m.clients.KubeClient = k
				k.boundPods = []BoundPod{
					{Pod: "pod1", Host: "ns1"},
					{Pod: "pod2", Host: "ns2"},
				}
			},
			clear: true,
			want: []BoundPod{
				{Pod: "pod1", Host: "ns1"},
				{Pod: "pod2", Host: "ns2"},
			},
			wantPanic: false,
		},
		{
			name: "successful get bound pods without clear",
			setupMock: func(m *MockedAPIProvider, k *KubeClientMock) {
				m.clients.KubeClient = k
				k.boundPods = []BoundPod{
					{Pod: "pod3", Host: "ns3"},
				}
			},
			clear: false,
			want: []BoundPod{
				{Pod: "pod3", Host: "ns3"},
			},
			wantPanic: false,
		},
		{
			name: "panic when kubeClient is not KubeClientMock",
			setupMock: func(m *MockedAPIProvider, k *KubeClientMock) {
				m.clients.KubeClient = nil
			},
			clear:     true,
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MockedAPIProvider{
				clients: &Clients{},
			}
			k := &KubeClientMock{}

			tt.setupMock(m, k)

			if tt.wantPanic {
				assert.Assert(t, panics(func() { m.GetBoundPods(tt.clear) }))
				return
			}

			got := m.GetBoundPods(tt.clear)
			assert.DeepEqual(t, tt.want, got)
		})
	}
}

func panics(f func()) (didPanic bool) {
	defer func() {
		if r := recover(); r != nil {
			didPanic = true
		}
	}()
	f()
	return
}
