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

package yunikorn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func TestHasNegativeNodeResources(t *testing.T) {
	healthyNode := dao.NodeDAOInfo{
		NodeID:    "node-1",
		Capacity:  map[string]int64{"memory": 1000, "vcore": 4},
		Allocated: map[string]int64{"memory": 100, "vcore": 1},
		Occupied:  map[string]int64{"memory": 0, "vcore": 0},
		Available: map[string]int64{"memory": 900, "vcore": 3},
	}

	tests := []struct {
		name  string
		nodes []dao.NodeDAOInfo
		want  bool
	}{
		{
			name:  "healthy node",
			nodes: []dao.NodeDAOInfo{healthyNode},
			want:  false,
		},
		{
			name: "negative available resources",
			nodes: []dao.NodeDAOInfo{{
				NodeID:    "yk8s-worker",
				Capacity:  map[string]int64{"memory": 1000, "vcore": 4},
				Allocated: map[string]int64{"memory": 100, "vcore": 1},
				Occupied:  map[string]int64{"memory": 0, "vcore": 0},
				Available: map[string]int64{"memory": -1, "vcore": 3},
			}},
			want: true,
		},
		{
			name: "negative allocated resources",
			nodes: []dao.NodeDAOInfo{func() dao.NodeDAOInfo {
				node := healthyNode
				node.Allocated = map[string]int64{"memory": -1, "vcore": 1}
				return node
			}()},
			want: true,
		},
		{
			name: "negative capacity resources",
			nodes: []dao.NodeDAOInfo{func() dao.NodeDAOInfo {
				node := healthyNode
				node.Capacity = map[string]int64{"memory": -1, "vcore": 4}
				return node
			}()},
			want: true,
		},
		{
			name: "negative occupied resources",
			nodes: []dao.NodeDAOInfo{func() dao.NodeDAOInfo {
				node := healthyNode
				node.Occupied = map[string]int64{"memory": -1, "vcore": 0}
				return node
			}()},
			want: true,
		},
		{
			name:  "empty node list",
			nodes: []dao.NodeDAOInfo{},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasNegativeNodeResources(tt.nodes); got != tt.want {
				t.Fatalf("HasNegativeNodeResources() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWaitForSchedulerHealth(t *testing.T) {
	healthyNodes := []dao.NodeDAOInfo{{
		NodeID:    "node-1",
		Capacity:  map[string]int64{"memory": 1000, "vcore": 4},
		Allocated: map[string]int64{"memory": 0, "vcore": 0},
		Occupied:  map[string]int64{"memory": 0, "vcore": 0},
		Available: map[string]int64{"memory": 1000, "vcore": 4},
	}}
	healthyHealthCheck := dao.SchedulerHealthDAOInfo{Healthy: true}
	unhealthyHealthCheck := dao.SchedulerHealthDAOInfo{
		Healthy: false,
		HealthChecks: []dao.HealthCheckInfo{{
			Name:             "Negative resources",
			Succeeded:        false,
			DiagnosisMessage: `Nodes with negative resources: ["yk8s-worker"]`,
		}},
	}

	var healthCheckCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws/v1/partition/default/nodes":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(healthyNodes); err != nil {
				t.Fatalf("failed to encode nodes response: %v", err)
			}
		case "/ws/v1/scheduler/healthcheck":
			w.Header().Set("Content-Type", "application/json")
			call := healthCheckCalls.Add(1)
			response := unhealthyHealthCheck
			if call >= 2 {
				response = healthyHealthCheck
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Fatalf("failed to encode health check response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	client := &RClient{
		BaseURL:    serverURL,
		httpClient: server.Client(),
	}

	if err := client.WaitForSchedulerHealth(configmanager.DefaultPartition, 5); err != nil {
		t.Fatalf("WaitForSchedulerHealth() error = %v", err)
	}
	if healthCheckCalls.Load() < 2 {
		t.Fatalf("expected health check to be polled until healthy, got %d calls", healthCheckCalls.Load())
	}
}

func TestWaitForSchedulerHealthNegativeNodes(t *testing.T) {
	var nodeResponses atomic.Int32
	healthyNodes := []dao.NodeDAOInfo{{
		NodeID:    "node-1",
		Capacity:  map[string]int64{"memory": 1000, "vcore": 4},
		Allocated: map[string]int64{"memory": 0, "vcore": 0},
		Occupied:  map[string]int64{"memory": 0, "vcore": 0},
		Available: map[string]int64{"memory": 1000, "vcore": 4},
	}}
	negativeNodes := []dao.NodeDAOInfo{{
		NodeID:    "yk8s-worker",
		Capacity:  map[string]int64{"memory": 1000, "vcore": 4},
		Allocated: map[string]int64{"memory": 100, "vcore": 1},
		Occupied:  map[string]int64{"memory": 0, "vcore": 0},
		Available: map[string]int64{"memory": -100, "vcore": 3},
	}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws/v1/partition/default/nodes":
			w.Header().Set("Content-Type", "application/json")
			response := negativeNodes
			if nodeResponses.Add(1) >= 2 {
				response = healthyNodes
			}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Fatalf("failed to encode nodes response: %v", err)
			}
		case "/ws/v1/scheduler/healthcheck":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(dao.SchedulerHealthDAOInfo{Healthy: true}); err != nil {
				t.Fatalf("failed to encode health check response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	client := &RClient{
		BaseURL:    serverURL,
		httpClient: server.Client(),
	}

	if err := client.WaitForSchedulerHealth(configmanager.DefaultPartition, 5); err != nil {
		t.Fatalf("WaitForSchedulerHealth() error = %v", err)
	}
	if nodeResponses.Load() < 2 {
		t.Fatalf("expected nodes API to be polled until resources recovered, got %d calls", nodeResponses.Load())
	}
}

func TestWaitForSchedulerHealthTimeout(t *testing.T) {
	negativeNodes := []dao.NodeDAOInfo{{
		NodeID:    "yk8s-worker",
		Capacity:  map[string]int64{"memory": 1000, "vcore": 4},
		Allocated: map[string]int64{"memory": 100, "vcore": 1},
		Occupied:  map[string]int64{"memory": 0, "vcore": 0},
		Available: map[string]int64{"memory": -100, "vcore": 3},
	}}
	unhealthyHealthCheck := dao.SchedulerHealthDAOInfo{
		Healthy: false,
		HealthChecks: []dao.HealthCheckInfo{{
			Name:             "Negative resources",
			Succeeded:        false,
			DiagnosisMessage: `Nodes with negative resources: ["yk8s-worker"]`,
		}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws/v1/partition/default/nodes":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(negativeNodes); err != nil {
				t.Fatalf("failed to encode nodes response: %v", err)
			}
		case "/ws/v1/scheduler/healthcheck":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(unhealthyHealthCheck); err != nil {
				t.Fatalf("failed to encode health check response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	client := &RClient{
		BaseURL:    serverURL,
		httpClient: server.Client(),
	}

	err = client.WaitForSchedulerHealth(configmanager.DefaultPartition, 2)
	if err == nil {
		t.Fatal("WaitForSchedulerHealth() expected error for persistent unhealthy state")
	}
	if !strings.Contains(err.Error(), "scheduler did not become healthy within 2s") {
		t.Fatalf("WaitForSchedulerHealth() error = %v, want timeout message", err)
	}
	if !strings.Contains(err.Error(), "nodes API reported negative resources") {
		t.Fatalf("WaitForSchedulerHealth() error = %v, want nodes API failure detail", err)
	}
}
