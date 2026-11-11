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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestUpdateStatusCancelledWhileAPIServerHangs(t *testing.T) {
	for _, hangOn := range []string{http.MethodGet, http.MethodPut} {
		t.Run(hangOn, func(t *testing.T) {
			pod := &v1.Pod{
				TypeMeta:   apis.TypeMeta{Kind: "Pod", APIVersion: "v1"},
				ObjectMeta: apis.ObjectMeta{Name: "pod-1", Namespace: "default"},
			}
			received := make(chan struct{})
			aborted := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != hangOn {
					w.Header().Set("Content-Type", "application/json")
					assert.NilError(t, json.NewEncoder(w).Encode(pod))
					return
				}
				// the server only notices the client going away once the request body is consumed
				_, err := io.Copy(io.Discard, r.Body)
				assert.NilError(t, err)
				close(received)
				select {
				case <-r.Context().Done():
					close(aborted)
				case <-release:
				}
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(release) })
			kubeClient := SchedulerKubeClient{clientSet: kubernetes.NewForConfigOrDie(&rest.Config{Host: server.URL})}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := kubeClient.UpdateStatus(ctx, pod)
				done <- err
			}()
			<-received
			cancel()

			select {
			case err := <-done:
				assert.Assert(t, err != nil)
			case <-time.After(10 * time.Second):
				t.Fatal("UpdateStatus did not return after its context was cancelled")
			}
			select {
			case <-aborted:
			case <-time.After(10 * time.Second):
				t.Fatal("the hanging request was not aborted")
			}
		})
	}
}

type placeholderRequestTransport func(*http.Request) (*http.Response, error)

func (f placeholderRequestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSchedulerKubeClientPlaceholderRequestsCancellation(t *testing.T) {
	for _, operation := range []string{"create", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			requestStarted := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			clientSet, err := kubernetes.NewForConfig(&rest.Config{
				Host: "https://kubernetes.invalid",
				Transport: placeholderRequestTransport(func(req *http.Request) (*http.Response, error) {
					close(requestStarted)
					select {
					case <-req.Context().Done():
						return nil, req.Context().Err()
					case <-release:
						return nil, context.DeadlineExceeded
					}
				}),
			})
			assert.NilError(t, err)
			kubeClient := SchedulerKubeClient{clientSet: clientSet}
			pod := &v1.Pod{ObjectMeta: apis.ObjectMeta{Namespace: "test", Name: "placeholder"}}
			done := make(chan error, 1)
			go func() {
				if operation == "create" {
					_, err := kubeClient.Create(ctx, pod)
					done <- err
				} else {
					done <- kubeClient.Delete(ctx, pod)
				}
			}()
			select {
			case <-requestStarted:
			case <-time.After(time.Second):
				t.Fatal("Kubernetes request did not start")
			}
			cancel()
			select {
			case err := <-done:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("Kubernetes request did not receive cancellation")
			}
		})
	}
}

const testKubeConfig = `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://localhost:6443
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user: {}
`

func TestClientRateLimit(t *testing.T) {
	testCases := []struct {
		name          string
		qps           int
		burst         int
		expectedQPS   float32
		expectedBurst int
	}{
		{"unset is no limiter", -1, -1, -1, 0},
		{"zero leaves the client-go defaults", 0, 0, 0, 0},
		{"burst defaults to qps", 100, 0, 100, 100},
		{"negative burst defaults to qps", 100, -1, 100, 100},
		{"qps and burst", 100, 50, 100, 50},
		{"burst is kept without a qps", -1, 50, -1, 50},
		{"burst is kept on the client-go defaults", 0, 50, 0, 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qps, burst := clientRateLimit(tc.qps, tc.burst)
			assert.Equal(t, tc.expectedQPS, qps, "unexpected QPS")
			assert.Equal(t, tc.expectedBurst, burst, "unexpected burst")
		})
	}
}

func TestNewRestConfig(t *testing.T) {
	kc := writeKubeConfig(t)

	testCases := []struct {
		name    string
		concern string
		qps     int
		burst   int
	}{
		{"unset", userAgentScheduler, -1, -1},
		{"client-go defaults", userAgentBootstrap, 0, 0},
		{"limited", userAgentEvents, 100, 200},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectedQPS, expectedBurst := clientRateLimit(tc.qps, tc.burst)
			config := newRestConfig(kc, tc.qps, tc.burst, tc.concern)
			assert.Equal(t, tc.concern, config.UserAgent, "user agent not set to the concern")
			// creating the limiter is always left to client-go: a negative QPS makes it
			// create none, a QPS of 0 falls back to the client-go defaults
			assert.Assert(t, config.RateLimiter == nil, "rate limiter must be left to client-go")
			assert.Equal(t, expectedQPS, config.QPS, "QPS not normalised")
			assert.Equal(t, expectedBurst, config.Burst, "burst not normalised")
		})
	}
}

// client-go must accept every configuration we generate, it rejects a QPS which is set
// without a burst and a rejected configuration is fatal at startup
func TestNewClientSetAcceptsRestConfig(t *testing.T) {
	kc := writeKubeConfig(t)

	testCases := []struct {
		name  string
		qps   int
		burst int
	}{
		{"unlimited", 0, 0},
		{"qps without burst", 100, 0},
		{"qps and burst", 100, 200},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := kubernetes.NewForConfig(newRestConfig(kc, tc.qps, tc.burst, userAgentScheduler))
			assert.NilError(t, err, "clientset creation failed")
		})
	}
}

// writeKubeConfig creates a kubeconfig file for a non existing cluster and returns its path
func writeKubeConfig(t *testing.T) string {
	t.Helper()
	kc := filepath.Join(t.TempDir(), "kubeconfig")
	err := os.WriteFile(kc, []byte(testKubeConfig), 0600)
	assert.NilError(t, err, "could not write kubeconfig")
	return kc
}
