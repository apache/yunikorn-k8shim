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
