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
	"net/http"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

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
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "placeholder"}}
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
