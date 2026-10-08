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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
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

func TestSchedulerKubeClientDeleteDoesNotDeleteReplacementPod(t *testing.T) {
	const (
		namespace = "ns"
		podName   = "victim"
	)
	staleUID := types.UID("uid-a")
	replacementUID := types.UID("uid-b")
	stalePod := &v1.Pod{ObjectMeta: apis.ObjectMeta{
		Namespace: namespace,
		Name:      podName,
		UID:       staleUID,
	}}
	replacementPod := &v1.Pod{ObjectMeta: apis.ObjectMeta{
		Namespace: namespace,
		Name:      podName,
		UID:       replacementUID,
	}}

	clientSet := fake.NewSimpleClientset(replacementPod)
	var observedDeleteOptions apis.DeleteOptions
	deleteObserved := false
	clientSet.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteAction, ok := action.(k8stesting.DeleteAction)
		if !ok {
			return true, nil, fmt.Errorf("expected DeleteAction, got %T", action)
		}
		deleteObserved = true
		observedDeleteOptions = deleteAction.GetDeleteOptions()

		if observedDeleteOptions.Preconditions != nil &&
			observedDeleteOptions.Preconditions.UID != nil &&
			*observedDeleteOptions.Preconditions.UID != replacementUID {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "pods"},
				deleteAction.GetName(),
				fmt.Errorf("UID precondition %q does not match current UID %q",
					*observedDeleteOptions.Preconditions.UID, replacementUID))
		}

		// The fake tracker ignores DeleteOptions preconditions. Falling through
		// here deliberately models Kubernetes' normal name-based delete only when
		// no mismatched UID precondition was supplied.
		return false, nil, nil
	})

	kubeClient := SchedulerKubeClient{clientSet: clientSet}
	deleteErr := kubeClient.Delete(stalePod)

	if !deleteObserved {
		t.Error("SchedulerKubeClient.Delete did not issue a Pod DELETE")
	}
	t.Logf("actual DeleteOptions: %#v", observedDeleteOptions)
	if observedDeleteOptions.Preconditions == nil || observedDeleteOptions.Preconditions.UID == nil {
		t.Errorf("DELETE UID precondition: got nil, want %q", staleUID)
	} else if *observedDeleteOptions.Preconditions.UID != staleUID {
		t.Errorf("DELETE UID precondition: got %q, want %q",
			*observedDeleteOptions.Preconditions.UID, staleUID)
	}
	if !apierrors.IsConflict(deleteErr) {
		t.Errorf("stale DELETE error: got %v, want Kubernetes Conflict", deleteErr)
	}

	livePod, getErr := clientSet.CoreV1().Pods(namespace).Get(context.Background(), podName, apis.GetOptions{})
	if getErr != nil {
		t.Errorf("replacement Pod UID %q was deleted by stale Pod UID %q: %v",
			replacementUID, staleUID, getErr)
	} else if livePod.UID != replacementUID {
		t.Errorf("live Pod UID: got %q, want replacement UID %q", livePod.UID, replacementUID)
	}
}

func TestSchedulerKubeClientDeleteRejectsEmptyUID(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	kubeClient := SchedulerKubeClient{clientSet: clientSet}
	pod := &v1.Pod{ObjectMeta: apis.ObjectMeta{
		Namespace: "ns",
		Name:      "victim",
	}}

	err := kubeClient.Delete(pod)

	if err == nil {
		t.Fatal("Delete with an empty Pod UID succeeded, want error")
	}
	if !apierrors.IsBadRequest(err) {
		t.Errorf("Delete with an empty Pod UID: got %T (%v), want Kubernetes BadRequest", err, err)
	}
	if !strings.Contains(err.Error(), "cannot delete pod ns/victim without UID") {
		t.Errorf("Delete error: got %q, want a clear empty-UID error", err)
	}
	for _, action := range clientSet.Actions() {
		if action.Matches("delete", "pods") {
			t.Errorf("Delete with an empty Pod UID issued an unexpected Kubernetes DELETE action: %#v", action)
		}
	}
}
