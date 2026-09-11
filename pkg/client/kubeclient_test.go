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
	"fmt"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestSchedulerKubeClientDeleteDoesNotDeleteReplacementPod(t *testing.T) {
	const (
		namespace = "ns"
		podName   = "victim"
	)
	staleUID := types.UID("uid-a")
	replacementUID := types.UID("uid-b")
	stalePod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      podName,
		UID:       staleUID,
	}}
	replacementPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      podName,
		UID:       replacementUID,
	}}

	clientSet := fake.NewSimpleClientset(replacementPod)
	var observedDeleteOptions metav1.DeleteOptions
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

	livePod, getErr := clientSet.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
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
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
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
