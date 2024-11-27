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

package k8s

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func ScheduleSuccessEvent(ns, podName, nodeName string) func(*v1.Event) bool {
	return func(e *v1.Event) bool {
		return e.Type == v1.EventTypeNormal &&
			e.Reason == "Scheduled" &&
			strings.HasPrefix(e.Name, podName) &&
			strings.Contains(e.Message, fmt.Sprintf("Successfully assigned %v/%v to %v", ns, podName, nodeName))
	}
}

func ScheduleFailureEvent(podName string) func(*v1.Event) bool {
	return func(e *v1.Event) bool {
		return strings.HasPrefix(e.Name, podName) &&
			e.Type == "Warning" &&
			e.Reason == "FailedScheduling"
	}
}

// Action is a function to be performed by the system.
type Action func() error

// observeEventAfterAction returns true if an event matching the predicate was emitted
// from the system after performing the supplied action.
func ObserveEventAfterAction(c clientset.Interface, ns string, eventPredicate func(*v1.Event) bool, action Action) (bool, error) {
	observedMatchingEvent := false
	informerStartedChan := make(chan struct{})
	var informerStartedGuard sync.Once

	// Create an informer to list/watch events from the test framework namespace.
	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: &cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				ls, err := c.CoreV1().Events(ns).List(context.TODO(), options)
				return ls, err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				// Signal parent goroutine that watching has begun.
				defer informerStartedGuard.Do(func() { close(informerStartedChan) })
				w, err := c.CoreV1().Events(ns).Watch(context.TODO(), options)
				return w, err
			},
		},
		ObjectType: &v1.Event{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				e, ok := obj.(*v1.Event)
				ginkgo.By(fmt.Sprintf("Considering event: \nType = [%s], Name = [%s], Reason = [%s], Message = [%s]", e.Type, e.Name, e.Reason, e.Message))
				gomega.Expect(ok).To(gomega.BeTrue())
				if eventPredicate(e) {
					observedMatchingEvent = true
				}
			},
		},
		ResyncPeriod: 0,
	})

	// Start the informer and block this goroutine waiting for the started signal.
	informerStopChan := make(chan struct{})
	defer func() { close(informerStopChan) }()
	go controller.Run(informerStopChan)
	<-informerStartedChan

	// Invoke the action function.
	err := action()
	if err != nil {
		return false, err
	}

	// Poll whether the informer has found a matching event with a timeout.
	// Wait up 2 minutes polling every second.
	timeout := 2 * time.Minute
	interval := 1 * time.Second
	err = wait.PollUntilContextTimeout(context.TODO(), interval, timeout, false, func(context.Context) (bool, error) {
		return observedMatchingEvent, nil
	})
	return err == nil, err
}

type EventHandler struct {
	updateCh chan struct{}
}

func (e *EventHandler) OnAdd(_ interface{}, _ bool) {}

func (e *EventHandler) OnUpdate(_, _ interface{}) {
	e.updateCh <- struct{}{}
}

func (e *EventHandler) OnDelete(_ interface{}) {}

func (e *EventHandler) WaitForUpdate(timeout time.Duration) bool {
	t := time.After(timeout)

	for {
		select {
		case <-t:
			return false
		case <-e.updateCh:
			return true
		}
	}
}

func ObserveConfigMapInformerUpdateAfterAction(action func()) {
	kubeClient := KubeCtl{}
	gomega.Expect(kubeClient.SetClient()).To(gomega.Succeed())

	// Setup ConfigMap informer
	stopChan := make(chan struct{})
	eventHandler := &EventHandler{updateCh: make(chan struct{})}
	err := kubeClient.StartConfigMapInformer(configmanager.YuniKornTestConfig.YkNamespace, stopChan, eventHandler)
	defer close(stopChan)
	gomega.Ω(err).ShouldNot(gomega.HaveOccurred())

	// Trigger action
	action()

	// Wait for ConfigMap informer recevie update event.
	updateOk := eventHandler.WaitForUpdate(30 * time.Second)
	gomega.Ω(updateOk).To(gomega.BeTrue())
}
