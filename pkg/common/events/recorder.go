/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package events

import (
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sync"
)

var eventRecorder record.EventRecorder = record.NewFakeRecorder(1024)
var once sync.Once

func GetRecorder() record.EventRecorder {
	once.Do(func() {
		// note, the initiation of the event recorder requires on a workable Kubernetes client,
		// in test mode we should skip this and just use a fake recorder instead.
		configs := conf.GetSchedulerConf()
		if !configs.TestMode {
			k8sClient := client.NewKubeClient(configs.KubeConfig)
			eventBroadcaster := record.NewBroadcaster()
			eventBroadcaster.StartRecordingToSink(&v1.EventSinkImpl{
				Interface: k8sClient.GetClientSet().CoreV1().Events("")})
			eventRecorder = eventBroadcaster.NewRecorder(scheme.Scheme,
				corev1.EventSource{Component: configs.SchedulerName})
		}
	})

	return eventRecorder
}