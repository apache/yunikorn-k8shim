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

package cache

import (
	"sync"

	"github.com/apache/yunikorn-k8shim/pkg/log"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
)

type PodEventHandler struct {
	recoveryRunning bool
	amProtocol      ApplicationManagementProtocol
	asyncEvents     []*podAsyncEvent
	sync.Mutex
}

const (
	AddPod = iota
	UpdatePod
	DeletePod
)

const (
	Recovery = iota
	Informers
)

type EventType int
type EventSource int

type podAsyncEvent struct {
	eventType EventType
	pod       *v1.Pod
}

func (p *PodEventHandler) HandleEvent(eventType EventType, source EventSource, pod *v1.Pod) *Application {
	if p.handleEventFromInformers(eventType, source, pod) {
		return nil
	}

	return p.internalHandle(eventType, source, pod)
}

func (p *PodEventHandler) handleEventFromInformers(eventType EventType, source EventSource, pod *v1.Pod) bool {
	p.Lock()
	defer p.Unlock()

	if p.recoveryRunning && source == Informers {
		log.Log(log.ShimAppMgmtGeneral).Debug("Storing async event", zap.Int("eventType", int(eventType)),
			zap.String("pod", pod.GetName()))
		p.asyncEvents = append(p.asyncEvents, &podAsyncEvent{eventType, pod})
		return true
	}
	return false
}

func (p *PodEventHandler) internalHandle(eventType EventType, source EventSource, pod *v1.Pod) *Application {
	switch eventType {
	case AddPod:
		return p.addPod(pod, source)
	case UpdatePod:
		return p.updatePod(pod)
	case DeletePod:
		return p.deletePod(pod)
	default:
		log.Log(log.ShimAppMgmtGeneral).Error("Unknown pod eventType", zap.Int("eventType", int(eventType)))
		return nil
	}
}

func (p *PodEventHandler) RecoveryDone(terminatedPods map[string]bool) {
	p.Lock()
	defer p.Unlock()

	noOfEvents := len(p.asyncEvents)
	if noOfEvents > 0 {
		log.Log(log.ShimAppMgmtGeneral).Info("Processing async events that arrived during recovery",
			zap.Int("no. of events", noOfEvents))
		for _, event := range p.asyncEvents {
			// ignore all events for pods that have already been determined to
			// be terminated, as these will not have been recovered and are
			// therefore not claiming any scheduler resources that we care about
			if terminatedPods[string(event.pod.UID)] {
				continue
			}
			p.internalHandle(event.eventType, Informers, event.pod)
		}
	} else {
		log.Log(log.ShimAppMgmtGeneral).Info("No async pod events to process")
	}

	p.recoveryRunning = false
	p.asyncEvents = nil
}

func (p *PodEventHandler) addPod(pod *v1.Pod, eventSource EventSource) *Application {
	recovery := eventSource == Recovery
	var app *Application
	var appExists bool

	// add app
	if appMeta, ok := getAppMetadata(pod, recovery); ok {
		// check if app already exist
		app = p.amProtocol.GetApplication(appMeta.ApplicationID)
		if app == nil {
			app = p.amProtocol.AddApplication(&AddApplicationRequest{
				Metadata: appMeta,
			})
		} else {
			appExists = true
		}
	}

	// add task
	if taskMeta, ok := getTaskMetadata(pod); ok {
		if _, taskErr := app.GetTask(string(pod.UID)); taskErr != nil {
			p.amProtocol.AddTask(&AddTaskRequest{
				Metadata: taskMeta,
			})
		}
	}

	// only trigger recovery once - if appExists = true, it means we already
	// called TriggerAppRecovery()
	if recovery && !appExists {
		err := app.TriggerAppRecovery()
		if err != nil {
			log.Log(log.ShimAppMgmtGeneral).Error("failed to recover app", zap.Error(err))
		}
	}

	return app
}

func (p *PodEventHandler) updatePod(pod *v1.Pod) *Application {
	if taskMeta, ok := getTaskMetadata(pod); ok {
		if app := p.amProtocol.GetApplication(taskMeta.ApplicationID); app != nil {
			p.amProtocol.NotifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
			return app
		}
	}
	return nil
}

func (p *PodEventHandler) deletePod(pod *v1.Pod) *Application {
	if taskMeta, ok := getTaskMetadata(pod); ok {
		if app := p.amProtocol.GetApplication(taskMeta.ApplicationID); app != nil {
			p.amProtocol.NotifyTaskComplete(taskMeta.ApplicationID, taskMeta.TaskID)
			return app
		}
	}
	return nil
}

func NewPodEventHandler(amProtocol ApplicationManagementProtocol, recoveryRunning bool) *PodEventHandler {
	asyncEvents := make([]*podAsyncEvent, 0)
	podEventHandler := &PodEventHandler{
		recoveryRunning: recoveryRunning,
		asyncEvents:     asyncEvents,
		amProtocol:      amProtocol,
	}

	return podEventHandler
}
