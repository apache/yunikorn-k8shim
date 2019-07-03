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

package state

import (
	"fmt"
	"github.com/cloudera/k8s-shim/pkg/log"
	"go.uber.org/zap"
)

var dispatcher *Dispatcher

// central dispatcher that dispatches scheduling events.
type Dispatcher struct {
	context   *Context
	eventChan chan SchedulingEvent
	stopChan  chan struct{}
	isRunning bool
}

func GetDispatcher() *Dispatcher {
	if dispatcher == nil {
		dispatcher = &Dispatcher{
			eventChan: make(chan SchedulingEvent, 1024),
			isRunning: false,
		}
	}
	return dispatcher
}

// this should be called only on initialization
func (p *Dispatcher) SetContext(ctx *Context) {
	p.context = ctx
}

// dispatches scheduler events to actual app/task handler,
// each app/task has its own state machine and maintain their own states.
// currently all events share same channel, so they are dispatched
// one by one in order.
func (p *Dispatcher) Dispatch(event SchedulingEvent) error {
	if !p.isRunning {
		return fmt.Errorf("dispatcher is not running")
	}

	select {
	case p.eventChan <- event:
		return nil
	default:
		return fmt.Errorf("failed to dispatch event")
	}
}

func (p *Dispatcher) handleApplicationEvent(event ApplicationEvent) {
	app, err := p.context.GetApplication(event.getApplicationId())
	if err != nil {
		log.Logger.Error("failed to handle application event", zap.Error(err))
		return
	}

	if err := app.handle(event); err != nil {
		log.Logger.Error("failed to handle application event",
			zap.String("event", string(event.getEvent())),
			zap.Error(err))
	}
}

func (p *Dispatcher) handleTaskEvent(event TaskEvent) {
	task, err := p.context.GetTask(event.getApplicationId(), event.getTaskId())
	if err != nil {
		log.Logger.Error("failed to handle application event", zap.Error(err))
		return
	}

	if err := task.handle(event); err != nil {
		log.Logger.Error("failed to handle task event",
			zap.String("event", string(event.getEvent())),
			zap.Error(err))
	}
}

func (p *Dispatcher) Start() {
	go func() {
		for {
			select {
			case event := <-p.eventChan:
				switch v := event.(type) {
				case ApplicationEvent:
					p.handleApplicationEvent(v)
				case TaskEvent:
					p.handleTaskEvent(v)
				default:
					log.Logger.Fatal("unsupported event")
				}
			case <-p.stopChan:
				close(p.eventChan)
				p.isRunning = false
				return
			}
		}
	}()
	p.isRunning = true
}
