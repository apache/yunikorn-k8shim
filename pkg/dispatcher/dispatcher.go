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

package dispatcher

import (
	"fmt"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	"sync"
)

var dispatcher *Dispatcher
var once sync.Once

type EventType int8

const (
	EventTypeApp EventType = iota
	EventTypeTask
	EventTypeNode
	EventTypeScheduler
)

// central dispatcher that dispatches scheduling events.
type Dispatcher struct {
	eventChan chan events.SchedulingEvent
	stopChan  chan struct{}
	handlers  map[EventType]func(interface{})
	isRunning bool
}

func init() {
	once.Do(func() {
		if dispatcher == nil {
			dispatcher = &Dispatcher{
				eventChan: make(chan events.SchedulingEvent, 1024),
				handlers:  make(map[EventType]func(interface{})),
				isRunning: false,
			}
		}
	})
}

func RegisterEventHandler(eventType EventType, handlerFn func(interface{})) {
	dispatcher.handlers[eventType] = handlerFn
}

// dispatches scheduler events to actual app/task handler,
// each app/task has its own state machine and maintain their own states.
// currently all events share same channel, so they are dispatched
// one by one in order.
func Dispatch(event events.SchedulingEvent) {
	// currently if dispatch fails, we simply log the error
	// we may revisit this later, e.g add retry here
	if err := dispatcher.dispatch(event); err != nil {
		log.Logger.Warn("failed to dispatch SchedulingEvent",
			zap.Error(err))
	}
}

func (p *Dispatcher) dispatch(event events.SchedulingEvent) error {
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

func Start() {
	go func() {
		for {
			select {
			case event := <-dispatcher.eventChan:
				switch v := event.(type) {
				case events.ApplicationEvent:
					dispatcher.handlers[EventTypeApp](v)
				case events.TaskEvent:
					dispatcher.handlers[EventTypeTask](v)
				default:
					log.Logger.Fatal("unsupported event")
				}
			case <-dispatcher.stopChan:
				close(dispatcher.eventChan)
				dispatcher.isRunning = false
				return
			}
		}
	}()
	dispatcher.isRunning = true
}
