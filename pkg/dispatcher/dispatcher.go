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
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
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
	running   atomic.Value
	lock      sync.RWMutex
}

func init() {
	once.Do(func() {
		if dispatcher == nil {
			dispatcher = &Dispatcher{
				eventChan: make(chan events.SchedulingEvent, conf.GetSchedulerConf().EventChannelCapacity),
				handlers:  make(map[EventType]func(interface{})),
				stopChan:  make(chan struct{}),
				running:   atomic.Value{},
				lock:      sync.RWMutex{},
			}
			dispatcher.setRunning(false)
		}
	})
}

func RegisterEventHandler(eventType EventType, handlerFn func(interface{})) {
	dispatcher.lock.Lock()
	defer dispatcher.lock.Unlock()
	dispatcher.handlers[eventType] = handlerFn
}

// a thread-safe way to get event handlers
func getEventHandler(eventType EventType) func(interface{}) {
	dispatcher.lock.RLock()
	defer dispatcher.lock.RUnlock()
	return dispatcher.handlers[eventType]
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

func (p *Dispatcher) isRunning() bool {
	return p.running.Load().(bool)
}

func (p *Dispatcher) setRunning(flag bool) {
	p.running.Store(flag)
}

func (p *Dispatcher) dispatch(event events.SchedulingEvent) error {
	if !p.isRunning() {
		return fmt.Errorf("dispatcher is not running")
	}

	select {
	case p.eventChan <- event:
		return nil
	default:
		return fmt.Errorf("failed to dispatch event," +
			" event channel is full or closed")
	}
}

func (p *Dispatcher) drain() {
	for len(p.eventChan) > 0 {
		log.Logger.Info("wait dispatcher to drain",
			zap.Int("remaining events", len(p.eventChan)))
		time.Sleep(1 * time.Second)
	}
	log.Logger.Info("dispatcher is draining out")
}

func Start() {
	log.Logger.Info("starting the dispatcher")
	go func() {
		for {
			select {
			case event := <-dispatcher.eventChan:
				switch v := event.(type) {
				case events.ApplicationEvent:
					getEventHandler(EventTypeApp)(v)
				case events.TaskEvent:
					getEventHandler(EventTypeTask)(v)
				case events.SchedulerEvent:
					getEventHandler(EventTypeScheduler)(v)
				case events.SchedulerNodeEvent:
					getEventHandler(EventTypeNode)(v)
				default:
					log.Logger.Fatal("unsupported event",
						zap.Any("event", v))
				}
			case <-dispatcher.stopChan:
				log.Logger.Info("shutting down event channel")
				dispatcher.setRunning(false)
				return
			}
		}
	}()
	dispatcher.setRunning(true)
}

// stop the dispatcher and wait at most 5 seconds gracefully
func Stop() {
	log.Logger.Info("stopping the dispatcher")
	select {
	case dispatcher.stopChan <- struct{}{}:
		maxTimeout := 5
		for dispatcher.isRunning() && maxTimeout > 0 {
			log.Logger.Info("waiting for dispatcher to be stopped",
				zap.Int("remainingSeconds", maxTimeout))
			time.Sleep(1 * time.Second)
			maxTimeout--
		}
		if !dispatcher.isRunning() {
			log.Logger.Info("dispatcher stopped")
		}
	default:
		log.Logger.Info("dispatcher is already stopped")
	}
}

