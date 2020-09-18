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

package dispatcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

var dispatcher *Dispatcher
var once sync.Once

type EventType int8

const (
	EventTypeApp EventType = iota
	EventTypeTask
	EventTypeNode
	EventTypeScheduler
	EventTypeAppStatus
)

var (
	AsyncDispatchLimit         int32
	AsyncDispatchCheckInterval = 3 * time.Second
	DispatchTimeout            time.Duration
	asyncDispatchCount         int32 = 0
)

// central dispatcher that dispatches scheduling events.
type Dispatcher struct {
	eventChan chan events.SchedulingEvent
	stopChan  chan struct{}
	handlers  map[EventType]func(interface{})
	running   atomic.Value
	lock      sync.RWMutex
}

func initDispatcher() {
	eventChannelCapacity := conf.GetSchedulerConf().EventChannelCapacity
	if dispatcher == nil {
		dispatcher = &Dispatcher{
			eventChan: make(chan events.SchedulingEvent, eventChannelCapacity),
			handlers:  make(map[EventType]func(interface{})),
			stopChan:  make(chan struct{}),
			running:   atomic.Value{},
			lock:      sync.RWMutex{},
		}
		dispatcher.setRunning(false)
	}
	DispatchTimeout = conf.GetSchedulerConf().DispatchTimeout
	AsyncDispatchLimit = int32(eventChannelCapacity / 10)
	if AsyncDispatchLimit < 10000 {
		AsyncDispatchLimit = 10000
	}
	log.Logger().Info("Init dispatcher",
		zap.Int("EventChannelCapacity", eventChannelCapacity),
		zap.Int32("AsyncDispatchLimit", AsyncDispatchLimit),
		zap.Float64("DispatchTimeoutInSeconds", DispatchTimeout.Seconds()))
}

func RegisterEventHandler(eventType EventType, handlerFn func(interface{})) {
	eventDispatcher := getDispatcher()
	eventDispatcher.lock.Lock()
	defer eventDispatcher.lock.Unlock()
	eventDispatcher.handlers[eventType] = handlerFn
}

// a thread-safe way to get event handlers
func getEventHandler(eventType EventType) func(interface{}) {
	eventDispatcher := getDispatcher()
	eventDispatcher.lock.RLock()
	defer eventDispatcher.lock.RUnlock()
	return eventDispatcher.handlers[eventType]
}

func getDispatcher() *Dispatcher {
	// init the dispatcher if it hasn't yet done
	// this is only called once
	once.Do(initDispatcher)
	return dispatcher
}

// dispatches scheduler events to actual app/task handler,
// each app/task has its own state machine and maintain their own states.
// currently all events share same channel, so they are dispatched
// one by one in order.
func Dispatch(event events.SchedulingEvent) {
	// currently if dispatch fails, we simply log the error
	// we may revisit this later, e.g add retry here
	if err := getDispatcher().dispatch(event); err != nil {
		log.Logger().Warn("failed to dispatch SchedulingEvent",
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
		p.asyncDispatch(event)
		return nil
	}
}

// async-dispatch try to enqueue the event in every 3 seconds util timeout,
// it's only called when event channel is full.
func (p *Dispatcher) asyncDispatch(event events.SchedulingEvent) {
	count := atomic.AddInt32(&asyncDispatchCount, 1)
	log.Logger().Warn("event channel is full, transition to async-dispatch mode",
		zap.Int32("asyncDispatchCount", count))
	if count > AsyncDispatchLimit {
		panic(fmt.Errorf("dispatcher exceeds async-dispatch limit"))
	}
	go func(beginTime time.Time, stop chan struct{}) {
		defer atomic.AddInt32(&asyncDispatchCount, -1)
		for p.isRunning() {
			select {
			case <-stop:
				return
			case p.eventChan <- event:
				return
			case <-time.After(AsyncDispatchCheckInterval):
				elapseTime := time.Since(beginTime)
				if elapseTime >= DispatchTimeout {
					log.Logger().Error("dispatch timeout",
						zap.Float64("elapseSeconds", elapseTime.Seconds()))
					return
				}
				log.Logger().Warn("event channel is full, keep waiting...",
					zap.Float64("elapseSeconds", elapseTime.Seconds()))
			}
		}
	}(time.Now(), p.stopChan)
}

func (p *Dispatcher) drain() {
	for len(p.eventChan) > 0 {
		log.Logger().Info("wait dispatcher to drain",
			zap.Int("remaining events", len(p.eventChan)))
		time.Sleep(1 * time.Second)
	}
	log.Logger().Info("dispatcher is draining out")
}

func Start() {
	log.Logger().Info("starting the dispatcher")
	go func() {
		for {
			select {
			case event := <-getDispatcher().eventChan:
				switch v := event.(type) {
				case events.ApplicationStatusEvent:
					getEventHandler(EventTypeAppStatus)(v)
				case events.ApplicationEvent:
					getEventHandler(EventTypeApp)(v)
				case events.TaskEvent:
					getEventHandler(EventTypeTask)(v)
				case events.SchedulerEvent:
					getEventHandler(EventTypeScheduler)(v)
				case events.SchedulerNodeEvent:
					getEventHandler(EventTypeNode)(v)
				default:
					log.Logger().Fatal("unsupported event",
						zap.Any("event", v))
				}
			case <-getDispatcher().stopChan:
				log.Logger().Info("shutting down event channel")
				getDispatcher().setRunning(false)
				return
			}
		}
	}()
	getDispatcher().setRunning(true)
}

// stop the dispatcher and wait at most 5 seconds gracefully
func Stop() {
	log.Logger().Info("stopping the dispatcher")
	select {
	case getDispatcher().stopChan <- struct{}{}:
		maxTimeout := 5
		for getDispatcher().isRunning() && maxTimeout > 0 {
			log.Logger().Info("waiting for dispatcher to be stopped",
				zap.Int("remainingSeconds", maxTimeout))
			time.Sleep(1 * time.Second)
			maxTimeout--
		}
		if !getDispatcher().isRunning() {
			log.Logger().Info("dispatcher stopped")
		}
	default:
		log.Logger().Info("dispatcher is already stopped")
	}
}
