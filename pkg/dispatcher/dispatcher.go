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

	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

var dispatcher *Dispatcher
var once sync.Once

type EventType int8

const (
	EventTypeApp EventType = iota
	EventTypeTask
	EventTypeNode
)

var (
	AsyncDispatchLimit         int32
	AsyncDispatchCheckInterval = 3 * time.Second
	DispatchTimeout            time.Duration
	asyncDispatchCount         atomic.Int32 = atomic.Int32{}
)

// central dispatcher that dispatches scheduling events.
type Dispatcher struct {
	eventChan chan events.SchedulingEvent
	stopChan  chan struct{}
	handlers  map[EventType]map[string]func(interface{})
	running   atomic.Bool
	lock      locking.RWMutex
	stopped   sync.WaitGroup
}

func initDispatcher() {
	eventChannelCapacity := conf.GetSchedulerConf().EventChannelCapacity
	dispatcher = &Dispatcher{
		eventChan: make(chan events.SchedulingEvent, eventChannelCapacity),
		handlers:  make(map[EventType]map[string]func(interface{})),
		stopChan:  make(chan struct{}),
		lock:      locking.RWMutex{},
	}
	dispatcher.setRunning(false)
	DispatchTimeout = conf.GetSchedulerConf().DispatchTimeout
	AsyncDispatchLimit = max(10000, int32(eventChannelCapacity/10)) //nolint:gosec

	log.Log(log.ShimDispatcher).Info("Init dispatcher",
		zap.Int("EventChannelCapacity", eventChannelCapacity),
		zap.Int32("AsyncDispatchLimit", AsyncDispatchLimit),
		zap.Float64("DispatchTimeoutInSeconds", DispatchTimeout.Seconds()))
}

func RegisterEventHandler(handlerID string, eventType EventType, handlerFn func(interface{})) {
	eventDispatcher := getDispatcher()
	eventDispatcher.lock.Lock()
	defer eventDispatcher.lock.Unlock()
	if _, ok := eventDispatcher.handlers[eventType]; !ok {
		eventDispatcher.handlers[eventType] = make(map[string]func(interface{}))
	}
	eventDispatcher.handlers[eventType][handlerID] = handlerFn
}

func UnregisterEventHandler(handlerID string, eventType EventType) {
	eventDispatcher := getDispatcher()
	eventDispatcher.lock.Lock()
	defer eventDispatcher.lock.Unlock()
	if _, ok := eventDispatcher.handlers[eventType]; ok {
		delete(eventDispatcher.handlers[eventType], handlerID)
		if len(eventDispatcher.handlers[eventType]) == 0 {
			delete(eventDispatcher.handlers, eventType)
		}
	}
}

func UnregisterAllEventHandlers() {
	eventDispatcher := getDispatcher()
	eventDispatcher.lock.Lock()
	defer eventDispatcher.lock.Unlock()
	eventDispatcher.handlers = make(map[EventType]map[string]func(interface{}))
}

// a thread-safe way to get event handlers
func getEventHandler(eventType EventType) func(interface{}) {
	eventDispatcher := getDispatcher()
	eventDispatcher.lock.RLock()
	defer eventDispatcher.lock.RUnlock()

	handlers := make([]func(interface{}), 0)
	for _, handler := range eventDispatcher.handlers[eventType] {
		handlers = append(handlers, handler)
	}
	return func(event interface{}) {
		for _, handler := range handlers {
			handler(event)
		}
	}
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
		log.Log(log.ShimDispatcher).Warn("failed to dispatch SchedulingEvent",
			zap.Error(err))
	}
}

func (p *Dispatcher) isRunning() bool {
	return p.running.Load()
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
	count := asyncDispatchCount.Add(1)
	log.Log(log.ShimDispatcher).Warn("event channel is full, transition to async-dispatch mode",
		zap.Int32("asyncDispatchCount", count))
	if count > AsyncDispatchLimit {
		panic(fmt.Errorf("dispatcher exceeds async-dispatch limit"))
	}
	go func(beginTime time.Time, stop chan struct{}) {
		defer asyncDispatchCount.Add(-1)
		for p.isRunning() {
			select {
			case <-stop:
				return
			case p.eventChan <- event:
				return
			case <-time.After(AsyncDispatchCheckInterval):
				elapseTime := time.Since(beginTime)
				if elapseTime >= DispatchTimeout {
					log.Log(log.ShimDispatcher).Error("dispatch timeout",
						zap.Float64("elapseSeconds", elapseTime.Seconds()))
					return
				}
				log.Log(log.ShimDispatcher).Warn("event channel is full, keep waiting...",
					zap.Float64("elapseSeconds", elapseTime.Seconds()))
			}
		}
	}(time.Now(), p.stopChan)
}

func (p *Dispatcher) drain() {
	for len(p.eventChan) > 0 {
		log.Log(log.ShimDispatcher).Info("wait dispatcher to drain",
			zap.Int("remaining events", len(p.eventChan)))
		time.Sleep(1 * time.Second)
	}
	log.Log(log.ShimDispatcher).Info("dispatcher is draining out")
}

func Start() {
	log.Log(log.ShimDispatcher).Info("starting the dispatcher")
	if getDispatcher().isRunning() {
		log.Log(log.ShimDispatcher).Info("dispatcher is already running")
		return
	}
	getDispatcher().stopChan = make(chan struct{})
	getDispatcher().stopped.Add(1)
	go func() {
		for {
			select {
			case event := <-getDispatcher().eventChan:
				switch v := event.(type) {
				case events.TaskEvent:
					getEventHandler(EventTypeTask)(v)
				case events.ApplicationEvent:
					getEventHandler(EventTypeApp)(v)
				case events.SchedulerNodeEvent:
					getEventHandler(EventTypeNode)(v)
				default:
					log.Log(log.ShimDispatcher).Fatal("unsupported event",
						zap.Any("event", v))
				}
			case <-getDispatcher().stopChan:
				log.Log(log.ShimDispatcher).Info("shutting down event channel")
				getDispatcher().setRunning(false)
				getDispatcher().stopped.Done()
				return
			}
		}
	}()
	getDispatcher().setRunning(true)
}

// stop the dispatcher and wait at most 5 seconds gracefully
func Stop() {
	log.Log(log.ShimDispatcher).Info("stopping the dispatcher")

	var chanClosed bool
	select {
	case <-getDispatcher().stopChan:
		chanClosed = true
	default:
	}

	if chanClosed {
		if getDispatcher().isRunning() {
			log.Log(log.ShimDispatcher).Info("dispatcher shutdown in progress")
		} else {
			log.Log(log.ShimDispatcher).Info("dispatcher is already stopped")
		}
		return
	}

	close(getDispatcher().stopChan)
	stopWait := make(chan struct{})

	go func() {
		defer close(stopWait)
		getDispatcher().stopped.Wait()
	}()

	// wait until the main event loop stops properly
	select {
	case <-stopWait:
		break
	case <-time.After(5 * time.Second):
		log.Log(log.ShimDispatcher).Info("dispatcher did not stop in time")
		break
	}

	if getDispatcher().isRunning() {
		log.Log(log.ShimDispatcher).Warn("dispatcher even processing did not stop properly")
	} else {
		log.Log(log.ShimDispatcher).Info("dispatcher stopped successfully")
	}
}
