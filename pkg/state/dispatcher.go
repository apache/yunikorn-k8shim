package state

import (
	"errors"
	"github.com/golang/glog"
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
func (p *Dispatcher) SetContext(context *Context) {
	p.context = context
}

// dispatches scheduler events to actual app/task handler,
// each app/task has its own state machine and maintain their own states.
// currently all events share same channel, so they are dispatched
// one by one in order.
func (p *Dispatcher) Dispatch(event SchedulingEvent) error {
	if !p.isRunning {
		return errors.New("dispatcher is not running")
	}

	select {
	case p.eventChan <- event:
		return nil
	default:
		return errors.New("failed to dispatch event")
	}
}

func (p *Dispatcher) handleApplicationEvent(event ApplicationEvent) {
	app, err := p.context.GetApplication(event.getApplicationId())
	if err != nil {
		glog.Error(err.Error())
		return
	}

	if err := app.handle(event); err != nil {
		glog.V(1).Infof("failed to handle event %s, error: %s",
			event.getEvent(), err.Error())
	}
}

func (p *Dispatcher) handleTaskEvent(event TaskEvent) {
	task, err := p.context.GetTask(event.getApplicationId(), event.getTaskId())
	if err != nil {
		glog.Error(err.Error())
		return
	}

	if err := task.handle(event); err != nil {
		glog.V(1).Infof("failed to handle event %s, error: %s",
			event.getEvent(), err.Error())
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
					panic("Unsupported event")
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
