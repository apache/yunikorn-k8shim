package federation

import (
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type AsyncUpdateFederationEvent struct {
	ConfirmedRequests []*si.UpdateRequest
}

func (a AsyncUpdateFederationEvent) GetEvent() events.FederationEventType {
	return events.Update
}

func (a AsyncUpdateFederationEvent) GetArgs() []interface{} {
	return nil
}

func (a AsyncUpdateFederationEvent) GetConfirmedUpdates() []*si.UpdateRequest {
	return a.ConfirmedRequests
}
