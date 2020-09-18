package federation

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var once sync.Once
var member *MembershipService

type MembershipService struct {
	headAddress string
	client si.SchedulerClient
	reportingChan chan *si.UpdateRequest
}


func GetMembershipService() *MembershipService {
	once.Do(func() {
		headAddr := os.Getenv("YUNIKORN_HEAD_ADDRESS")
		// Set up a connection to the server.
		conn, err := grpc.Dial(headAddr, grpc.WithInsecure())
		if err != nil {
			log.Logger().Fatal("failed to connect to the server",
				zap.Error(err))
		}

		member = &MembershipService {
			headAddress: headAddr,
			client: si.NewSchedulerClient(conn),
			reportingChan: make(chan *si.UpdateRequest, 1024*1024),
		}
	})
	return member
}

// start membership service
func (m *MembershipService) Start() {
	// start a go routine that sends updates to the head in gRPC stream
	go m.asyncUpdate()
}

func (m *MembershipService) ReconcileMembership() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// register the RM first
	if _, err := m.client.RegisterResourceManager(ctx, &si.RegisterResourceManagerRequest{
		RmID:                 conf.GetSchedulerConf().ClusterID,
		Version:              "v1",
		PolicyGroup:          "queues",
	}); err != nil {
		log.Logger().Error("could not register to the head",
			zap.Error(err))
		return err
	}

	log.Logger().Info("membership registered",
		zap.String("headAddress", m.headAddress))
	return nil
}

// func (m *MembershipService) asyncUpdate() {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	cleanupFn := func() {
// 		log.Logger().Warn("release TCP connection")
// 		cancel()
// 	}
// 	defer cleanupFn()
// 	stream, err := m.client.Update(ctx)
// 	if err != nil {
// 		log.Logger().Fatal("failed to get the stream",
// 			zap.Error(err))
// 	}
//
// 	for {
// 		select {
// 		case report :=<- m.reportingChan:
// 			if err := stream.Send(report); err != nil {
// 				log.Logger().Error("failed to send UpdateRequest to the server head",
// 					zap.Any("request", report),
// 					zap.Error(err))
//
// 			} else {
// 				log.Logger().Info("reporting updates to the head server")
// 			}
// 		}
// 	}
// }

func (m *MembershipService) asyncUpdate() {
	for {
		select {
		  case report :=<- m.reportingChan:
			  {
				  ctx := context.Background()
				  stream, err := m.client.Update(ctx)
				  if err != nil {
					  log.Logger().Fatal("failed to get the stream",
						  zap.Error(err))
				  }
				  if err := stream.Send(report); err != nil {
					  log.Logger().Error("failed to send UpdateRequest to the server head",
						  zap.Any("request", report),
						  zap.Error(err))

				  } else {
					  log.Logger().Info("reporting updates to the head server")
				  }
			  }
		}
	}
}

func (m *MembershipService) FederationEventHandler() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(AsyncUpdateFederationEvent); ok {
			for _, update := range event.GetConfirmedUpdates() {
				log.Logger().Info("Enqueue the update event in the reporting channel")
				m.reportingChan <- update
			}
		}
	}
}