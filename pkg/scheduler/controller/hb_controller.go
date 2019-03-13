package controller
//
//import (
//	"github.com/leftnoteasy/si-spec/lib/go/si"
//	"github.com/leftnoteasy/uscheduler/pkg/unityscheduler/api"
//	"github.com/sunilgovind/simplekubescheduler/pkg/embedded-us/pkg/common"
//	"github.com/sunilgovind/simplekubescheduler/pkg/embedded-us/pkg/scheduler/state"
//	"log"
//	"sync"
//	"time"
//)
//
//type HeartbeatController struct {
//	Proxy api.SchedulerApi
//}
//
//// heartbeat node resource to the scheduler
//func (hb *HeartbeatController) hb(context *state.Context) {
//	log.Println("** Sending heartbeat to the scheduler")
//	nodeList, err := context.KubernetesClient.GetNodes()
//	if err != nil {
//		log.Printf("fail to get nodes info, heartbeat failed")
//		return
//	}
//
//	var nodesInfo = make([]*si.UpdateNodeInfo, len(nodeList.Items))
//	for idx, node := range nodeList.Items {
//		nodesInfo[idx] = &si.UpdateNodeInfo{
//			NodeId: node.Metadata.Uid,
//			SchedulableResource: common.GetNodeResource(&node.Status),
//		}
//		log.Printf("node uid %s, resource: %s, ",
//			nodesInfo[idx].NodeId,
//			nodesInfo[idx].SchedulableResource.String())
//	}
//
//	request := si.UpdateRequest{
//		UpdatedNodes: nodesInfo,
//		RmId:         common.ClusterId,
//	}
//
//	log.Printf("HB Request: %s", request.String())
//	err = context.UnityScheduler.Update(&request)
//	if err != nil {
//		log.Printf("failed to update node resource, request: %s", request.String())
//	}
//}
//
//func heartbeat(interval int, done chan struct{}, context *state.Context, wg *sync.WaitGroup) {
//	//processorLock.Lock()
//	//defer processorLock.Unlock()
//	for {
//		select {
//		case <-time.After(time.Duration(interval) * time.Second):
//			hb(context)
//		case <-done:
//			wg.Done()
//			log.Println("Stopped heartbeat loop.")
//			return
//		}
//	}
//}