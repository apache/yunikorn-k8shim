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

package controller
//
//import (
//	"github.infra.cloudera.com/yunikorn/k8shim/pkg/common"
//	"github.infra.cloudera.com/yunikorn/k8shim/pkg/scheduler/state"
//	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
//	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"
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
