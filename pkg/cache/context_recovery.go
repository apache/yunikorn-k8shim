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

package cache

import (
	"fmt"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/listers/core/v1"
	"time"
)

func (ctx *Context) WaitForRecovery(maxTimeout time.Duration) error {
	return ctx.waitForRecovery(ctx.nodeInformer.Lister(), maxTimeout)
}

func (ctx *Context) waitForRecovery(lister v1.NodeLister, maxTimeout time.Duration) error {
	allNodes, err := lister.List(labels.Everything())
	if err != nil {
		return err
	}

	deadline := time.Now().Add(maxTimeout)
	for {
		numOfHealthyNodes := 0
		for _, node := range allNodes {
			if cached, ok := ctx.nodes.nodesMap[node.Name]; ok {
				if cached.getNodeState() == events.States().Node.Healthy {
					numOfHealthyNodes++
				}
			}
		}

		if numOfHealthyNodes >= len(allNodes) {
			log.Logger.Info("recovering complete",
				zap.Int("nodesRecovered", numOfHealthyNodes))
			return nil
		}

		log.Logger.Info("still waiting for recovery",
			zap.Int("totalNodes", len(allNodes)),
			zap.Int("numOfRecoveredNodes", numOfHealthyNodes))

		if time.Now().After(deadline) {
			log.Logger.Warn("timeout waiting for exiting recovery state",
				zap.String("timeout", maxTimeout.String()))
			return fmt.Errorf(fmt.Sprintf("timeout waiting for exiting recovery state, timeout=%s",
				maxTimeout.String()))
		}

		time.Sleep(time.Duration(1) * time.Second)
	}
}