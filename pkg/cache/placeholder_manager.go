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

package cache

import (
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

// placeholder manager is a service to manage the lifecycle of app placeholders
type PlaceholderManager struct {
	clients *client.Clients
	sync.RWMutex
}

var placeholderMgr *PlaceholderManager
var once sync.Once

func GetPlaceholderManager() *PlaceholderManager {
	once.Do(func() {
		placeholderMgr = &PlaceholderManager{}
	})
	return placeholderMgr
}

func (mgr *PlaceholderManager) createAppPlaceholders(app *Application) error {
	mgr.Lock()
	defer mgr.Unlock()

	// iterate all task groups, create placeholders for all the min members
	for _, tg := range app.getTaskGroups() {
		for i := int32(0); i < tg.MinMember; i++ {
			placeholderName := utils.GeneratePlaceholderName(tg.Name, app.GetApplicationID(), i)
			placeholder := newPlaceholder(placeholderName, app, tg)
			// create the placeholder on K8s
			_, err := mgr.clients.KubeClient.Create(placeholder.pod)
			if err != nil {
				// if failed to create the place holder pod
				// caller should handle this error
				log.Logger().Error("failed to create placeholder pod",
					zap.Error(err))
				return err
			}
			log.Logger().Info("placeholder created",
				zap.String("placeholder", placeholder.String()))
		}
	}

	return nil
}

// recycle all the placeholders for an application
func (mgr *PlaceholderManager) Recycle(appID string) {
	log.Logger().Info("start to recycle app placeholders",
		zap.String("appID", appID))
}

// this is only used in testing
func (mgr *PlaceholderManager) setMockedClients(mockedClients *client.Clients) {
	mgr.Lock()
	defer mgr.Unlock()
	mgr.clients = mockedClients
}
