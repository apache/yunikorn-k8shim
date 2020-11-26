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
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

// placeholder manager is a service to manage the lifecycle of app placeholders
type PlaceholderManager struct {
	clients   *client.Clients
	orphanPod map[string]*v1.Pod
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

// clean up all the placeholders for an application
func (mgr *PlaceholderManager) CleanUp(app *Application) {
	log.Logger().Info("start to clean up app placeholders",
		zap.String("appID", app.GetApplicationID()))
	for taskID, task := range app.taskMap {
		if task.GetTaskPlaceholder() {
			// remove pod
			err := mgr.clients.KubeClient.Delete(task.pod)
			if err != nil {
				log.Logger().Error("failed to clean up placeholder pod",
					zap.Error(err))
				mgr.orphanPod[taskID] = task.pod
			}
		}
	}
	log.Logger().Info("finish to clean up app placeholders",
		zap.String("appID", app.GetApplicationID()))
}

// this is only used in testing
func (mgr *PlaceholderManager) setMockedClients(mockedClients *client.Clients) {
	mgr.Lock()
	defer mgr.Unlock()
	mgr.clients = mockedClients
}

func (mgr *PlaceholderManager) cleanOrphanPlaceholders() {
	for taskID, pod := range mgr.orphanPod {
		err := mgr.clients.KubeClient.Delete(pod)
		if err == nil {
			delete(mgr.orphanPod, taskID)
		}
	}
}

func (mgr *PlaceholderManager) Start() {
	go func() {
		for {
			mgr.cleanOrphanPlaceholders()
			time.Sleep(5 * time.Second)
		}
	}()
}
