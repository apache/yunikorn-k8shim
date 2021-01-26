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
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

// placeholder manager is a service to manage the lifecycle of app placeholders
type PlaceholderManager struct {
	clients *client.Clients
	// when the placeholder manager is unable to delete a pod,
	// this pod becomes to be an "orphan" pod. We add them to a map
	// and keep retrying deleting them in order to avoid wasting resources.
	orphanPod map[string]*v1.Pod
	stopChan  chan struct{}
	running   atomic.Value
	sync.RWMutex
}

var placeholderMgr *PlaceholderManager
var once sync.Once

func NewPlaceholderManager(clients *client.Clients) *PlaceholderManager {
	var r atomic.Value
	r.Store(false)
	placeholderMgr = &PlaceholderManager{
		clients: clients,
		running: r,
	}
	return placeholderMgr
}

func GetPlaceholderManager() *PlaceholderManager {
	once.Do(func() {
		if placeholderMgr == nil {
			log.Logger().Fatal("PlaceholderManager is not initiated")
		}
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
	mgr.Lock()
	defer mgr.Unlock()
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

func (mgr *PlaceholderManager) cleanOrphanPlaceholders() {
	mgr.Lock()
	defer mgr.Unlock()
	for taskID, pod := range mgr.orphanPod {
		log.Logger().Debug("start to clean up orphan pod",
			zap.String("taskID", taskID),
			zap.String("podName", pod.Name))
		err := mgr.clients.KubeClient.Delete(pod)
		if err != nil {
			log.Logger().Warn("failed to clean up orphan pod", zap.Error(err))
		} else {
			delete(mgr.orphanPod, taskID)
		}
	}
}

func (mgr *PlaceholderManager) Start() {
	if mgr.isRunning() {
		log.Logger().Info("The placeholder manager has been started")
		return
	}
	log.Logger().Info("starting the Placeholder Manager")
	mgr.stopChan = make(chan struct{})
	mgr.setRunning(true)
	go func() {
		for {
			select {
			case <-mgr.stopChan:
				log.Logger().Info("PlaceholderManager has been stopped")
				mgr.setRunning(false)
				return
			default:
				// clean orphan placeholders every 5 seconds
				log.Logger().Info("clean up orphan pod")
				mgr.cleanOrphanPlaceholders()
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func (mgr *PlaceholderManager) Stop() {
	if !mgr.isRunning() {
		log.Logger().Info("The placeholder manager has been stopped")
		return
	}
	log.Logger().Info("stopping the Placeholder Manager")
	mgr.stopChan <- struct{}{}
	time.Sleep(3 * time.Second)
}

func (mgr *PlaceholderManager) isRunning() bool {
	return mgr.running.Load().(bool)
}

func (mgr *PlaceholderManager) setRunning(flag bool) {
	mgr.running.Store(flag)
}
