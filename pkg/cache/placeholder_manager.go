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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

// PlaceholderManager is a service to manage the lifecycle of app placeholders
type PlaceholderManager struct {
	// clients can neve be nil, even the kubeclient cannot be nil as the shim will not start without it
	clients *client.Clients
	// when the placeholder manager is unable to delete a pod,
	// this pod becomes to be an "orphan" pod. We add them to a map
	// and keep retrying deleting them in order to avoid wasting resources.
	orphanPods map[string]*v1.Pod
	stopChan   chan struct{}
	running    atomic.Value
	// a simple mutex will do we do not have separate read and write paths
	sync.Mutex
}

var placeholderMgr *PlaceholderManager
var placeholderManagerLock sync.Mutex

func NewPlaceholderManager(clients *client.Clients) *PlaceholderManager {
	var r atomic.Value
	r.Store(false)
	placeholderManagerLock.Lock()
	defer placeholderManagerLock.Unlock()
	placeholderMgr = &PlaceholderManager{
		clients:    clients,
		running:    r,
		orphanPods: make(map[string]*v1.Pod),
		stopChan:   make(chan struct{}),
	}
	return placeholderMgr
}

func getPlaceholderManager() *PlaceholderManager {
	placeholderManagerLock.Lock()
	defer placeholderManagerLock.Unlock()
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
func (mgr *PlaceholderManager) cleanUp(app *Application) {
	mgr.Lock()
	defer mgr.Unlock()
	log.Logger().Info("start to clean up app placeholders",
		zap.String("appID", app.GetApplicationID()))
	for taskID, task := range app.taskMap {
		if task.IsPlaceholder() {
			// remove pod
			err := mgr.clients.KubeClient.Delete(task.pod)
			if err != nil {
				log.Logger().Warn("failed to clean up placeholder pod",
					zap.Error(err))
				if !strings.Contains(err.Error(), "not found") {
					mgr.orphanPods[taskID] = task.pod
				}
			}
		}
	}
	log.Logger().Info("finished cleaning up app placeholders",
		zap.String("appID", app.GetApplicationID()))
}

func (mgr *PlaceholderManager) cleanOrphanPlaceholders() {
	mgr.Lock()
	defer mgr.Unlock()
	for taskID, pod := range mgr.orphanPods {
		log.Logger().Debug("start to clean up orphan pod",
			zap.String("taskID", taskID),
			zap.String("podName", pod.Name))
		err := mgr.clients.KubeClient.Delete(pod)
		if err != nil {
			log.Logger().Warn("failed to clean up orphan pod", zap.Error(err))
		} else {
			delete(mgr.orphanPods, taskID)
		}
	}
}

func (mgr *PlaceholderManager) Start() {
	if mgr.isRunning() {
		log.Logger().Info("PlaceholderManager is already started")
		return
	}
	log.Logger().Info("starting the PlaceholderManager")
	mgr.setRunning(true)
	go func() {
		// clean orphan placeholders approximately every 5 seconds, check for stop every 100 milliseconds
		for {
			mgr.cleanOrphanPlaceholders()
			for i := 0; i < 50; i++ {
				select {
				case <-mgr.stopChan:
					mgr.setRunning(false)
					log.Logger().Info("PlaceholderManager has been stopped")
					return
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()
}

func (mgr *PlaceholderManager) Stop() {
	if !mgr.isRunning() {
		log.Logger().Info("PlaceholderManager already stopped")
		return
	}
	log.Logger().Info("stopping the PlaceholderManager")
	mgr.stopChan <- struct{}{}
}

func (mgr *PlaceholderManager) isRunning() bool {
	return mgr.running.Load().(bool)
}

func (mgr *PlaceholderManager) setRunning(flag bool) {
	mgr.running.Store(flag)
}
