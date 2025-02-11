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
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// PlaceholderManager is a service to manage the lifecycle of app placeholders
type PlaceholderManager struct {
	// clients can neve be nil, even the kubeclient cannot be nil as the shim will not start without it
	clients *client.Clients
	// when the placeholder manager is unable to delete a pod,
	// this pod becomes to be an "orphan" pod. We add them to a map
	// and keep retrying deleting them in order to avoid wasting resources.
	orphanPods  map[string]*v1.Pod
	stopChan    chan struct{}
	running     atomic.Bool
	cleanupTime time.Duration
	// a simple mutex will do we do not have separate read and write paths
	locking.RWMutex
}

var (
	placeholderMgr *PlaceholderManager
	mu             locking.Mutex
)

func NewPlaceholderManager(clients *client.Clients) *PlaceholderManager {
	mu.Lock()
	defer mu.Unlock()
	placeholderMgr = &PlaceholderManager{
		clients:     clients,
		orphanPods:  make(map[string]*v1.Pod),
		stopChan:    make(chan struct{}),
		cleanupTime: 5 * time.Second,
	}
	return placeholderMgr
}

func getPlaceholderManager() *PlaceholderManager {
	mu.Lock()
	defer mu.Unlock()
	return placeholderMgr
}

func (mgr *PlaceholderManager) createAppPlaceholders(app *Application) error {
	mgr.Lock()
	defer mgr.Unlock()

	// map task group to count of already created placeholders
	tgCounts := make(map[string]int32)
	for _, ph := range app.getPlaceHolderTasks() {
		tgCounts[ph.GetTaskGroupName()]++
	}

	// iterate all task groups, create placeholders for all the min members
	for _, tg := range app.getTaskGroups() {
		count := tgCounts[tg.Name]
		// only create missing pods for each task group
		for i := count; i < tg.MinMember; i++ {
			placeholderName := GeneratePlaceholderName(tg.Name, app.GetApplicationID())
			placeholder := newPlaceholder(placeholderName, app, tg)
			// create the placeholder on K8s
			_, err := mgr.clients.KubeClient.Create(placeholder.pod)
			if err != nil {
				log.Log(log.ShimCachePlaceholder).Error("failed to create placeholder pod",
					zap.Error(err))
				return err
			}
			log.Log(log.ShimCachePlaceholder).Info("placeholder created",
				zap.Stringer("placeholder", placeholder))
		}
	}

	return nil
}

// clean up all the placeholders for an application
func (mgr *PlaceholderManager) cleanUp(app *Application) {
	mgr.Lock()
	defer mgr.Unlock()
	log.Log(log.ShimCachePlaceholder).Info("start to clean up app placeholders",
		zap.String("appID", app.GetApplicationID()))
	for _, task := range app.GetPlaceHolderTasks() {
		// remove pod
		err := mgr.clients.KubeClient.Delete(task.GetTaskPod())
		if err != nil {
			log.Log(log.ShimCachePlaceholder).Warn("failed to clean up placeholder pod",
				zap.Error(err))
			if !strings.Contains(err.Error(), "not found") {
				mgr.orphanPods[task.GetTaskID()] = task.GetTaskPod()
			}
		}
	}
	log.Log(log.ShimCachePlaceholder).Info("finished cleaning up app placeholders",
		zap.String("appID", app.GetApplicationID()))
}

func (mgr *PlaceholderManager) cleanOrphanPlaceholders() {
	mgr.Lock()
	defer mgr.Unlock()
	for taskID, pod := range mgr.orphanPods {
		log.Log(log.ShimCachePlaceholder).Debug("start to clean up orphan pod",
			zap.String("taskID", taskID),
			zap.String("podName", pod.Name))
		err := mgr.clients.KubeClient.Delete(pod)
		if err != nil {
			log.Log(log.ShimCachePlaceholder).Warn("failed to clean up orphan pod", zap.Error(err))
		} else {
			delete(mgr.orphanPods, taskID)
		}
	}
}

func (mgr *PlaceholderManager) Start() {
	if mgr.isRunning() {
		log.Log(log.ShimCachePlaceholder).Info("PlaceholderManager is already started")
		return
	}
	log.Log(log.ShimCachePlaceholder).Info("starting the PlaceholderManager")
	mgr.setRunning(true)
	go func() {
		// clean orphan placeholders approximately every 5 seconds
		for {
			select {
			case <-mgr.stopChan:
				mgr.setRunning(false)
				log.Log(log.ShimCachePlaceholder).Info("PlaceholderManager has been stopped")
				return
			case <-time.After(mgr.getCleanupTime()):
				mgr.cleanOrphanPlaceholders()
			}
		}
	}()
}

func (mgr *PlaceholderManager) Stop() {
	if !mgr.isRunning() {
		log.Log(log.ShimCachePlaceholder).Info("PlaceholderManager already stopped")
		return
	}
	log.Log(log.ShimCachePlaceholder).Info("stopping the PlaceholderManager")
	mgr.stopChan <- struct{}{}
}

func (mgr *PlaceholderManager) isRunning() bool {
	return mgr.running.Load()
}

func (mgr *PlaceholderManager) setRunning(flag bool) {
	mgr.running.Store(flag)
}

func (mgr *PlaceholderManager) getOrphanPodsLength() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return len(mgr.orphanPods)
}

func (mgr *PlaceholderManager) setCleanupTime(value time.Duration) {
	mgr.Lock()
	defer mgr.Unlock()
	mgr.cleanupTime = value
}

func (mgr *PlaceholderManager) getCleanupTime() time.Duration {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.cleanupTime
}
