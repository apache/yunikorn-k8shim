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
	"context"
	"strings"
	"sync"
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
	orphanPods map[string]*v1.Pod
	stopChan   chan struct{}
	// closed when the cleanup loop exits so Stop can wait for shutdown to complete
	doneChan     chan struct{}
	stopOnce     sync.Once
	started      atomic.Bool
	cancel       context.CancelFunc
	lifecycleCtx context.Context
	cleanupTime  time.Duration
	// serializes placeholder creation with application cleanup
	operationLock locking.Mutex
	// operations hold a read lock; Stop cancels them before waiting for the write lock
	lifecycleGate locking.RWMutex
	// protects orphanPods and cleanupTime
	locking.RWMutex
}

var (
	placeholderMgr *PlaceholderManager
	mu             locking.Mutex
)

func NewPlaceholderManager(clients *client.Clients) *PlaceholderManager {
	mu.Lock()
	defer mu.Unlock()
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	placeholderMgr = &PlaceholderManager{
		clients:      clients,
		orphanPods:   make(map[string]*v1.Pod),
		stopChan:     make(chan struct{}),
		doneChan:     make(chan struct{}),
		lifecycleCtx: lifecycleCtx,
		cancel:       cancel,
		cleanupTime:  5 * time.Second,
	}
	return placeholderMgr
}

func getPlaceholderManager() *PlaceholderManager {
	mu.Lock()
	defer mu.Unlock()
	return placeholderMgr
}

func (mgr *PlaceholderManager) createAppPlaceholders(app *Application) error {
	mgr.lifecycleGate.RLock()
	defer mgr.lifecycleGate.RUnlock()
	if err := mgr.lifecycleCtx.Err(); err != nil {
		return err
	}
	mgr.operationLock.Lock()
	defer mgr.operationLock.Unlock()
	if err := mgr.lifecycleCtx.Err(); err != nil {
		return err
	}

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
			if err := mgr.lifecycleCtx.Err(); err != nil {
				return err
			}
			placeholderName := GeneratePlaceholderName(tg.Name, app.GetApplicationID())
			placeholder := newPlaceholder(placeholderName, app, tg)
			// create the placeholder on K8s
			_, err := mgr.clients.KubeClient.Create(mgr.lifecycleCtx, placeholder.pod)
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
	mgr.lifecycleGate.RLock()
	defer mgr.lifecycleGate.RUnlock()
	if mgr.lifecycleCtx.Err() != nil {
		return
	}
	mgr.operationLock.Lock()
	defer mgr.operationLock.Unlock()
	if mgr.lifecycleCtx.Err() != nil {
		return
	}

	log.Log(log.ShimCachePlaceholder).Info("start to clean up app placeholders",
		zap.String("appID", app.GetApplicationID()))
	for _, task := range app.GetPlaceHolderTasks() {
		if mgr.lifecycleCtx.Err() != nil {
			return
		}
		// remove pod
		err := mgr.clients.KubeClient.Delete(mgr.lifecycleCtx, task.GetTaskPod())
		if err != nil {
			if mgr.lifecycleCtx.Err() != nil {
				return
			}
			log.Log(log.ShimCachePlaceholder).Warn("failed to clean up placeholder pod",
				zap.Error(err))
			if !strings.Contains(err.Error(), "not found") {
				mgr.Lock()
				mgr.orphanPods[task.GetTaskID()] = task.GetTaskPod()
				mgr.Unlock()
			}
		}
	}
	log.Log(log.ShimCachePlaceholder).Info("finished cleaning up app placeholders",
		zap.String("appID", app.GetApplicationID()))
}

func (mgr *PlaceholderManager) cleanUpAsync(app *Application) {
	if !mgr.started.Load() {
		return
	}
	// The operation checks cancellation under the lifecycle gate even if this
	// goroutine is not scheduled until after Stop returns.
	go mgr.cleanUp(app)
}

func (mgr *PlaceholderManager) cleanOrphanPlaceholders() {
	mgr.lifecycleGate.RLock()
	defer mgr.lifecycleGate.RUnlock()
	if mgr.lifecycleCtx.Err() != nil {
		return
	}
	mgr.Lock()
	defer mgr.Unlock()
	for taskID, pod := range mgr.orphanPods {
		if mgr.lifecycleCtx.Err() != nil {
			return
		}
		log.Log(log.ShimCachePlaceholder).Debug("start to clean up orphan pod",
			zap.String("taskID", taskID),
			zap.String("podName", pod.Name))
		err := mgr.clients.KubeClient.Delete(mgr.lifecycleCtx, pod)
		if err != nil {
			if mgr.lifecycleCtx.Err() != nil {
				return
			}
			log.Log(log.ShimCachePlaceholder).Warn("failed to clean up orphan pod", zap.Error(err))
		} else {
			delete(mgr.orphanPods, taskID)
		}
	}
}

func (mgr *PlaceholderManager) Start() {
	if !mgr.started.CompareAndSwap(false, true) {
		log.Log(log.ShimCachePlaceholder).Info("PlaceholderManager is already started")
		return
	}
	log.Log(log.ShimCachePlaceholder).Info("starting the PlaceholderManager")
	go func() {
		ticker := time.NewTicker(mgr.getCleanupTime())
		defer func() {
			ticker.Stop()
			log.Log(log.ShimCachePlaceholder).Info("PlaceholderManager has been stopped")
			// Closing broadcasts completion to every Stop caller; no send is needed.
			close(mgr.doneChan)
		}()
		for {
			select {
			case <-mgr.stopChan:
				return
			case <-ticker.C:
				mgr.cleanOrphanPlaceholders()
			}
		}
	}()
}

func (mgr *PlaceholderManager) Stop() {
	if !mgr.started.Load() {
		log.Log(log.ShimCachePlaceholder).Info("PlaceholderManager already stopped")
		return
	}
	mgr.stopOnce.Do(func() {
		log.Log(log.ShimCachePlaceholder).Info("stopping the PlaceholderManager")
		mgr.cancel()
		close(mgr.stopChan)
	})
	// Cancel before waiting: active API requests must be able to release readers.
	mgr.lifecycleGate.Lock()
	mgr.lifecycleGate.Unlock() //nolint:staticcheck // The empty critical section is a barrier waiting for active operations.
	// Do not hold the write lock here: the loop may be waiting for a read lock.
	<-mgr.doneChan
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
