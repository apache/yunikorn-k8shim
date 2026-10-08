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
	// closed when the cleanup loop exits so Stop can wait for shutdown to complete
	doneChan     chan struct{}
	stopped      atomic.Bool
	started      atomic.Bool
	cancel       context.CancelFunc
	lifecycleCtx context.Context
	cleanupTime  time.Duration
	// tracks operations admitted under the manager lock before cancellation
	operations sync.WaitGroup
	// protects orphanPods, lifecycle state and cleanupTime
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
	ctx, kubeClient := mgr.beginOperation()
	if kubeClient == nil {
		return ctx.Err()
	}
	defer mgr.operations.Done()

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
			if err := ctx.Err(); err != nil {
				return err
			}
			placeholderName := GeneratePlaceholderName(tg.Name, app.GetApplicationID())
			placeholder := newPlaceholder(placeholderName, app, tg)
			// create the placeholder on K8s
			_, err := kubeClient.Create(ctx, placeholder.pod)
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
	ctx, kubeClient := mgr.beginOperation()
	if kubeClient == nil {
		return
	}
	defer mgr.operations.Done()
	orphans := make(map[string]*v1.Pod)
	defer mgr.addOrphanPods(orphans)

	log.Log(log.ShimCachePlaceholder).Info("start to clean up app placeholders",
		zap.String("appID", app.GetApplicationID()))
	for _, task := range app.GetPlaceHolderTasks() {
		if ctx.Err() != nil {
			return
		}
		// remove pod
		err := kubeClient.Delete(ctx, task.GetTaskPod())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Log(log.ShimCachePlaceholder).Warn("failed to clean up placeholder pod",
				zap.Error(err))
			if !strings.Contains(err.Error(), "not found") {
				orphans[task.GetTaskID()] = task.GetTaskPod()
			}
		}
	}
	log.Log(log.ShimCachePlaceholder).Info("finished cleaning up app placeholders",
		zap.String("appID", app.GetApplicationID()))
}

func (mgr *PlaceholderManager) cleanUpAsync(app *Application) {
	if !mgr.started.Load() || mgr.stopped.Load() {
		return
	}
	// The operation checks cancellation even if scheduled after Stop returns.
	go mgr.cleanUp(app)
}

// beginOperation snapshots the shared context and client and registers work before
// Stop can cancel the context and wait. Canceled operations are not registered.
func (mgr *PlaceholderManager) beginOperation() (context.Context, client.KubeClient) {
	mgr.RLock()
	defer mgr.RUnlock()
	ctx := mgr.lifecycleCtx
	if ctx.Err() != nil {
		return ctx, nil
	}
	mgr.operations.Add(1)
	return ctx, mgr.clients.KubeClient
}

func (mgr *PlaceholderManager) addOrphanPods(pods map[string]*v1.Pod) {
	if len(pods) == 0 {
		return
	}
	mgr.Lock()
	defer mgr.Unlock()
	for taskID, pod := range pods {
		mgr.orphanPods[taskID] = pod
	}
}

func (mgr *PlaceholderManager) cleanOrphanPlaceholders() {
	mgr.Lock()
	ctx, kubeClient := mgr.lifecycleCtx, mgr.clients.KubeClient
	if ctx.Err() != nil {
		mgr.Unlock()
		return
	}
	mgr.operations.Add(1)
	orphans := mgr.orphanPods
	mgr.orphanPods = make(map[string]*v1.Pod)
	mgr.Unlock()
	defer mgr.operations.Done()
	defer mgr.addOrphanPods(orphans)

	for taskID, pod := range orphans {
		if ctx.Err() != nil {
			return
		}
		log.Log(log.ShimCachePlaceholder).Debug("start to clean up orphan pod",
			zap.String("taskID", taskID),
			zap.String("podName", pod.Name))
		if err := kubeClient.Delete(ctx, pod); err != nil {
			log.Log(log.ShimCachePlaceholder).Warn("failed to clean up orphan pod", zap.Error(err))
		} else {
			delete(orphans, taskID)
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
			case <-mgr.lifecycleCtx.Done():
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
	if mgr.stopped.CompareAndSwap(false, true) {
		log.Log(log.ShimCachePlaceholder).Info("stopping the PlaceholderManager")
		mgr.Lock()
		mgr.cancel()
		mgr.Unlock()
	}
	<-mgr.lifecycleCtx.Done()
	// Cancellation prevents further registration before waiting for active work.
	mgr.operations.Wait()
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
