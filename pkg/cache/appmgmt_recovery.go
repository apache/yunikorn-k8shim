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
	"errors"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// WaitForRecovery initiates and waits for the app management service to finish recovery. If recovery
// is canceled (used by testing code) or an error occurs, an error will be returned. In production, this
// method will block until recovery completes.
func (svc *AppManagementService) WaitForRecovery() error {
	apps, err := svc.recoverApps()
	if err != nil {
		return err
	}
	if !svc.waitForAppRecovery(apps) {
		return errors.New("recovery aborted")
	}
	return nil
}

func (svc *AppManagementService) recoverApps() (map[string]*Application, error) {
	log.Log(log.ShimAppMgmt).Info("Starting app recovery")
	recoveringApps := make(map[string]*Application)
	pods, err := svc.ListPods()
	if err != nil {
		log.Log(log.ShimAppMgmt).Error("failed to list apps", zap.Error(err))
		return recoveringApps, err
	}

	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.Unix() < pods[j].CreationTimestamp.Unix()
	})

	// Track terminated pods that we have already seen in order to
	// skip redundant handling of async events in RecoveryDone
	// This filter is used for terminated pods to remain consistent
	// with pod filters in the informer
	terminatedYkPods := make(map[string]bool)
	for _, pod := range pods {
		if utils.GetApplicationIDFromPod(pod) != "" {
			if !utils.IsPodTerminated(pod) {
				app := svc.podEventHandler.HandleEvent(AddPod, Recovery, pod)
				recoveringApps[app.GetApplicationID()] = app
				continue
			}
			terminatedYkPods[string(pod.UID)] = true
		}
	}
	log.Log(log.ShimAppMgmt).Info("Recovery finished")
	svc.podEventHandler.RecoveryDone(terminatedYkPods)

	return recoveringApps, nil
}

// waitForAppRecovery blocks until either all applications have been processed (returning true)
// or cancelWaitForAppRecovery is called (returning false)
func (svc *AppManagementService) waitForAppRecovery(recoveringApps map[string]*Application) bool {
	svc.cancelRecovery.Store(false) // reset cancellation token
	recoveryStartTime := time.Now()
	counter := 0
	for {
		// check for cancellation token
		if svc.cancelRecovery.Load() {
			log.Log(log.ShimAppMgmt).Info("Waiting for recovery canceled.")
			svc.cancelRecovery.Store(false)
			return false
		}

		svc.removeRecoveredApps(recoveringApps)
		if len(recoveringApps) == 0 {
			log.Log(log.ShimAppMgmt).Info("Application recovery complete.")
			return true
		}
		counter++
		if counter%10 == 0 {
			log.Log(log.ShimAppMgmt).Info("Waiting for application recovery",
				zap.Duration("timeElapsed", time.Since(recoveryStartTime).Round(time.Second)),
				zap.Int("appsRemaining", len(recoveringApps)))
		}
		time.Sleep(1 * time.Second)
	}
}

// cancelWaitForAppRecovery is used by testing code to ensure that waitForAppRecovery does not block forever
func (svc *AppManagementService) cancelWaitForAppRecovery() {
	svc.cancelRecovery.Store(true)
}

// removeRecoveredApps is used to walk the currently recovering apps list and remove those that have finished recovering
func (svc *AppManagementService) removeRecoveredApps(recoveringApps map[string]*Application) {
	for _, app := range recoveringApps {
		state := app.GetApplicationState()
		if state != ApplicationStates().New && state != ApplicationStates().Recovering {
			log.Log(log.ShimAppMgmt).Info("Recovered application",
				zap.String("appId", app.GetApplicationID()),
				zap.String("state", state))
			delete(recoveringApps, app.GetApplicationID())
		}
	}
}
