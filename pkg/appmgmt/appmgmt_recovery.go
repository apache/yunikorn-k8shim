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

package appmgmt

import (
	"errors"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/general"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// WaitForRecovery initiates and waits for the app management service to finish recovery. If recovery
// is canceled (used by testing code) or an error occurs, an error will be returned. In production, this
// method will block until recovery completes.
func (svc *AppManagementService) WaitForRecovery() error {
	if !svc.apiProvider.IsTestingMode() {
		apps, err := svc.recoverApps()
		if err != nil {
			return err
		}
		if !svc.waitForAppRecovery(apps) {
			return errors.New("recovery aborted")
		}
	}
	return nil
}

func (svc *AppManagementService) recoverApps() (map[string]interfaces.ManagedApp, error) {
	log.Logger().Info("Starting app recovery")
	recoveringApps := make(map[string]interfaces.ManagedApp)
	for _, mgr := range svc.managers {
		if m, ok := mgr.(interfaces.Recoverable); ok {
			pods, err := m.ListPods()
			if err != nil {
				log.Logger().Error("failed to list apps", zap.Error(err))
				return recoveringApps, err
			}

			sort.Slice(pods, func(i, j int) bool {
				return pods[i].CreationTimestamp.Unix() < pods[j].CreationTimestamp.Unix()
			})

			for _, pod := range pods {
				if utils.NeedRecovery(pod) {
					app := svc.podEventHandler.HandleEvent(general.AddPod, general.Recovery, pod)
					recoveringApps[app.GetApplicationID()] = app
				}
			}
			log.Logger().Info("Recovery finished")
			svc.podEventHandler.RecoveryDone()
		}
	}

	return recoveringApps, nil
}

// waitForAppRecovery blocks until either all applications have been processed (returning true)
// or cancelWaitForAppRecovery is called (returning false)
func (svc *AppManagementService) waitForAppRecovery(recoveringApps map[string]interfaces.ManagedApp) bool {
	svc.cancelRecovery.Store(false) // reset cancellation token
	recoveryStartTime := time.Now()
	counter := 0
	for {
		// check for cancellation token
		if svc.cancelRecovery.Load() {
			log.Logger().Info("Waiting for recovery canceled.")
			svc.cancelRecovery.Store(false)
			return false
		}

		svc.removeRecoveredApps(recoveringApps)
		if len(recoveringApps) == 0 {
			log.Logger().Info("Application recovery complete.")
			return true
		}
		counter++
		if counter%10 == 0 {
			log.Logger().Info("Waiting for application recovery",
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
func (svc *AppManagementService) removeRecoveredApps(recoveringApps map[string]interfaces.ManagedApp) {
	for _, app := range recoveringApps {
		state := app.GetApplicationState()
		if state != cache.ApplicationStates().New && state != cache.ApplicationStates().Recovering {
			log.Logger().Info("Recovered application",
				zap.String("appId", app.GetApplicationID()),
				zap.String("state", state))
			delete(recoveringApps, app.GetApplicationID())
		}
	}
}
