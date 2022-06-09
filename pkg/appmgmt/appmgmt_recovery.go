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
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/general"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

func (svc *AppManagementService) WaitForRecovery(maxTimeout time.Duration) error {
	if !svc.apiProvider.IsTestingMode() {
		apps, err := svc.recoverApps()
		if err != nil {
			return err
		}

		return svc.waitForAppRecovery(apps, maxTimeout)
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
				app := svc.podEventHandler.HandleEvent(general.AddPod, general.Recovery, pod)
				recoveringApps[app.GetApplicationID()] = app
			}
			log.Logger().Info("Recovery finished")
			svc.podEventHandler.RecoveryDone()
		}
	}

	return recoveringApps, nil
}

func (svc *AppManagementService) waitForAppRecovery(
	recoveringApps map[string]interfaces.ManagedApp, maxTimeout time.Duration) error {
	if len(recoveringApps) > 0 {
		log.Logger().Info("wait for app recovery",
			zap.Int("appToRecover", len(recoveringApps)))
		// check app states periodically, ensure all apps exit from recovering state
		if err := utils.WaitForCondition(func() bool {
			for _, app := range recoveringApps {
				log.Logger().Debug("appInfo",
					zap.String("appId", app.GetApplicationID()),
					zap.String("state", app.GetApplicationState()))
				if app.GetApplicationState() == cache.ApplicationStates().Accepted {
					delete(recoveringApps, app.GetApplicationID())
				}
			}

			if len(recoveringApps) == 0 {
				log.Logger().Info("app recovery is successful")
				return true
			}

			return false
		}, 1*time.Second, maxTimeout); err != nil {
			return fmt.Errorf("timeout waiting for app recovery in %s",
				maxTimeout.String())
		}
	}

	return nil
}
