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
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
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
			appMetas, pods, err := m.ListApplications()
			if err != nil {
				log.Logger().Error("failed to list apps", zap.Error(err))
				return recoveringApps, err
			}

			// Collect the ManagedApp objects and also call AddApplication()
			// to register the app
			log.Logger().Debug("Adding retrieved applications")
			apps := make([]interfaces.ManagedApp, 0)
			for _, appMeta := range appMetas {
				log.Logger().Debug("Adding application", zap.String("appId", appMeta.ApplicationID))
				if app := svc.amProtocol.AddApplication(
					&interfaces.AddApplicationRequest{
						Metadata:    appMeta,
						ForceUpdate: true,
					}); app != nil {

					recoveringApps[app.GetApplicationID()] = app
					apps = append(apps, app)
				}
			}

			// Collect the tasks and call AddTask()
			log.Logger().Debug("Adding tasks from retrieved pods")
			for _, pod := range pods {
				if taskMeta, ok := svc.generalMgr.GetTaskMetadata(pod); ok {
					if app := svc.amProtocol.GetApplication(taskMeta.ApplicationID); app != nil {
						if _, taskErr := app.GetTask(string(pod.UID)); taskErr != nil {
							log.Logger().Debug("Adding task", zap.Any("taskMeta", taskMeta.TaskID))
							svc.amProtocol.AddTask(&interfaces.AddTaskRequest{
								Metadata: taskMeta,
							})
						}
					}
				}
			}

			// trigger recovery of the apps
			// this is simply submit the app again
			for _, app := range apps {
				log.Logger().Info("Triggering recovery for app", zap.String("appId", app.GetApplicationID()))
				if err := app.TriggerAppRecovery(); err != nil {
					log.Logger().Error("failed to recover app", zap.Error(err))
					return recoveringApps, fmt.Errorf("failed to recover app %s, reason: %v",
						app.GetApplicationID(), err)
				}
			}
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
				if app.GetApplicationState() == events.States().Application.Accepted {
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
