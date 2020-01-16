/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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

	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/utils"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
)

func  (svc *AppManagementService) WaitForRecovery(maxTimeout time.Duration) error {
	// get all existing applications
	if !svc.apiProvider.IsTestingMode() {
		recoveringApps := make(map[string]*cache.Application)
		for _, mgr := range svc.managers {
			appMetas, err := mgr.ListApplications()
			if err != nil {
				log.Logger.Error("failed to list apps", zap.Error(err))
				return err
			}

			// trigger recovery of the apps
			// this is simply submit the app again
			for _, appMeta := range appMetas {
				if app, recovering := svc.amProtocol.AddApplication(
					&cache.AddApplicationRequest{
						Metadata: appMeta,
						Recovery: true,
				}); recovering {
					recoveringApps[app.GetApplicationID()] = app
				}
			}
		}

		// wait for app recovery
		if len(recoveringApps) > 0 {
			// check app states periodically, ensure all apps exit from recovering state
			if err := utils.WaitForCondition(func() bool {
				for _, app := range recoveringApps {
					log.Logger.Info("appInfo",
						zap.String("appId", app.GetApplicationID()),
						zap.String("state", app.GetApplicationState()))
					if app.GetApplicationState() == events.States().Application.Accepted {
						delete(recoveringApps, app.GetApplicationID())
					}
				}

				if len(recoveringApps) == 0 {
					log.Logger.Info("app recovery is successful")
					return true
				}

				return false
			}, 1 * time.Second, maxTimeout); err != nil{
				return fmt.Errorf("timeout waiting for app recovery in %s",
					maxTimeout.String())
			}
		}
	}
	return nil
}