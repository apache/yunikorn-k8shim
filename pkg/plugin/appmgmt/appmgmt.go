/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-k8shim/pkg/plugin/appmgmt/general"
	"github.com/cloudera/yunikorn-k8shim/pkg/plugin/appmgmt/sparkoperator"
	"go.uber.org/zap"
)

// scheduler operator service is a central service that interacts with
// one or more K8s operators for app scheduling.
type SchedulerAppManager struct {
	// CRD informer
	// crdInformer cache.SharedIndexInformer
	apiProvider  client.APIProvider
	amProtocol   cache.ApplicationManagementProtocol
	amService    []AppManagementService
	skipRecovery bool
}

func NewAppManager(amProtocol cache.ApplicationManagementProtocol, apiProvider client.APIProvider) *SchedulerAppManager {
	appManager := &SchedulerAppManager{
		amProtocol: amProtocol,
		apiProvider: apiProvider,
		amService:   make([]AppManagementService, 0),
	}

	appManager.Register(
		// registered app plugins
		general.New(amProtocol, apiProvider),
		sparkoperator.New(amProtocol, apiProvider))
	return appManager
}

func (svc *SchedulerAppManager) Register(amServices ...AppManagementService) {
	for _, amService := range amServices {
		log.Logger.Info("registering app management service",
			zap.String("serviceName", amService.Name()))
		svc.amService = append(svc.amService, amService)
	}
}

func (svc *SchedulerAppManager) StartAll() error {
	for _, optService := range svc.amService {
		// init service before starting
		if err := optService.ServiceInit(); err != nil {
			return err
		}

		log.Logger.Info("starting app management service",
			zap.String("serviceName", optService.Name()))
		if err := optService.Start(); err != nil {
			log.Logger.Error("failed to start management service",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
			// shutdown all
			svc.StopAll()
			return err
		}
		log.Logger.Info("service started",
			zap.String("serviceName", optService.Name()))
	}

	return nil
}

func (svc *SchedulerAppManager) StopAll() {
	log.Logger.Info("shutting down all app management services")
	for _, optService := range svc.amService {
		if err := optService.Stop(); err != nil {
			log.Logger.Error("stop service error",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
		}
	}
}
