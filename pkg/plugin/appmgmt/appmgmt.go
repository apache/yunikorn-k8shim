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
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"github.com/cloudera/yunikorn-k8shim/pkg/plugin/appmgmt/general"
	"github.com/cloudera/yunikorn-k8shim/pkg/plugin/appmgmt/sparkopt"
	"go.uber.org/zap"
)

// app manager service is a central service that interacts with
// one or more K8s operators for app scheduling.
type AppManagementService struct {
	apiProvider   client.APIProvider
	amProtocol    cache.ApplicationManagementProtocol
	managers      []AppManager
	launchOptions *AMServiceLaunchOptions
}

type AMServiceLaunchOptions struct {
	TestMode bool
}

func NewAMService(amProtocol cache.ApplicationManagementProtocol,
	apiProvider client.APIProvider, launchOptions *AMServiceLaunchOptions) *AppManagementService {
	appManager := &AppManagementService{
		amProtocol:    amProtocol,
		apiProvider:   apiProvider,
		launchOptions: launchOptions,
		managers:      make([]AppManager, 0),
	}

	if !launchOptions.TestMode {
		appManager.register(
			// registered app plugins
			// for general apps
			general.New(amProtocol, apiProvider),
			// for spark operator - SparkApplication
			sparkopt.New(amProtocol, apiProvider))
	}

	return appManager
}

func (svc *AppManagementService) register(managers ...AppManager) {
	for _, mgr := range managers {
		log.Logger.Info("registering app management service",
			zap.String("serviceName", mgr.Name()))
		svc.managers = append(svc.managers, mgr)
	}
}

func (svc *AppManagementService) Start() error {
	for _, optService := range svc.managers {
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
			return err
		}

		log.Logger.Info("service started",
			zap.String("serviceName", optService.Name()))
	}

	return nil
}

func (svc *AppManagementService) Stop() {
	log.Logger.Info("shutting down app management services")
	for _, optService := range svc.managers {
		if err := optService.Stop(); err != nil {
			log.Logger.Error("stop service error",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
		}
	}
}
