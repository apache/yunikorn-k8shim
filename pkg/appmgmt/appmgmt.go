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
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/general"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/sparkoperator"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

// AppManagementService is a central service that interacts with
// one or more K8s operators for app scheduling.
type AppManagementService struct {
	apiProvider     client.APIProvider
	amProtocol      interfaces.ApplicationManagementProtocol
	managers        []interfaces.AppManager
	podEventHandler *general.PodEventHandler
	cancelRecovery  atomic.Bool
}

func NewAMService(amProtocol interfaces.ApplicationManagementProtocol,
	apiProvider client.APIProvider) *AppManagementService {

	podEventHandler := general.NewPodEventHandler(amProtocol, true)

	appManager := &AppManagementService{
		amProtocol:      amProtocol,
		apiProvider:     apiProvider,
		managers:        make([]interfaces.AppManager, 0),
		podEventHandler: podEventHandler,
	}

	log.Log(log.ShimAppMgmt).Info("Initializing new AppMgmt service")
	appManager.register(
		// registered app plugins
		// for general apps
		general.NewManager(apiProvider, podEventHandler),
		// for spark operator - SparkApplication
		sparkoperator.NewManager(amProtocol, apiProvider),
	)

	return appManager
}

func (svc *AppManagementService) GetAllManagers() []interfaces.AppManager {
	return svc.managers
}

func (svc *AppManagementService) GetManagerByName(name string) interfaces.AppManager {
	for _, mgr := range svc.managers {
		if mgr.Name() == name {
			return mgr
		}
	}
	return nil
}

func (svc *AppManagementService) register(managers ...interfaces.AppManager) {
	for _, mgr := range managers {
		if conf.GetSchedulerConf().IsOperatorPluginEnabled(mgr.Name()) {
			log.Log(log.ShimAppMgmt).Info("registering app management service",
				zap.String("serviceName", mgr.Name()))
			svc.managers = append(svc.managers, mgr)
		} else {
			log.Log(log.ShimAppMgmt).Info("skip registering app management service",
				zap.String("serviceName", mgr.Name()))
		}
	}
}

func (svc *AppManagementService) Start() error {
	for _, optService := range svc.managers {
		// init service before starting
		if err := optService.ServiceInit(); err != nil {
			log.Log(log.ShimAppMgmt).Error("service init fails",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
			return err
		}

		log.Log(log.ShimAppMgmt).Info("starting app management service",
			zap.String("serviceName", optService.Name()))
		if err := optService.Start(); err != nil {
			log.Log(log.ShimAppMgmt).Error("failed to start management service",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
			return err
		}

		log.Log(log.ShimAppMgmt).Info("app management service started",
			zap.String("serviceName", optService.Name()))
	}

	return nil
}

func (svc *AppManagementService) Stop() {
	log.Log(log.ShimAppMgmt).Info("shutting down app management services")
	for _, optService := range svc.managers {
		optService.Stop()
	}
}
