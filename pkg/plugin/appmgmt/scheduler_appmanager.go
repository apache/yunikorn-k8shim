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
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
)

// scheduler operator service is a central service that interacts with
// one or more K8s operators for app scheduling.
type SchedulerAppManager struct {
	// CRD informer
	// crdInformer cache.SharedIndexInformer
	sharedContext    *cache.SharedContext
	amProtocol       *cache.ApplicationManagementProtocol
	operatorServices []AppManagementService
	skipRecovery     bool
}

func NewAppManager(sharedContext *cache.SharedContext) *SchedulerAppManager {
	appManager := &SchedulerAppManager{
		sharedContext:    sharedContext,
		operatorServices: make([]AppManagementService, 0),
	}

	return appManager
}

func (svc *SchedulerAppManager) Register(service AppManagementService) {
	log.Logger.Info("registering app management service", zap.String("serviceName", service.Name()))
	svc.operatorServices = append(svc.operatorServices, service)
}

// returns recovering app
func (svc *SchedulerAppManager) RecoverApplication(pod *v1.Pod) (app *cache.Application, recovering bool) {
	for _, optService := range svc.operatorServices {
		return optService.RecoverApplication(pod)
	}
	return nil, false
}

func (svc *SchedulerAppManager) StartAll() error {
	for _, optService := range svc.operatorServices {
		if err := optService.ServiceInit(); err != nil {
			return err
		}

		log.Logger.Info("starting app operator service",
			zap.String("serviceName", optService.Name()))
		if err := optService.Start(); err != nil {
			log.Logger.Error("failed to start operator service",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
			// shutdown all
			svc.StopAll()
			return err
		}
		log.Logger.Info("service started",
			zap.String("serviceName", optService.Name()))
	}
}

func (svc *SchedulerAppManager) StopAll() {
	log.Logger.Info("shutting down all app operator services")
	for _, optService := range svc.operatorServices {
		if err := optService.Stop(); err != nil {
			log.Logger.Error("stop service error",
				zap.String("serviceName", optService.Name()),
				zap.Error(err))
		}
	}
}
