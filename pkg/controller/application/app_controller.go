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

package application

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/yunikorn-k8shim/pkg/client"

	"go.uber.org/zap"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appv1 "github.com/apache/yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	shimcache "github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

type AppManager struct {
	amProtocol interfaces.ApplicationManagementProtocol
	//controller *Controller
	apiProvider client.APIProvider
}

const appIDDelimiter = "-"

func NewAppManager(amProtocol interfaces.ApplicationManagementProtocol, apiProvider client.APIProvider) *AppManager {
	return &AppManager{
		amProtocol:  amProtocol,
		apiProvider: apiProvider,
	}
}

// this implements AppManagementService interface
func (appMgr *AppManager) Name() string {
	return constants.AppManagerHandlerName
}

// this implements AppManagementService interface
func (appMgr *AppManager) ServiceInit() error {
	return nil
}

// this implements AppManagementService interface
func (appMgr *AppManager) Start() error {
	appMgr.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.ApplicationInformerHandlers,
		AddFn:    appMgr.addApp,
		DeleteFn: appMgr.deleteApp,
	})
	return nil
}

// this implements AppManagementService interface
func (appMgr *AppManager) Stop() {
	//noop
}

// handle the updates from the scheduler and sync the status change
func (appMgr *AppManager) HandleApplicationStateUpdate() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			if shimcache.AppStateChange.String() == event.GetEvent() {
				if shimEvent, ok := event.(shimcache.ApplicationStatusChangeEvent); ok {
					appID := event.GetApplicationID()
					log.Logger().Info("Status Change callback received",
						zap.String("app id", appID),
						zap.String("new status", shimEvent.GetState()))
					var app = appMgr.amProtocol.GetApplication(appID).(*shimcache.Application)
					appName, err := getNameFromAppID(appID)
					if err != nil {
						log.Logger().Warn("Failed to handle status update",
							zap.String("application ID", appID),
							zap.Error(err))
					}
					appCRD, err := appMgr.apiProvider.GetAPIs().AppInformer.Lister().Applications(app.GetTags()[constants.AppTagNamespace]).Get(appName)
					if err != nil {
						log.Logger().Warn("Failed to query app CRD for status update",
							zap.String("Application ID", appID),
							zap.Error(err))
					}
					crdState := convertShimAppStateToAppCRDState(shimEvent.GetState())
					if crdState != "Undefined" {
						appMgr.updateAppCRDStatus(appCRD, crdState)
					} else {
						log.Logger().Error("Invalid status, skip saving it",
							zap.String("App id", appID))
						//create some error
					}
					log.Logger().Debug("Application status changed",
						zap.String("AppID", appID))
				}
			}
		}
	}
}

/*
Remove the application from the scheduler as well
*/
func (appMgr *AppManager) deleteApp(obj interface{}) {
	app, ok := obj.(*appv1.Application)
	if !ok {
		log.Logger().Error("obj is not an Application")
		return
	}
	appID := constructAppID(app.Name, app.Namespace)
	err := appMgr.amProtocol.RemoveApplication(appID)
	if err != nil {
		log.Logger().Error("Application removal failed",
			zap.String("appID", appID),
			zap.Error(err))
		return
	}
	log.Logger().Debug("App CRD deleted",
		zap.String("Name", appID))
}

/*
Add application to scheduler
*/
func (appMgr *AppManager) addApp(obj interface{}) {
	appCRD, ok := obj.(*appv1.Application)
	if !ok {
		log.Logger().Error("obj is not an Application")
		return
	}
	if appMeta, ok := appMgr.getAppMetadata(appCRD); ok {
		app := appMgr.amProtocol.GetApplication(appMeta.ApplicationID)
		if app == nil {
			appMgr.amProtocol.AddApplication(&interfaces.AddApplicationRequest{
				Metadata: appMeta,
			})
			// set and save status = New in case it is not set. In case of recovery don't overwrite it
			if len(appCRD.Status.AppStatus) == 0 {
				appMgr.updateAppCRDStatus(appCRD, appv1.NewApplicationState)
			}
		}
	}
}

func (appMgr *AppManager) updateAppCRDStatus(appCRD *appv1.Application, status appv1.ApplicationStateType) {
	if appCRD == nil {
		log.Logger().Error("AppCRD is nil, there is nothing to update")
		return
	}
	appCopy := appCRD.DeepCopy()
	appCopy.Status = appv1.ApplicationStatus{
		AppStatus:  status,
		Message:    "app CRD status change",
		LastUpdate: v1.NewTime(time.Now()),
	}
	_, err := appMgr.apiProvider.GetAPIs().AppClient.ApacheV1alpha1().Applications(appCRD.Namespace).UpdateStatus(context.Background(), appCopy, v1.UpdateOptions{})
	if err != nil {
		log.Logger().Error("Failed to update application CRD",
			zap.String("AppId", appCopy.Name))
		return
	}
}

func (appMgr *AppManager) getAppMetadata(app *appv1.Application) (interfaces.ApplicationMetadata, bool) {
	appID := constructAppID(app.Name, app.Namespace)

	// tags will at least have defaultNamespace info
	// labels or annotations from the pod can be added when needed
	// user info is retrieved via service account
	tags := map[string]string{}
	if app.Namespace == "" {
		tags[constants.AppTagNamespace] = constants.DefaultAppNamespace
	} else {
		tags[constants.AppTagNamespace] = app.Namespace
	}

	return interfaces.ApplicationMetadata{
		ApplicationID: appID,
		QueueName:     app.Spec.Queue,
		User:          "default",
		Tags:          tags,
	}, true
}

func constructAppID(name string, namespace string) string {
	return namespace + appIDDelimiter + name
}

func getNameFromAppID(appID string) (string, error) {
	if len(appID) > 0 {
		splitted := strings.Split(appID, appIDDelimiter)
		if len(splitted) < 2 {
			return "", fmt.Errorf("unknown appID format")
		}
		return splitted[1], nil
	}
	return "", fmt.Errorf("appID should not be empty")
}

const undefinedState = "Undefined"

func convertShimAppStateToAppCRDState(status string) appv1.ApplicationStateType {
	switch status {
	case "New":
		return appv1.NewApplicationState
	case "Accepted":
		return appv1.AcceptedState
	case "Starting":
		return appv1.StartingState
	case "Running":
		return appv1.RunningState
	case "Waiting":
		return appv1.WaitingState
	case "Rejected":
		return appv1.RejectedState
	case "Completed":
		return appv1.CompletedState
	case "Killed":
		return appv1.KilledState
	default:
		return undefinedState
	}
}
