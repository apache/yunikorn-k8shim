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
	"os"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"

	"go.uber.org/zap"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	corecache "github.com/apache/incubator-yunikorn-core/pkg/cache"
	appv1 "github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/appmgmt/interfaces"
	shimcache "github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	applicationclient "github.com/apache/incubator-yunikorn-k8shim/pkg/client/clientset/versioned"
	appscheme "github.com/apache/incubator-yunikorn-k8shim/pkg/client/clientset/versioned/scheme"
	appinformers "github.com/apache/incubator-yunikorn-k8shim/pkg/client/informers/externalversions"
	applister "github.com/apache/incubator-yunikorn-k8shim/pkg/client/listers/yunikorn.apache.org/v1alpha1"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/events"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

type AppManager struct {
	amProtocol interfaces.ApplicationManagementProtocol
	controller *Controller
}

func NewAppManager(amProtocol interfaces.ApplicationManagementProtocol) *AppManager {
	return &AppManager{
		amProtocol: amProtocol,
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
	appMgr.controller = NewController(appMgr)
	appMgr.controller.Run()
	return nil
}

// this implements AppManagementService interface
func (appMgr *AppManager) Stop() {
	// noop
}

// handle the updates from the scheduler and sync the status change
func (appMgr *AppManager) HandleCallbackEvents() func(obj interface{}) {
	return func(obj interface{}) {
		if event, ok := obj.(events.ApplicationEvent); ok {
			if events.AppStateChange == event.GetEvent() {
				if shimEvent, ok := event.(shimcache.ApplicationStatusChangeEvent); ok {
					appID := event.GetApplicationID()
					log.Logger.Info("Status Change callback received",
						zap.String("app id", appID),
						zap.String("new status", shimEvent.State))
					var app = appMgr.amProtocol.GetApplication(appID).(*shimcache.Application)
					appCRD, err := appMgr.controller.lister.Applications(app.GetTags()[constants.AppTagNamespace]).Get(appID)
					if err != nil {
						log.Logger.Warn("Failed to query app CRD for status update",
							zap.String("Application ID", appID),
							zap.Error(err))
						return
					}
					crdState := convertShimAppStateToAppCRDState(shimEvent.State)
					if crdState != "Undefined" {
						appMgr.updateAppCRDStatus(appCRD, crdState)
					} else {
						log.Logger.Error("Invalid status, skip saving it",
							zap.String("App id", appID))
					}
				}
			}
		}
	}
}

type Controller struct {
	kubeClient       kubernetes.Interface
	extensionsClient apiextensionsclient.Interface
	appClient        applicationclient.Interface
	informer         cache.SharedIndexInformer
	lister           applister.ApplicationLister
}

func NewController(appMgr *AppManager) *Controller {
	kubeconfig := os.Getenv("KUBECONFIG")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Logger.Fatal(err.Error())
	}

	kubeClient := kubernetes.NewForConfigOrDie(config)
	apiextensionsClient := apiextensionsclient.NewForConfigOrDie(config)
	appClient := applicationclient.NewForConfigOrDie(config)

	informerFactory := appinformers.NewSharedInformerFactory(appClient, time.Minute*1)
	informer := informerFactory.Apache().V1alpha1().Applications()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    appMgr.addApp,
		UpdateFunc: appMgr.updateApp,
		DeleteFunc: appMgr.deleteApp,
	})
	informerFactory.Start(wait.NeverStop)

	utilruntime.Must(appv1.AddToScheme(appscheme.Scheme))
	return &Controller{
		kubeClient:       kubeClient,
		extensionsClient: apiextensionsClient,
		appClient:        appClient,
		informer:         informer.Informer(),
		lister:           informer.Lister(),
	}
}

func (c *Controller) Run() {
	//TODO: handle the case when the crd is not registered: register it programatically
	defer utilruntime.HandleCrash()

	log.Logger.Info("Waiting cache to be synced.")
	// Handle timeout for syncing.
	timeout := time.NewTimer(time.Second * 30)
	timeoutCh := make(chan struct{})
	go func() {
		<-timeout.C
		timeoutCh <- struct{}{}
	}()
	if ok := cache.WaitForCacheSync(timeoutCh, c.informer.HasSynced); !ok {
		log.Logger.Error("Timeout expired during waiting for caches to sync. Skip starting the application controller")
	} else {
		log.Logger.Info("Starting application controller.")
	}
}

/*
Update the application in the scheduler
*/
func (appMgr *AppManager) updateApp(oldObj interface{}, newObj interface{}) {
	newApp, ok := newObj.(*appv1.Application)
	if !ok {
		log.Logger.Error("newObj is not an Application")
	}
	oldApp, ok := oldObj.(*appv1.Application)
	if !ok {
		log.Logger.Error("oldObj is not an Application")
	}
	// No need to update if ResourceVersion is not changed
	if newApp.ResourceVersion == oldApp.ResourceVersion {
		log.Logger.Info("No need to update because app object is not modified")
		return
	}

	// handle spec change
	if !equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		//TODO: handle update, discuss what should be updated since only the queue is the common property yet
	}
}

/*
Remove the application from the scheduler as well
*/
func (appMgr *AppManager) deleteApp(obj interface{}) {
	app, ok := obj.(*appv1.Application)
	if !ok {
		log.Logger.Error("obj is not an Application")
	}
	appID := app.Name
	err := appMgr.amProtocol.RemoveApplication(appID)
	if err != nil {
		log.Logger.Error("Application removal failed",
			zap.String("appID", appID),
			zap.Error(err))
	}
	log.Logger.Debug("App CRD deleted",
		zap.String("Name", appID))
	//TODO: delete related pods and deployments
}

/*
Add application to scheduler
*/
func (appMgr *AppManager) addApp(obj interface{}) {
	appCRD, ok := obj.(*appv1.Application)
	if !ok {
		log.Logger.Error("obj is not an Application")
	}
	if appMeta, ok := appMgr.getAppMetadata(appCRD); ok {
		app := appMgr.amProtocol.GetApplication(appMeta.ApplicationID)
		if app == nil {
			appMgr.amProtocol.AddApplication(&interfaces.AddApplicationRequest{
				Metadata: appMeta,
				//TODO: debug recovery
				//TODO: try to define the owner reference for the submitted pods to this CRD and try of we delete the
				//CRD the pods will be deleted as well
				Recovery: false,
			})
			// set and save status = New
			appMgr.updateAppCRDStatus(appCRD, appv1.NewApplicationState)
		}
	}
}

func (appMgr *AppManager) updateAppCRDStatus(appCRD *appv1.Application, status appv1.ApplicationStateType) {
	copy := appCRD.DeepCopy()
	copy.Status = appv1.ApplicationStatus{
		AppStatus:  status,
		Message:    "app CRD status change",
		LastUpdate: v1.NewTime(time.Now()),
	}
	_, err := appMgr.controller.appClient.ApacheV1alpha1().Applications(appCRD.Namespace).UpdateStatus(copy)
	if err != nil {
		log.Logger.Error("Failed to update application CRD",
			zap.String("AppId", copy.Name))
		return
	}
}

func (appMgr *AppManager) getAppMetadata(app *appv1.Application) (interfaces.ApplicationMetadata, bool) {
	appID := app.Name

	// tags will at least have namespace info
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

func convertShimAppStateToAppCRDState(status string) appv1.ApplicationStateType {
	switch status {
	case corecache.New.String():
		return appv1.NewApplicationState
	case corecache.Accepted.String():
		return appv1.AcceptedState
	case corecache.Starting.String():
		return appv1.StartingState
	case corecache.Running.String():
		return appv1.RunningState
	case corecache.Waiting.String():
		return appv1.WaitingState
	case corecache.Rejected.String():
		return appv1.RejectedState
	case corecache.Completed.String():
		return appv1.CompletedState
	case corecache.Killed.String():
		return appv1.KilledState
	default:
		return "Undefined"
	}
}
