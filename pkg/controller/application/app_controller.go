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
	"go.uber.org/zap"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
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
				log.Logger.Info("Status Change callback",
					zap.Any("new state", event.GetArgs()))
				//TODO: check how to get the status from the event
				if shimEvent, ok := event.(shimcache.ApplicationStatusChangeEvent); ok {
					appId := event.GetApplicationID()
					var app = appMgr.amProtocol.GetApplication(appId).(*shimcache.Application)
					appCRD, err := appMgr.controller.lister.Applications(app.GetTags()[constants.AppTagNamespace]).Get(appId)
					if err == nil {
						log.Logger.Error("Failed to query app CRD for status update",
							zap.String("Application ID", appId))
						return
					}
					crdState := convertShimAppStateToAppCRDState(shimEvent.State)
					if crdState != "Undefined" {
						appMgr.updateAppCRDStatus(appCRD, crdState)
					} else {
						log.Logger.Error("Invalid status, skip saving it",
							zap.String("App id", appId))
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
		log.Logger.Fatal("Timeout expired during waiting for caches to sync.")
	}

	log.Logger.Info("Starting custom controller.")
	//select {}
}

/*
Update the application in the scheduler
*/
func (appMgr *AppManager) updateApp(oldObj interface{}, newObj interface{}) {
	//TODO: implement the update
}

/*
Remove the application from the scheduler as well
*/
func (appMgr *AppManager) deleteApp(obj interface{}) {
	app, _ := obj.(*appv1.Application)
	appId := app.Name
	appMgr.amProtocol.RemoveApplication(appId)
	log.Logger.Debug("App CRD deleted",
		zap.String("Name", appId))
	//TODO: check why is not remobed from core
}

/*
Add application to scheduler
*/
func (appMgr *AppManager) addApp(obj interface{}) {
	appCRD, _ := obj.(*appv1.Application)
	if appMeta, ok := appMgr.getAppMetadata(appCRD); ok {
		app := appMgr.amProtocol.GetApplication(appMeta.ApplicationID)
		if app == nil {
			appMgr.amProtocol.AddApplication(&interfaces.AddApplicationRequest{
				Metadata: appMeta,
				//TODO: ask about recovery
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
		AppStatus:  appv1.NewApplicationState,
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
	appId := app.Name

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
		ApplicationID: appId,
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
