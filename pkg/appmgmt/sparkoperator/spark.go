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

package sparkoperator

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"go.uber.org/zap"
	k8sCache "k8s.io/client-go/tools/cache"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/interfaces"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	crcClientSet "github.com/apache/yunikorn-k8shim/pkg/sparkclient/clientset/versioned"
	crInformers "github.com/apache/yunikorn-k8shim/pkg/sparkclient/informers/externalversions"
)

// Manager implements interfaces#Recoverable, interfaces#AppManager
type Manager struct {
	amProtocol         interfaces.ApplicationManagementProtocol
	apiProvider        client.APIProvider
	crdInformer        k8sCache.SharedIndexInformer
	crdInformerFactory crInformers.SharedInformerFactory
	stopCh             chan struct{}
}

func NewManager(amProtocol interfaces.ApplicationManagementProtocol, apiProvider client.APIProvider) *Manager {
	return &Manager{
		amProtocol:  amProtocol,
		apiProvider: apiProvider,
		stopCh:      make(chan struct{}),
	}
}

// ServiceInit implements AppManagementService interface
/*
It watches for changes to the SparkApplications CRD objects
Two event handlers are defined to react accordingly when an SparkApplication is updated or deleted.
Note that there's no need for an event handler for AddFunc because when a SparkApplication object
is first created, the application ID has not been generated yet. It will only be available after the driver
pod starts and then the Spark K8s backend will assign a string that starts with "spark-" as the app ID
*/
func (os *Manager) ServiceInit() error {
	crClient, err := crcClientSet.NewForConfig(
		os.apiProvider.GetAPIs().KubeClient.GetConfigs())
	if err != nil {
		return err
	}

	var factoryOpts []crInformers.SharedInformerOption
	os.crdInformerFactory = crInformers.NewSharedInformerFactoryWithOptions(
		crClient, 0, factoryOpts...)
	os.crdInformerFactory.Sparkoperator().V1beta2().SparkApplications().Informer()
	os.crdInformer = os.crdInformerFactory.Sparkoperator().V1beta2().SparkApplications().Informer()
	os.crdInformer.AddEventHandler(k8sCache.ResourceEventHandlerFuncs{
		UpdateFunc: os.updateApplication,
		DeleteFunc: os.deleteApplication,
	})
	log.Logger().Info("Spark operator AppMgmt service initialized")

	return nil
}

func (os *Manager) Name() string {
	return "spark-k8s-operator"
}

func (os *Manager) Start() error {
	if os.crdInformerFactory != nil {
		log.Logger().Info("starting", zap.String("Name", os.Name()))
		go os.crdInformerFactory.Start(os.stopCh)
	}
	return nil
}

func (os *Manager) Stop() {
	log.Logger().Info("stopping", zap.String("Name", os.Name()))
	os.stopCh <- struct{}{}
}

/*
When a SparkApplication's state is updated and the new state is one of
FailedState or CompletedState, send the ApplicationFail and ApplicationComplete
message, respectively, through the app mgmt protocol
*/
func (os *Manager) updateApplication(old, new interface{}) {
	appOld := old.(*v1beta2.SparkApplication)
	appNew := new.(*v1beta2.SparkApplication)
	currState := appNew.Status.AppState.State
	log.Logger().Debug("spark app updated",
		zap.Any("old", appOld),
		zap.Any("new", appNew),
		zap.Any("new state", string(currState)))
	if currState == v1beta2.FailedState {
		log.Logger().Debug("SparkApp has failed. Ready to initiate app cleanup")
		os.amProtocol.NotifyApplicationFail(appNew.Status.SparkApplicationID)
	} else if currState == v1beta2.CompletedState {
		log.Logger().Debug("SparkApp has completed. Ready to initiate app cleanup")
		os.amProtocol.NotifyApplicationComplete(appNew.Status.SparkApplicationID)
	}
}

/*
When a request to delete a SparkApplicaiton is detected,
send an ApplicationComplete message through the app mgmt protocol
*/
func (os *Manager) deleteApplication(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	log.Logger().Info("spark app deleted", zap.Any("SparkApplication", app))
	os.amProtocol.NotifyApplicationComplete(app.Status.SparkApplicationID)
}
