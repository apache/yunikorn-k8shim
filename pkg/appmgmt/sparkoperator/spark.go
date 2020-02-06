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
	crcClientSet "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crInformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/cache"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/client"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sCache "k8s.io/client-go/tools/cache"
)

const (
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
)

type Manager struct {
	amProtocol         cache.ApplicationManagementProtocol
	apiProvider        client.APIProvider
	crdInformer        k8sCache.SharedIndexInformer
	crdInformerFactory crInformers.SharedInformerFactory
	stopCh             chan struct{}
}

func New(amProtocol cache.ApplicationManagementProtocol, apiProvider client.APIProvider) *Manager {
	return &Manager{
		amProtocol:  amProtocol,
		apiProvider: apiProvider,
		stopCh:      make(chan struct{}),
	}
}

// this implements AppManagementService interface
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
		AddFunc:    os.addApplication,
		UpdateFunc: os.updateApplication,
		DeleteFunc: os.deleteApplication,
	})

	os.apiProvider.AddEventHandler(&client.ResourceEventHandlers{
		Type:     client.PodInformerHandlers,
		FilterFn: os.filterPod,
		AddFn:    os.addPod,
		UpdateFn: os.updatePod,
		DeleteFn: os.deletePod,
	})

	return nil
}

func (os *Manager) Name() string {
	return "spark-operator-service"
}

func (os *Manager) Start() error {
	log.Logger.Info("starting", zap.String("Name", os.Name()))
	go os.crdInformerFactory.Start(os.stopCh)
	return nil
}

func (os *Manager) Stop() error {
	log.Logger.Info("stopping", zap.String("Name", os.Name()))
	os.stopCh<- struct{}{}
	return nil
}

func (os *Manager) getTaskMetadata(pod *v1.Pod) (cache.TaskMetadata, bool) {
	// spark executors are having a common label
	if appName, ok := pod.Labels[SparkAppNameLabel]; ok {
		return cache.TaskMetadata{
			ApplicationID: appName,
			TaskID:        string(pod.UID),
			Pod:           pod,
		}, true
	}
	return cache.TaskMetadata{}, false
}

func (os *Manager) getAppMetadata(sparkApp *v1beta2.SparkApplication) cache.ApplicationMetadata {
	// extract tags from annotations
	tags := make(map[string]string)
	for annotationKey, annotationValue := range sparkApp.GetAnnotations() {
		tags[annotationKey] = annotationValue
	}

	// set queue name if app labels it
	queueName := common.ApplicationDefaultQueue
	if an, ok := sparkApp.Labels[common.LabelQueueName]; ok {
		queueName = an
	}

	return cache.ApplicationMetadata{
		ApplicationID: sparkApp.Name,
		QueueName:     queueName,
		User:          "default",
		Tags:          tags,
	}
}

// list all existing applications
func (os *Manager) ListApplications() (map[string]cache.ApplicationMetadata, error) {
	lister := os.crdInformerFactory.Sparkoperator().V1beta2().SparkApplications().Lister()
	sparkApps, err := lister.List(labels.Everything())
	if err == nil {
		existingApps := make(map[string]cache.ApplicationMetadata)
		for _, sparkApp := range sparkApps {
			existingApps[sparkApp.Name] = os.getAppMetadata(sparkApp)
		}
		return existingApps, nil
	}
	return nil, err
}

// callbacks for SparkApplication CRD
func (os *Manager) addApplication(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	log.Logger.Info("spark app added", zap.Any("SparkApplication", app))
	os.amProtocol.AddApplication(&cache.AddApplicationRequest{
		Metadata: os.getAppMetadata(app),
		Recovery: false,
	})
}

func (os *Manager) updateApplication(old, new interface{}) {
	appOld := old.(*v1beta2.SparkApplication)
	appNew := new.(*v1beta2.SparkApplication)
	log.Logger.Info("spark app updated - old", zap.Any("SparkApplication", appOld))
	log.Logger.Info("spark app updated - new", zap.Any("SparkApplication", appNew))
}

func (os *Manager) deleteApplication(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	log.Logger.Info("spark app deleted", zap.Any("SparkApplication", app))
	os.amProtocol.NotifyApplicationComplete(os.getAppMetadata(app).ApplicationID)
}

// callbacks for Spark pods
func (os *Manager) filterPod(obj interface{}) bool {
	if pod, err := utils.Convert2Pod(obj); err == nil {
		if _, isTask := os.getTaskMetadata(pod); isTask {
			return true
		}
	}
	return false
}

func (os *Manager) addPod(obj interface{}) {
	if pod, err := utils.Convert2Pod(obj); err == nil {
		if meta, isTask := os.getTaskMetadata(pod); isTask {
			os.amProtocol.AddTask(&cache.AddTaskRequest{
				Metadata: meta,
				Recovery: false,
			})
		}
	}
}

func (os *Manager) updatePod(old, new interface{}) {
	// noop
}

func (os *Manager) deletePod(obj interface{}) {
	if pod, err := utils.Convert2Pod(obj); err == nil {
		if meta, isTask := os.getTaskMetadata(pod); isTask {
			os.amProtocol.NotifyTaskComplete(meta.ApplicationID, meta.TaskID)
		}
	}
}
