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

package sparkopt

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crcClientSet "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crInformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
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

func (os *Manager) getAppMetadata(pod *v1.Pod) (cache.ApplicationMetadata, bool) {
	return cache.ApplicationMetadata{}, false
}

// list all existing applications
func (os *Manager) ListApplications() (map[string]cache.ApplicationMetadata, error) {
	lister := os.crdInformerFactory.Sparkoperator().V1beta2().SparkApplications().Lister()
	sparkApps, err := lister.List(labels.Everything())
	if err == nil {
		existingApps := make(map[string]cache.ApplicationMetadata)
		for _, sparkApp := range sparkApps {
			existingApps[sparkApp.Name] = cache.ApplicationMetadata{
				ApplicationID: sparkApp.Name,
				QueueName:     sparkApp.Namespace,
				User:          "",
				Tags:          make(map[string]string),
			}
		}
		return existingApps, nil
	}
	return nil, err
}

func (os *Manager) ListAppTasks(appID string) (map[string]cache.TaskMetadata, error) {
	lister := os.apiProvider.GetAPIs().PodInformer.Lister()
	matchedLabels := make(map[string]string)
	matchedLabels[SparkAppNameLabel] = appID
	sparkTasks, err := lister.List(labels.SelectorFromSet(matchedLabels))
	if err == nil {
		existingTasks := make(map[string]cache.TaskMetadata)
		for _, sparkPod := range sparkTasks {
			if meta, ok := os.getTaskMetadata(sparkPod); ok {
				existingTasks[meta.TaskID] = meta
			}
		}
		return existingTasks, nil
	}
	return nil, err
}

// callbacks for SparkApplication CRD
func (os *Manager) addApplication(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	log.Logger.Info("spark app added", zap.Any("SparkApplication", app))
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
}

// callbacks for Spark pods
func (os *Manager) filterPod(obj interface{}) bool {
	return true
}

func (os *Manager) addPod(obj interface{}) {

}

func (os *Manager) updatePod(old, new interface{}) {

}

func (os *Manager) deletePod(obj interface{}) {

}
