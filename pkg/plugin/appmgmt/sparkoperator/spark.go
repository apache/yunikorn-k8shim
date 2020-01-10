package sparkoperator

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	k8scache "k8s.io/client-go/tools/cache"
)


type SparkOperatorService struct {
	// am protocol is used to sync info with scheduler cache
	amProtocol    cache.ApplicationManagementProtocol
	// shared context provides clients to communicate with api-server
	sharedContext *cache.SharedContext
	crdInformer k8scache.SharedIndexInformer
	crdInformerFactory crinformers.SharedInformerFactory
	stopCh <-chan struct{}
}

func NewWSparkOperatorService(crdInformerFactory crdinformers.SharedInformerFactory,) {

}

// this implements AppManagementService interface
func (os *SparkOperatorService) ServiceInit() error {
	// Create the client config. Use kubeConfig if given, otherwise assume in-cluster.
	configs, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	crClient, err := crclientset.NewForConfig(configs)
	if err != nil {
		return err
	}

	var factoryOpts []crinformers.SharedInformerOption
	os.crdInformerFactory = crinformers.NewSharedInformerFactoryWithOptions(
		crClient, 0, factoryOpts...)
	os.crdInformerFactory.Sparkoperator().V1beta2().SparkApplications().Informer()
	os.crdInformer = os.crdInformerFactory.Sparkoperator().V1beta2().SparkApplications().Informer()
	os.crdInformer.AddEventHandler(k8scache.ResourceEventHandlerFuncs{
		AddFunc:    os.addApplication,
		UpdateFunc: os.updateApplication,
		DeleteFunc: os.deleteApplication,
	})

	os.sharedContext.AddEventHandler(&cache.ResourceHandlerCallbacks{
		ResourceHandlersType: cache.PodInformerHandlers,
		FilterFn:             os.filterPod,
		AddFn:                os.addPod,
		UpdateFn:             os.updatePod,
		DeleteFn:             os.deletePod,
	})

	return nil
}

func (os *SparkOperatorService) Name() string {
	return "spark-operator-service"
}

func (os *SparkOperatorService) Start() error {
	go os.crdInformerFactory.Start(os.stopCh)
	return nil
}

func (os *SparkOperatorService) Stop() error {
	// TODO add stop logic
	return nil
}

func (os *SparkOperatorService) RecoverApplication(pod *v1.Pod) (app *cache.Application, recovering bool) {
	// do nothing
	return nil, false
}

// callbacks for SparkApplication CRD
func (os *SparkOperatorService) addApplication(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	log.Logger.Info("spark app added", zap.Any("SparkApplication", app))
}

func (os *SparkOperatorService) updateApplication(old, new interface{}) {
	appOld := old.(*v1beta2.SparkApplication)
	appNew := new.(*v1beta2.SparkApplication)
 	log.Logger.Info("spark app updated - old", zap.Any("SparkApplication", appOld))
	log.Logger.Info("spark app updated - new", zap.Any("SparkApplication", appNew))
}


func (os *SparkOperatorService) deleteApplication(obj interface{}) {
	app := obj.(*v1beta2.SparkApplication)
	log.Logger.Info("spark app deleted", zap.Any("SparkApplication", app))
}

// callbacks for Spark pods
func (os *SparkOperatorService) filterPod(obj interface{}) bool {
	return true
}

func (os *SparkOperatorService) addPod(obj interface{}) {

}

func (os *SparkOperatorService) updatePod(old, new interface{}) {

}


func (os *SparkOperatorService) deletePod(obj interface{}) {

}