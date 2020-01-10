package sparkoperator

import (
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crcClientSet "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crInformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	k8sCache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type SparkOperatorService struct {
	amProtocol         cache.ApplicationManagementProtocol
	apiProvider        client.APIProvider
	crdInformer        k8sCache.SharedIndexInformer
	crdInformerFactory crInformers.SharedInformerFactory
	stopCh             <-chan struct{}
}

func New(amProtocol cache.ApplicationManagementProtocol, apiProvider client.APIProvider) *SparkOperatorService {
	return &SparkOperatorService{
		amProtocol:  amProtocol,
		apiProvider: apiProvider,
		stopCh:      nil, // TODO add stop flags
	}
}

// this implements AppManagementService interface
func (os *SparkOperatorService) ServiceInit() error {
	configs, err := clientcmd.BuildConfigFromFlags("",
		os.apiProvider.GetClientSet().Conf.KubeConfig)
	if err != nil {
		return err
	}

	crClient, err := crcClientSet.NewForConfig(configs)
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

func (os *SparkOperatorService) GetTaskMetadata(pod *v1.Pod) (cache.TaskMetadata, bool) {
	return cache.TaskMetadata{}, false
}

func (os *SparkOperatorService) GetAppMetadata(pod *v1.Pod) (cache.ApplicationMetadata, bool) {
	return cache.ApplicationMetadata{}, false
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
