package cache

import (
	"github.com/cloudera/yunikorn-core/pkg/api"
	"github.com/cloudera/yunikorn-k8shim/pkg/client"
	"github.com/cloudera/yunikorn-k8shim/pkg/conf"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"
	"sync"
	"time"
)

type ResourceHandlersType int

const (
	PodInformerHandlers ResourceHandlersType = iota
	NodeInformerHandlers
	ConfigMapInformerHandlers
	StorageInformerHandlers
	PVInformerHandlers
	PVCInformerHandlers
)

// resource handlers defines operations for add/update/delete operations
// on the corresponding resources. The associated the type field points
// the handler functions to the correct receiver.
type ResourceHandlerCallbacks struct {
	ResourceHandlersType
	FilterFn func(obj interface{}) bool
	AddFn func(obj interface{})
	UpdateFn func(old, new interface{})
	DeleteFn func(obj interface{})
}

type ResourceHandler interface {
	GetClientSet() *client.SchedulerClientSet
	AddEventHandler (handlers *ResourceHandlerCallbacks)
	Run(stopCh <-chan struct{})
}

// shared context maintains a set of useful struts that used to be accessing cross components
type SharedContext struct {
	clients *client.SchedulerClientSet
	lock *sync.RWMutex
}

func NewResourceHandlerContext(scheduler api.SchedulerApi, kubeClient client.KubeClient, configs *conf.SchedulerConf) *SharedContext {
	// we have disabled re-sync to keep ourselves up-to-date
	informerFactory := informers.NewSharedInformerFactory(kubeClient.GetClientSet(), 0)

	// init informers
	// volume informers are also used to get the Listers for the predicates
	nodeInformer := informerFactory.Core().V1().Nodes()
	podInformer := informerFactory.Core().V1().Pods()
	configMapInformer := informerFactory.Core().V1().ConfigMaps()
	storageInformer := informerFactory.Storage().V1().StorageClasses()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	// create a volume binder (needs the informers)
	volumeBinder := volumebinder.NewVolumeBinder(
		kubeClient.GetClientSet(),
		nodeInformer, pvcInformer,
		pvInformer,
		storageInformer,
		configs.VolumeBindTimeout)

	return &SharedContext{
		clients: &client.SchedulerClientSet{
			Conf:              configs,
			KubeClient:        kubeClient,
			SchedulerApi:      scheduler,
			PodInformer:       podInformer,
			NodeInformer:      nodeInformer,
			ConfigMapInformer: configMapInformer,
			PVInformer:        pvInformer,
			PVCInformer:       pvcInformer,
			StorageInformer:   storageInformer,
			VolumeBinder:      volumeBinder,
		},
		lock: &sync.RWMutex{},
	}
}

func (s *SharedContext) GetClientSet() *client.SchedulerClientSet {
	return s.clients
}

func (s *SharedContext) AddEventHandler(handlers *ResourceHandlerCallbacks) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// register all handlers
	var h cache.ResourceEventHandler
	fns := cache.ResourceEventHandlerFuncs{
		AddFunc:    handlers.AddFn,
		UpdateFunc: handlers.UpdateFn,
		DeleteFunc: handlers.DeleteFn,
	}

	// if filter function exists
	// add a wrapper
	if handlers.FilterFn != nil {
		h = cache.FilteringResourceEventHandler{
			FilterFunc: handlers.FilterFn,
			Handler:    fns,
		}
	} else {
		h = fns
	}

	s.addEventHandlers(handlers.ResourceHandlersType, h, 0)
}

func (s *SharedContext) addEventHandlers(
	handlerType ResourceHandlersType, handler cache.ResourceEventHandler, resyncPeriod time.Duration) {
	switch handlerType {
	case PodInformerHandlers:
		s.GetClientSet().PodInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case NodeInformerHandlers:
		s.GetClientSet().NodeInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case ConfigMapInformerHandlers:
		s.GetClientSet().ConfigMapInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case StorageInformerHandlers:
		s.GetClientSet().StorageInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PVInformerHandlers:
		s.GetClientSet().PVInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	case PVCInformerHandlers:
		s.GetClientSet().PVCInformer.Informer().
			AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	}
}

func (s *SharedContext) Run(stopCh <-chan struct{}) {
	// lunch clients
	s.clients.Run(stopCh)
}

