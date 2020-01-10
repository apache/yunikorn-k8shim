package appmgmt

import (
	"fmt"
	"time"

	"github.com/cloudera/yunikorn-k8shim/pkg/cache"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/events"
	"github.com/cloudera/yunikorn-k8shim/pkg/common/utils"
	"github.com/cloudera/yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/listers/core/v1"
)

func (svc *SchedulerAppManager) WaitForRecovery(maxTimeout time.Duration) error {
	// Currently, disable recovery when testing in a mocked cluster,
	// because mock pod/node lister is not easy. We do have unit tests for
	// waitForAppRecovery/waitForNodeRecovery separately.
	if !svc.skipRecovery {
		if err := svc.waitForAppRecovery(svc.apiProvider.GetClientSet().PodInformer.Lister(), maxTimeout); err != nil {
			log.Logger.Error("app recovery failed", zap.Error(err))
			return err
		}
	}

	return nil
}

func (svc *SchedulerAppManager) recoverApp(pod *corev1.Pod) (*cache.Application, bool){
	// pod from a existing app must have been assigned to a node,
	// this means the app was scheduled and needs to be recovered
	if utils.IsAssignedPod(pod) && utils.IsSchedulablePod(pod) {
		for _, appmgmt := range svc.amService {
			if appMeta, ok := appmgmt.GetAppMetadata(pod); ok {
				if _, exist := svc.amProtocol.GetApplication(appMeta.ApplicationID); !exist {
					// if app already exist, that means it is already under recovering
					// otherwise, recovery this app through am protocol
					app := svc.amProtocol.AddApplication(&cache.AddApplicationRequest{
						Metadata: appMeta,
						Recovery: true,
					})
					return app, true
				}
			}
		}
	}
	return nil, false
}

// Wait until all previous scheduled applications are recovered, or fail as timeout.
// During this process, shim submits all applications again to the scheduler-core and verifies app
// state to ensure they are accepted, this must be done before recovering app allocations.
func (svc *SchedulerAppManager) waitForAppRecovery(lister v1.PodLister, maxTimeout time.Duration) error {
	// give informers sometime to warm up...
	allPods, err := waitAndListPods(lister)
	if err != nil {
		return err
	}

	// scan all pods and discover apps, for apps already scheduled before,
	// trigger app recovering
	toRecoverApps := make(map[string]*cache.Application, 0)
	for _, pod := range allPods {
		if app, recovering := svc.recoverApp(pod); recovering {
			toRecoverApps[app.GetApplicationID()] = app
		}
	}

	if len(toRecoverApps) > 0 {
		// check app states periodically, ensure all apps exit from recovering state
		if err := utils.WaitForCondition(func() bool {
			for _, app := range toRecoverApps {
				log.Logger.Info("appInfo",
					zap.String("appId", app.GetApplicationID()),
					zap.String("state", app.GetApplicationState()))
				if app.GetApplicationState() == events.States().Application.Accepted {
					delete(toRecoverApps, app.GetApplicationID())
				}
			}

			if len(toRecoverApps) == 0 {
				log.Logger.Info("app recovery is successful")
				return true
			}

			return false
		}, 1 * time.Second, maxTimeout); err != nil{
			return fmt.Errorf("timeout waiting for app recovery in %s", maxTimeout.String())
		}
	}

	return nil
}

func waitAndListPods(lister v1.PodLister) (pods []*corev1.Pod, err error){
	var allPods []*corev1.Pod
	if err := utils.WaitForCondition(func() bool {
		if allPods, _ = lister.List(labels.Everything()); allPods != nil {
			if len(allPods) > 0 {
				return true
			}
		}
		return false
	}, time.Second, time.Minute); err != nil {
		return nil, err
	}

	return allPods, nil
}