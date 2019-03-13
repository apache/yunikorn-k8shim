package conf

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/k8s-shim/pkg/common"
	"os"
	"time"
)

type SchedulerConf struct {
	ClusterId string `json: cluster_id"`
	ClusterVersion string `json: cluster_version`
	SchedulerName string `json:"scheduler_name"`
	Interval int `json:"scheduling_interval_in_second"`
	KubeConfig string `json:"absolute_kube_config_file_path"`
}

func (conf *SchedulerConf) GetSchedulingInterval() time.Duration {
	return time.Duration(conf.Interval) * time.Second
}

func (conf *SchedulerConf) GetKubeConfigPath() string {
	return conf.KubeConfig
}

func ParseFromCommandline() *SchedulerConf {
	var clusterId *string
	var clusterVersion *string
	var schedulerName *string
	var kubeConfig *string
	var schedulingInterval *int

	//if home := getHomeDir(); home != "" {
	//	kubeConfig = flag.String("kubeconfig",
	//		filepath.Join(home, ".kube", "config"),
	//		"(optional) absolute path to the kubeconfig file")
	//} else {
	kubeConfig = flag.String("kubeconfig", "",
		"absolute path to the kubeconfig file")
	//}

	schedulingInterval = flag.Int("interval", 1, "scheduling interval in seconds")
	clusterId = flag.String("clusterid", common.ClusterId, "cluster id")
	clusterVersion = flag.String("clusterversion", common.ClusterVersion, "cluster version")
	schedulerName = flag.String("name", common.SchedulerName, "name of the scheduler")

	flag.Parse()

	return &SchedulerConf{
		ClusterId: *clusterId,
		ClusterVersion: *clusterVersion,
		SchedulerName: *schedulerName,
		Interval: *schedulingInterval,
		KubeConfig: *kubeConfig,
	}
}

func getHomeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return ""
}

func (conf *SchedulerConf) DumpConfiguration() {
	c,_ := json.MarshalIndent(&conf, "", " ")
	glog.V(3).Info(fmt.Sprintf("Scheduler conf: \n %s", string(c)))
}