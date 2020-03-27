module github.com/apache/incubator-yunikorn-k8shim

go 1.12

require (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20200212043150-3df703098970
	github.com/apache/incubator-yunikorn-core v0.0.0-20200326231431-eebff3eda3db
	github.com/apache/incubator-yunikorn-scheduler-interface v0.0.0-20200324235502-68d212f575dc
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/looplab/fsm v0.1.0
	go.uber.org/zap v1.13.0
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20191004102349-159aefb8556b
	k8s.io/apiextensions-apiserver v0.0.0-20191212015246-8fe0c124fb40 // indirect
	k8s.io/apimachinery v0.0.0-20191004074956-c5d2f014d689
	k8s.io/apiserver v0.0.0-20191212015046-43d571094e6f // indirect
	k8s.io/client-go v11.0.1-0.20191029005444-8e4128053008+incompatible
	k8s.io/csi-translation-lib v0.0.0-20191212015623-92af21758231 // indirect
	k8s.io/klog v1.0.0
	k8s.io/kube-openapi v0.0.0-20200204173128-addea2498afe // indirect
	k8s.io/kubernetes v1.14.10
	k8s.io/utils v0.0.0-20200327001022-6496210b90e8 // indirect
)

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20191212015549-86a326830157
