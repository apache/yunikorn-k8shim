module github.com/apache/incubator-yunikorn-k8shim

go 1.12

require (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20200212043150-3df703098970
	github.com/apache/incubator-yunikorn-core v0.0.0-20200326231431-eebff3eda3db
	github.com/apache/incubator-yunikorn-scheduler-interface v0.0.0-20200324235502-68d212f575dc
	github.com/looplab/fsm v0.1.0
	github.com/yuin/goldmark v1.1.26 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/tools v0.0.0-20200325203130-f53864d0dba1 // indirect
	google.golang.org/genproto v0.0.0-20200326112834-f447254575fd // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20190624085159-95846d7ef82a
	k8s.io/apimachinery v0.0.0-20190624085041-961b39a1baa0
	k8s.io/client-go v0.0.0-20190624085356-2c6e35a5b9cf
	k8s.io/klog v0.3.3
	k8s.io/kubernetes v1.14.3
)

replace k8s.io/cloud-provider v0.0.0-20190624091323-9dc79cf4f9c7 => k8s.io/cloud-provider v0.0.0-20190516232619-2bf8e45c8454
