module github.com/apache/incubator-yunikorn-k8shim

go 1.12

require (
	cloud.google.com/go v0.55.0 // indirect
	cloud.google.com/go/pubsub v1.3.1 // indirect
	dmitri.shuralyov.com/gpu/mtl v0.0.0-20191203043605-d42048ed14fd // indirect
	github.com/BurntSushi/xgb v0.0.0-20200324125942-20f126ea2843 // indirect
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20200212043150-3df703098970
	github.com/apache/incubator-yunikorn-core v0.0.0-20200403014952-0e3dc0a683e7
	github.com/apache/incubator-yunikorn-scheduler-interface v0.0.0-20200327234544-149aaa3d8e48
	github.com/cncf/udpa/go v0.0.0-20200324003616-bae28a880fdb // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.4.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/looplab/fsm v0.1.0
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/rogpeppe/go-internal v1.5.2 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	go.uber.org/zap v1.13.0
	golang.org/x/crypto v0.0.0-20200323165209-0ec3e9974c59 // indirect
	golang.org/x/exp v0.0.0-20200320212757-167ffe94c325 // indirect
	golang.org/x/image v0.0.0-20200119044424-58c23975cae1 // indirect
	golang.org/x/mobile v0.0.0-20200222142934-3c8601c510d0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20191004102349-159aefb8556b
	k8s.io/apiextensions-apiserver v0.0.0-20191212015246-8fe0c124fb40 // indirect
	k8s.io/apimachinery v0.0.0-20191004074956-c5d2f014d689
	k8s.io/apiserver v0.0.0-20191212015046-43d571094e6f // indirect
	k8s.io/client-go v11.0.1-0.20191029005444-8e4128053008+incompatible
	k8s.io/cloud-provider v0.0.0-00010101000000-000000000000 // indirect
	k8s.io/csi-translation-lib v0.0.0-20191212015623-92af21758231 // indirect
	k8s.io/klog v1.0.0
	k8s.io/kube-openapi v0.0.0-20200204173128-addea2498afe // indirect
	k8s.io/kubernetes v1.14.10
	k8s.io/utils v0.0.0-20200327001022-6496210b90e8 // indirect
	rsc.io/sampler v1.99.99 // indirect
)

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20191212015549-86a326830157

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20200302205851-738671d3881b
