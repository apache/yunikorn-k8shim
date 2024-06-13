//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

module github.com/apache/yunikorn-k8shim

go 1.21

require (
	github.com/apache/yunikorn-core v1.5.1-1
	github.com/apache/yunikorn-scheduler-interface v1.5.1-1
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/looplab/fsm v1.0.1
	github.com/onsi/ginkgo/v2 v2.15.0
	github.com/onsi/gomega v1.30.0
	github.com/prometheus/client_golang v1.18.0
	github.com/sasha-s/go-deadlock v0.3.1
	go.uber.org/zap v1.26.0
	gopkg.in/yaml.v3 v3.0.1
	gotest.tools/v3 v3.5.1
	k8s.io/api v0.29.6
	k8s.io/apimachinery v0.29.6
	k8s.io/cli-runtime v0.29.6
	k8s.io/client-go v0.29.6
	k8s.io/component-base v0.29.6
	k8s.io/klog/v2 v2.110.1
	k8s.io/kube-scheduler v0.29.6
	k8s.io/kubectl v0.29.6
	k8s.io/kubernetes v1.29.6
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230305170008-8188dc5388df // indirect
	github.com/asaskevich/govalidator v0.0.0-20190424111038-f61b66f89f4a // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/distribution/reference v0.5.0 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/cel-go v0.17.7 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/gregjones/httpcache v0.0.0-20190212212710-3befbb6ad0cc // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions/v2 v2.0.0 // indirect
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/moby/term v0.0.0-20221205130635-1aeaba878587 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/monochromegane/go-gitignore v0.0.0-20200626010858-205db1a8cc00 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mxk/go-flowrate v0.0.0-20140419014527-cca7078d478f // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/selinux v1.11.0 // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/petermattis/goid v0.0.0-20240327183114-c42a807a84ba // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.45.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/spf13/cobra v1.7.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/xlab/treeprint v1.2.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.10 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.10 // indirect
	go.etcd.io/etcd/client/v3 v3.5.10 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.46.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.44.0 // indirect
	go.opentelemetry.io/otel v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.20.0 // indirect
	go.opentelemetry.io/otel/sdk v1.20.0 // indirect
	go.opentelemetry.io/otel/trace v1.20.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	go.starlark.net v0.0.0-20230525235612-a134d8f9ddca // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/oauth2 v0.12.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/term v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.17.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230822172742-b8732ec3820d // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230822172742-b8732ec3820d // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect
	google.golang.org/grpc v1.59.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apiextensions-apiserver v0.0.0 // indirect
	k8s.io/apiserver v0.29.6 // indirect
	k8s.io/cloud-provider v0.29.6 // indirect
	k8s.io/component-helpers v0.29.6 // indirect
	k8s.io/controller-manager v0.29.6 // indirect
	k8s.io/csi-translation-lib v0.29.6 // indirect
	k8s.io/dynamic-resource-allocation v0.29.6 // indirect
	k8s.io/kms v0.29.6 // indirect
	k8s.io/kube-openapi v0.0.0-20231010175941-2dd684a91f00 // indirect
	k8s.io/kubelet v0.29.6 // indirect
	k8s.io/mount-utils v0.29.6 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.28.0 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/kustomize/api v0.13.5-0.20230601165947-6ce0bf390ce3 // indirect
	sigs.k8s.io/kustomize/kyaml v0.14.3-0.20230601165947-6ce0bf390ce3 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace (
	github.com/opencontainers/runc => github.com/opencontainers/runc v1.1.12
	github.com/petermattis/goid => github.com/petermattis/goid v0.0.0-20240327183114-c42a807a84ba
	golang.org/x/crypto => golang.org/x/crypto v0.21.0
	golang.org/x/lint => golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/net => golang.org/x/net v0.23.0
	golang.org/x/sys => golang.org/x/sys v0.18.0
	golang.org/x/text => golang.org/x/text v0.14.0
	golang.org/x/tools => golang.org/x/tools v0.17.0
	google.golang.org/protobuf => google.golang.org/protobuf v1.33.0
	k8s.io/api => k8s.io/api v0.29.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.29.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.29.6
	k8s.io/apiserver => k8s.io/apiserver v0.29.6
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.29.6
	k8s.io/client-go => k8s.io/client-go v0.29.6
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.29.6
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.29.6
	k8s.io/code-generator => k8s.io/code-generator v0.29.6
	k8s.io/component-base => k8s.io/component-base v0.29.6
	k8s.io/component-helpers => k8s.io/component-helpers v0.29.6
	k8s.io/controller-manager => k8s.io/controller-manager v0.29.6
	k8s.io/cri-api => k8s.io/cri-api v0.29.6
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.29.6
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.29.6
	k8s.io/endpointslice => k8s.io/endpointslice v0.29.6
	k8s.io/klog/v2 => k8s.io/klog/v2 v2.110.1
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.29.6
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.29.6
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.29.6
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.29.6
	k8s.io/kubectl => k8s.io/kubectl v0.29.6
	k8s.io/kubelet => k8s.io/kubelet v0.29.6
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.29.6
	k8s.io/metrics => k8s.io/metrics v0.29.6
	k8s.io/mount-utils => k8s.io/mount-utils v0.29.6
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.29.6
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.29.6
	k8s.io/utils => k8s.io/utils v0.0.0-20230726121419-3b25d923346b
)
