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

go 1.16

require (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20201215015655-2e8b733f5ad0
	github.com/apache/yunikorn-core v0.0.0-20221201192233-339cc04bfe3d
	github.com/apache/yunikorn-scheduler-interface v0.0.0-20221130170804-42d2286739d8
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/looplab/fsm v0.1.0
	github.com/onsi/ginkgo v1.14.0
	github.com/onsi/gomega v1.10.1
	go.uber.org/zap v1.19.0
	gopkg.in/yaml.v2 v2.4.0
	gotest.tools v2.2.0+incompatible
	gotest.tools/v3 v3.0.3
	k8s.io/api v0.23.14
	k8s.io/apimachinery v0.23.14
	k8s.io/apiserver v0.23.14
	k8s.io/cli-runtime v0.23.14
	k8s.io/client-go v0.23.14
	k8s.io/component-base v0.23.14
	k8s.io/klog v1.0.0
	k8s.io/kube-scheduler v0.23.14
	k8s.io/kubernetes v1.23.14
)

replace (
	golang.org/x/crypto => golang.org/x/crypto v0.3.0
	golang.org/x/lint => golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/net => golang.org/x/net v0.3.0
	golang.org/x/sys => golang.org/x/sys v0.2.0
	golang.org/x/text => golang.org/x/text v0.4.0
	golang.org/x/tools => golang.org/x/tools v0.3.0
	k8s.io/api => k8s.io/api v0.23.14
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.23.14
	k8s.io/apimachinery => k8s.io/apimachinery v0.23.14
	k8s.io/apiserver => k8s.io/apiserver v0.23.14
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.23.14
	k8s.io/client-go => k8s.io/client-go v0.23.14
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.23.14
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.23.14
	k8s.io/code-generator => k8s.io/code-generator v0.23.14
	k8s.io/component-base => k8s.io/component-base v0.23.14
	k8s.io/component-helpers => k8s.io/component-helpers v0.23.14
	k8s.io/controller-manager => k8s.io/controller-manager v0.23.14
	k8s.io/cri-api => k8s.io/cri-api v0.23.14
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.23.14
	k8s.io/klog/v2 => k8s.io/klog/v2 v2.30.0
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.23.14
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.23.14
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.23.14
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.23.14
	k8s.io/kubectl => k8s.io/kubectl v0.23.14
	k8s.io/kubelet => k8s.io/kubelet v0.23.14
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.23.14
	k8s.io/metrics => k8s.io/metrics v0.23.14
	k8s.io/mount-utils => k8s.io/mount-utils v0.23.14
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.23.14
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.23.14
	k8s.io/utils => k8s.io/utils v0.0.0-20211116205334-6203023598ed
	sigs.k8s.io/json => sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6
)
