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

module github.com/apache/incubator-yunikorn-k8shim

go 1.16

require (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20201215015655-2e8b733f5ad0
	github.com/apache/incubator-yunikorn-core v0.0.0-20220204165339-fbf1c4f21f90
	github.com/apache/incubator-yunikorn-scheduler-interface v0.0.0-20220113193536-681c62202d1f
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/looplab/fsm v0.1.0
	github.com/onsi/ginkgo v1.11.0
	github.com/onsi/gomega v1.7.0
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	go.uber.org/zap v1.13.0
	gopkg.in/yaml.v2 v2.2.8
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.20.11
	k8s.io/apimachinery v0.20.11
	k8s.io/apiserver v0.20.11
	k8s.io/client-go v0.20.11
	k8s.io/component-base v0.20.11
	k8s.io/klog v1.0.0
	k8s.io/kube-scheduler v0.20.11
	k8s.io/kubernetes v1.20.11
)

replace (
	k8s.io/api => k8s.io/api v0.20.11
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.20.11
	k8s.io/apimachinery => k8s.io/apimachinery v0.20.11
	k8s.io/apiserver => k8s.io/apiserver v0.20.11
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.20.11
	k8s.io/client-go => k8s.io/client-go v0.20.11
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.20.11
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.20.11
	k8s.io/code-generator => k8s.io/code-generator v0.20.11
	k8s.io/component-base => k8s.io/component-base v0.20.11
	k8s.io/component-helpers => k8s.io/component-helpers v0.20.11
	k8s.io/controller-manager => k8s.io/controller-manager v0.20.11
	k8s.io/cri-api => k8s.io/cri-api v0.20.11
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.20.11
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.20.11
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.20.11
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.20.11
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.20.11
	k8s.io/kubectl => k8s.io/kubectl v0.20.11
	k8s.io/kubelet => k8s.io/kubelet v0.20.11
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.20.11
	k8s.io/metrics => k8s.io/metrics v0.20.11
	k8s.io/mount-utils => k8s.io/mount-utils v0.20.11
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.20.11
	vbom.ml/util => github.com/fvbommel/util v0.0.0-20160121211510-db5cfe13f5cc
)
