/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package support

import (
	"context"

	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/resourceslice/tracker"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
)

func InformerFactory(clientSet kubernetes.Interface) informers.SharedInformerFactory {
	return informers.NewSharedInformerFactory(clientSet, 0)
}

func ClientSet() kubernetes.Interface {
	return client.NewKubeClientMock(false).GetClientSet()
}

func Lister() *test.SharedListerMock {
	return test.NewSharedListerMock()
}

func SharedDRAManager() *dynamicresources.DefaultDRAManager {
	if feature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		resourceClaimInformer := InformerFactory(ClientSet()).Resource().V1().ResourceClaims().Informer()
		resourceClaimCache := assumecache.NewAssumeCache(klog.NewKlogr(), resourceClaimInformer, "ResourceClaim", "", nil)
		resourceSliceTracker, err := tracker.StartTracker(context.TODO(), tracker.Options{
			EnableDeviceTaintRules: feature.DefaultFeatureGate.Enabled(features.DRADeviceTaints),
			SliceInformer:          InformerFactory(ClientSet()).Resource().V1().ResourceSlices(),
			ClassInformer:          InformerFactory(ClientSet()).Resource().V1().DeviceClasses(),
			TaintInformer:          InformerFactory(ClientSet()).Resource().V1beta2().DeviceTaintRules()})
		if err != nil {
			return nil
		}
		sharedDRAManager := dynamicresources.NewDRAManager(context.TODO(), resourceClaimCache, resourceSliceTracker, InformerFactory(ClientSet()))
		return sharedDRAManager
	} else {
		return nil
	}
}
