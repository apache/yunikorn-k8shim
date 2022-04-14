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
	"testing"

	"gotest.tools/assert"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
)

func TestNewFrameworkHandle(t *testing.T) {
	clientSet := clientSet()
	handle := NewFrameworkHandle(lister(), informerFactory(clientSet), clientSet)
	_, ok := handle.(*frameworkHandle)
	assert.Assert(t, ok, "handle was of wrong type")
}

func TestSnapshotSharedLister(t *testing.T) {
	sl := lister()
	clientSet := clientSet()
	handle := NewFrameworkHandle(sl, informerFactory(clientSet), clientSet)
	sl2 := handle.SnapshotSharedLister()
	assert.Equal(t, sl, sl2, "wrong shared lister")
}

func TestSharedInformerFactory(t *testing.T) {
	clientSet := clientSet()
	si := informerFactory(clientSet)
	handle := NewFrameworkHandle(lister(), si, clientSet)
	si2 := handle.SharedInformerFactory()
	assert.Equal(t, si, si2, "wrong shared informer")
}

func TestClientSet(t *testing.T) {
	cs := clientSet()
	handle := NewFrameworkHandle(lister(), informerFactory(cs), cs)
	cs2 := handle.ClientSet()
	assert.Equal(t, cs, cs2, "wrong clientset")
}

func lister() framework.SharedLister {
	cache := external.NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	return NewSharedLister(cache)
}

func informerFactory(clientSet kubernetes.Interface) informers.SharedInformerFactory {
	return informers.NewSharedInformerFactory(clientSet, 0)
}

func clientSet() kubernetes.Interface {
	return client.NewKubeClientMock(false).GetClientSet()
}
