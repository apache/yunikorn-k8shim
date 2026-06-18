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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
)

func TestNewFrameworkHandle(t *testing.T) {
	clientSet := ClientSet()
	handle := NewFrameworkHandle(Lister(), InformerFactory(clientSet), clientSet, test.NewCSIManagerMock(client.NewMockedAPIProvider(false).GetAPIs().CSINodeInformer.Lister()), SharedDRAManager())
	_, ok := handle.(*frameworkHandle)
	assert.Assert(t, ok, "handle was of wrong type")
}

func TestSnapshotSharedLister(t *testing.T) {
	sl := Lister()
	clientSet := ClientSet()
	handle := NewFrameworkHandle(sl, InformerFactory(clientSet), clientSet, test.NewCSIManagerMock(client.NewMockedAPIProvider(false).GetAPIs().CSINodeInformer.Lister()), SharedDRAManager())
	sl2 := handle.SnapshotSharedLister()
	assert.Equal(t, sl, sl2, "wrong shared lister")
}

func TestSharedInformerFactory(t *testing.T) {
	clientSet := ClientSet()
	si := InformerFactory(clientSet)
	lister := Lister()
	handle := NewFrameworkHandle(lister, si, clientSet, test.NewCSIManagerMock(client.NewMockedAPIProvider(false).GetAPIs().CSINodeInformer.Lister()), SharedDRAManager())
	si2 := handle.SharedInformerFactory()
	assert.Equal(t, si, si2, "wrong shared informer")
}

func TestClientSet(t *testing.T) {
	cs := ClientSet()
	handle := NewFrameworkHandle(Lister(), InformerFactory(cs), cs, test.NewCSIManagerMock(client.NewMockedAPIProvider(false).GetAPIs().CSINodeInformer.Lister()), SharedDRAManager())
	cs2 := handle.ClientSet()
	assert.Equal(t, cs, cs2, "wrong clientset")
}
