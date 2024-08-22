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

package client

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestInformerTypes(t *testing.T) {
	assert.Equal(t, 15, len(informerTypes), "wrong informerTypes length")

	assert.Equal(t, "Pod", PodInformerHandlers.String())
	assert.Equal(t, "Node", NodeInformerHandlers.String())
	assert.Equal(t, "ConfigMap", ConfigMapInformerHandlers.String())
	assert.Equal(t, "PV", PVInformerHandlers.String())
	assert.Equal(t, "PVC", PVCInformerHandlers.String())
	assert.Equal(t, "Storage", StorageInformerHandlers.String())
	assert.Equal(t, "CSINode", CSINodeInformerHandlers.String())
	assert.Equal(t, "CSIDriver", CSIDriverInformerHandlers.String())
	assert.Equal(t, "CSIStorageCapacity", CSIStorageCapacityInformerHandlers.String())
	assert.Equal(t, "Namespace", NamespaceInformerHandlers.String())
	assert.Equal(t, "PriorityClass", PriorityClassInformerHandlers.String())
	assert.Equal(t, "Service", ServiceInformerHandlers.String())
	assert.Equal(t, "ReplicationController", ReplicationControllerInformerHandlers.String())
	assert.Equal(t, "ReplicaSet", ReplicaSetInformerHandlers.String())
	assert.Equal(t, "StatefulSet", StatefulSetInformerHandlers.String())
}
