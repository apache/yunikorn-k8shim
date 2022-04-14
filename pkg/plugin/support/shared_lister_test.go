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
	v1 "k8s.io/api/core/v1"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
)

func TestNewSharedLister(t *testing.T) {
	cache := external.NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	lister := NewSharedLister(cache)

	node := &v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
		},
	}
	cache.AddNode(node)

	nodeInfo, err := lister.NodeInfos().Get("host0001")
	assert.NilError(t, err, "err returned from Get call")
	assert.Assert(t, nodeInfo != nil, "node was nil")
	assert.Equal(t, "host0001", nodeInfo.Node().Name)
}
