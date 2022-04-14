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
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
	"github.com/apache/yunikorn-k8shim/pkg/client"
)

func TestList(t *testing.T) {
	lister := initLister(t)
	nodes, err := lister.List()
	assert.NilError(t, err, "List failed")
	assert.Assert(t, nodes != nil, "nodes was nil")
	assert.Equal(t, 2, len(nodes), "wrong length")
	m := make(map[string]*framework.NodeInfo)
	for _, node := range nodes {
		m[node.Node().Name] = node
	}
	_, ok := m["host0001"]
	assert.Assert(t, ok, "host0001 missing")
	_, ok = m["host0002"]
	assert.Assert(t, ok, "host0002 missing")
}

func TestGet(t *testing.T) {
	lister := initLister(t)
	node, err := lister.Get("host0001")
	assert.NilError(t, err, "Get failed")
	assert.Assert(t, node != nil, "node was nil")
	assert.Equal(t, "host0001", node.Node().Name, "wrong name for node")
	node, err = lister.Get("invalid")
	assert.Assert(t, err != nil, "invalid node was found")
	assert.Assert(t, node == nil, "node was not nil")
}

func TestHavePodsWithAffinityList(t *testing.T) {
	lister := initLister(t)
	nodes, err := lister.HavePodsWithAffinityList()
	assert.NilError(t, err, "HavePodsWithAffinityList failed")
	assert.Assert(t, nodes != nil, "nodes was nil")
	assert.Equal(t, 1, len(nodes), "wrong length")
	assert.Equal(t, "host0001", nodes[0].Node().Name, "wrong name for node")
}

func TestHavePodsWithRequiredAntiAffinityList(t *testing.T) {
	lister := initLister(t)
	nodes, err := lister.HavePodsWithRequiredAntiAffinityList()
	assert.NilError(t, err, "HavePodsWithAffinityList failed")
	assert.Assert(t, nodes != nil, "nodes was nil")
	assert.Equal(t, 1, len(nodes), "wrong length")
	assert.Equal(t, "host0002", nodes[0].Node().Name, "wrong name for node")
}

func initLister(t *testing.T) *nodeInfoListerImpl {
	cache := external.NewSchedulerCache(client.NewMockedAPIProvider(false).GetAPIs())
	lister, ok := NewSharedLister(cache).NodeInfos().(*nodeInfoListerImpl)
	assert.Assert(t, ok, "wrong type for node lister")

	cache.AddNode(&v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0001",
			Namespace: "default",
			UID:       "Node-UID-00001",
		},
	})
	cache.AddNode(&v1.Node{
		ObjectMeta: apis.ObjectMeta{
			Name:      "host0002",
			Namespace: "default",
			UID:       "Node-UID-00002",
		},
	})

	cache.GetNode("host0001").PodsWithAffinity = append(cache.GetNode("host0001").PodsWithAffinity, &framework.PodInfo{})
	cache.GetNode("host0002").PodsWithRequiredAntiAffinity = append(cache.GetNode("host0002").PodsWithRequiredAntiAffinity, &framework.PodInfo{})

	return lister
}
