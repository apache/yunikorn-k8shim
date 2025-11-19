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
	"errors"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/apache/yunikorn-k8shim/pkg/cache/external"
)

type nodeInfoListerImpl struct {
	cache *external.SchedulerCache
}

func (n nodeInfoListerImpl) List() ([]fwk.NodeInfo, error) {
	return n.cache.GetNodesInfo(), nil
}

func (n nodeInfoListerImpl) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return n.cache.GetNodesInfoPodsWithAffinity(), nil
}

func (n nodeInfoListerImpl) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return n.cache.GetNodesInfoPodsWithReqAntiAffinity(), nil
}

func (n nodeInfoListerImpl) Get(nodeName string) (fwk.NodeInfo, error) {
	nodes := n.cache.GetNodesInfoMap()
	node, ok := nodes[nodeName]
	if !ok {
		return nil, errors.New("node not found")
	}
	return node, nil
}

var _ framework.NodeInfoLister = &nodeInfoListerImpl{}

// NewNodeInfoLister returns a new NodeInfoLister which references the scheduler cache. The returned lister is
// not safe for access without acquiring the scheduler cache read lock first.
func NewNodeInfoLister(cache *external.SchedulerCache) framework.NodeInfoLister {
	nl := &nodeInfoListerImpl{
		cache: cache,
	}
	return nl
}
