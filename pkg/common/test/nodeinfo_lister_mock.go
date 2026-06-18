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

package test

import (
	"errors"

	fwk "k8s.io/kube-scheduler/framework"
)

type NodeInfoListerMock struct {
	nodeInfos []fwk.NodeInfo
}

func (n *NodeInfoListerMock) List() ([]fwk.NodeInfo, error) {
	return n.nodeInfos, nil
}

func (n *NodeInfoListerMock) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	result := make([]fwk.NodeInfo, 0, len(n.nodeInfos))
	for _, node := range n.nodeInfos {
		if len(node.GetPodsWithAffinity()) > 0 {
			result = append(result, node)
		}
	}
	return result, nil
}

func (n *NodeInfoListerMock) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	result := make([]fwk.NodeInfo, 0, len(n.nodeInfos))
	for _, node := range n.nodeInfos {
		if len(node.GetPodsWithRequiredAntiAffinity()) > 0 {
			result = append(result, node)
		}
	}
	return result, nil
}

func (n *NodeInfoListerMock) Get(nodeName string) (fwk.NodeInfo, error) {
	for _, node := range n.nodeInfos {
		if node.Node().Name == nodeName {
			return node, nil
		}
	}
	return nil, errors.New("node not found")
}

func (n *NodeInfoListerMock) Set(nodeInfos []fwk.NodeInfo) {
	n.nodeInfos = nodeInfos
}

func NewNodeInfoListerMock() *NodeInfoListerMock {
	return &NodeInfoListerMock{}
}
