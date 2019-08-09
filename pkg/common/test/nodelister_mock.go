/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package test

import (
	"fmt"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientv1 "k8s.io/client-go/listers/core/v1"
)

type NodeListerMock struct {
	allNodes []*v1.Node
}

func NewNodeListerMock() *NodeListerMock {
	return &NodeListerMock{
		allNodes: make([]*v1.Node, 0),
	}
}

func (n *NodeListerMock) AddNode(node *v1.Node) {
	n.allNodes = append(n.allNodes, node)
}

func (n *NodeListerMock) List(selector labels.Selector) (ret []*v1.Node, err error) {
	return n.allNodes, nil
}

func (n *NodeListerMock) Get(name string) (*v1.Node, error) {
	for _, n := range n.allNodes{
		if n.Name == name {
			return n, nil
		}
	}
	return nil, fmt.Errorf("node %s is not found", name)
}

func (n *NodeListerMock) ListWithPredicate(predicate clientv1.NodeConditionPredicate) ([]*v1.Node, error) {
	// ignore predicates for now
	return n.allNodes, nil
}
