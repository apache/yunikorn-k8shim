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

package common

import (
	v1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

// stores info about what scheduler cares about a node
type Node struct {
	name     string
	uid      string
	resource *si.Resource
}

func CreateFrom(node *v1.Node) Node {
	return Node{
		name:     node.Name,
		uid:      string(node.UID),
		resource: GetNodeResource(&node.Status),
	}
}

func CreateFromNodeSpec(nodeName string, nodeUID string, nodeResource *si.Resource) Node {
	return Node{
		name:     nodeName,
		uid:      nodeUID,
		resource: nodeResource,
	}
}
