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
package cache

import (
	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
	"go.uber.org/zap"
)

// map namespace to queues
type NamespaceMapper struct {

}

// filter namespace for the scheduler
func (m *NamespaceMapper) filterNamespace(obj interface{}) bool {
	return true
}

func (m *NamespaceMapper) addNamespace(object interface{}) {
	if namespace, err := utils.Convert2Namespace(object); err != nil {
		log.Logger.Error("namespace mapping is not performance", zap.Error(err))
	} else {
		log.Logger.Info("adding namespace", zap.String("namespace", namespace.Name))

	}
}

func (m *NamespaceMapper) updateNamespace(oldObject, newObject interface{}) {

}

func (m *NamespaceMapper) deleteNamespace(object interface{}) {

}
