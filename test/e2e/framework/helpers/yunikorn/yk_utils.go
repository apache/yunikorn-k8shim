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

package yunikorn

import (
	"fmt"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

type ResourceUsage struct {
	resource map[string]int64
}

func (r *ResourceUsage) ParseResourceUsage(resource map[string]int64) {
	r.resource = resource
}

func (r *ResourceUsage) GetResourceValue(resourceName string) int64 {
	return r.resource[resourceName]
}

func GetYKUrl() string {
	return fmt.Sprintf("%s://%s",
		configmanager.YuniKornTestConfig.YkScheme,
		GetYKHost(),
	)
}

func GetYKHost() string {
	return fmt.Sprintf("%s:%s",
		configmanager.YuniKornTestConfig.YkHost,
		configmanager.YuniKornTestConfig.YkPort,
	)
}

func GetYKScheme() string {
	return configmanager.YuniKornTestConfig.YkScheme
}

func CreateDefaultConfigMap() *v1.ConfigMap {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.ConfigMapName,
			Namespace: configmanager.YuniKornTestConfig.YkNamespace,
		},
		Data: make(map[string]string),
	}
	cm.Data[configmanager.DefaultPolicyGroup] = configs.DefaultSchedulerConfig
	return cm
}
