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
	"errors"
	"fmt"
	"regexp"

	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
)

type ResourceUsage struct {
	memory string
	vCPU   string
}

func (r *ResourceUsage) ParseResourceUsageString(resource string) error {
	var expression = regexp.MustCompile(`memory:(\d+) vcore:(\d+)`)
	matches := expression.FindStringSubmatch(resource)
	if matches == nil {
		return errors.New("no match found")
	}
	r.memory = matches[1]
	r.vCPU = matches[2]
	return nil
}

func (r *ResourceUsage) GetMemory() string {
	return r.memory
}

func (r *ResourceUsage) GetVCPU() string {
	return r.vCPU
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
