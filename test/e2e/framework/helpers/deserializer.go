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
package helpers

import (
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
)

func yaml2Obj(yamlPath string) runtime.Object {
	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, _, err := decode(GetFileContents(yamlPath), nil, nil)
	check(err)

	return obj
}

func Y2Map(yamlPath string) map[interface{}]interface{} {
	m := make(map[interface{}]interface{})
	err := yaml.Unmarshal(GetFileContents(yamlPath), &m)
	check(err)
	return m
}
