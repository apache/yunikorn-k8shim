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

package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestQueueOperatorCacheScopesOnlyConfigMaps(t *testing.T) {
	options := queueOperatorCacheOptions("scheduler")
	var configMaps cache.ByObject
	found := false
	for object, settings := range options.ByObject {
		if _, ok := object.(*corev1.ConfigMap); ok {
			configMaps = settings
			found = true
		}
	}
	if !found {
		t.Fatal("ConfigMap cache settings are missing")
	}
	if len(configMaps.Namespaces) != 1 {
		t.Fatalf("ConfigMap namespaces = %v, want one", configMaps.Namespaces)
	}
	if _, found := configMaps.Namespaces["scheduler"]; !found {
		t.Fatalf("ConfigMap cache does not include target namespace: %v", configMaps.Namespaces)
	}
	if got := configMaps.Namespaces["scheduler"].FieldSelector.String(); got != "metadata.name=yunikorn-configs" {
		t.Fatalf("ConfigMap field selector = %q, want only yunikorn-configs", got)
	}
	if options.DefaultNamespaces != nil {
		t.Fatalf("default namespace scoping would incorrectly restrict Queue CRs: %v", options.DefaultNamespaces)
	}
	if len(options.ByObject) != 1 {
		t.Fatalf("unexpected object cache settings: %v", options.ByObject)
	}
}
