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

package v1alpha1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	coordinationv1 "k8s.io/api/coordination/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/yaml"
)

func TestCheckedInQueueCRD(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "deployments", "queue-operator", "queue-crd.yaml")
	data, err := os.ReadFile(path)
	assert.NilError(t, err)

	scheme := runtime.NewScheme()
	assert.NilError(t, apiextensionsv1.AddToScheme(scheme))
	obj, _, err := serializer.NewCodecFactory(scheme).UniversalDeserializer().Decode(data, nil, nil)
	assert.NilError(t, err)
	crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
	assert.Assert(t, ok)
	assert.Equal(t, crd.Name, "queues.yunikorn.apache.org")
	assert.Equal(t, crd.Spec.Group, GroupVersion.Group)
	assert.Equal(t, len(crd.Spec.Versions), 1)
	assert.Equal(t, crd.Spec.Versions[0].Name, GroupVersion.Version)
	assert.Assert(t, crd.Spec.Versions[0].Served)
	assert.Assert(t, crd.Spec.Versions[0].Storage)
	assert.Assert(t, crd.Spec.Versions[0].Subresources != nil)
	assert.Assert(t, crd.Spec.Versions[0].Subresources.Status != nil)
	_, found := crd.Annotations["api-approved.kubernetes.io"]
	assert.Assert(t, !found)
}

func TestCheckedInQueueRBACUsesCanonicalGroup(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "deployments", "queue-operator", "queue-operator-rbac.yaml")
	data, err := os.ReadFile(path)
	assert.NilError(t, err)

	foundQueueRule := false
	for _, document := range splitYAMLDocuments(string(data)) {
		var header struct {
			Kind string `json:"kind"`
		}
		assert.NilError(t, yaml.Unmarshal([]byte(document), &header))
		if header.Kind != "ClusterRole" {
			continue
		}
		role := &rbacv1.ClusterRole{}
		assert.NilError(t, yaml.Unmarshal([]byte(document), role))
		for _, rule := range role.Rules {
			for _, resource := range rule.Resources {
				if resource != "queues" && resource != "queues/status" {
					continue
				}
				foundQueueRule = true
				assert.DeepEqual(t, rule.APIGroups, []string{GroupVersion.Group})
			}
		}
	}
	assert.Assert(t, foundQueueRule)
}

func TestCheckedInQueueRBACScopesSensitiveResources(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "deployments", "queue-operator", "queue-operator-rbac.yaml")
	data, err := os.ReadFile(path)
	assert.NilError(t, err)

	var rules []rbacv1.PolicyRule
	var lease *coordinationv1.Lease
	for _, document := range splitYAMLDocuments(string(data)) {
		var header struct {
			Kind string `json:"kind"`
		}
		assert.NilError(t, yaml.Unmarshal([]byte(document), &header))
		switch header.Kind {
		case "ClusterRole":
			role := &rbacv1.ClusterRole{}
			assert.NilError(t, yaml.Unmarshal([]byte(document), role))
			rules = append(rules, role.Rules...)
		case "Role":
			role := &rbacv1.Role{}
			assert.NilError(t, yaml.Unmarshal([]byte(document), role))
			rules = append(rules, role.Rules...)
		case "Lease":
			lease = &coordinationv1.Lease{}
			assert.NilError(t, yaml.Unmarshal([]byte(document), lease))
		}
	}
	assert.Assert(t, len(rules) > 0)
	assert.Assert(t, lease != nil)
	assert.Equal(t, lease.Name, "04cd498c.queue.yunikorn.k8s.io")

	wantNames := map[string]string{ //nolint:gosec // Kubernetes resource names, not credentials.
		"secrets":                         "yunikorn-queue-operator-webhook-ca",
		"validatingwebhookconfigurations": "yunikorn-queue-operator",
		"leases":                          "04cd498c.queue.yunikorn.k8s.io",
	}
	found := make(map[string]bool)
	for _, rule := range rules {
		for _, resource := range rule.Resources {
			wantName, sensitive := wantNames[resource]
			if !sensitive {
				continue
			}
			found[resource] = true
			assert.DeepEqual(t, rule.ResourceNames, []string{wantName})
			for _, verb := range rule.Verbs {
				assert.Assert(t, verb != "list" && verb != "watch" && verb != "create" && verb != "delete",
					"sensitive resource %s has broad verb %s", resource, verb)
			}
		}
	}
	for resource := range wantNames {
		assert.Assert(t, found[resource], "missing scoped RBAC rule for %s", resource)
	}
}

func splitYAMLDocuments(data string) []string {
	var documents []string
	for _, document := range strings.Split(data, "\n---") {
		if strings.TrimSpace(document) != "" {
			documents = append(documents, document)
		}
	}
	return documents
}
