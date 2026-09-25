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
	"testing"

	"gotest.tools/v3/assert"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
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
