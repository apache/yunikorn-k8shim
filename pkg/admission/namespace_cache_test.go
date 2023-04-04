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

package admission

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
)

const testNS = "test-ns"

func TestFlags(t *testing.T) {
	cache := NewNamespaceCache(nil)
	cache.nameSpaces["notset"] = nsFlags{
		enableYuniKorn: -1,
		generateAppID:  -1,
	}
	cache.nameSpaces["exist-unset"] = nsFlags{
		enableYuniKorn: 0,
		generateAppID:  0,
	}
	cache.nameSpaces["exist-set"] = nsFlags{
		enableYuniKorn: 1,
		generateAppID:  1,
	}
	cache.nameSpaces["generate-set"] = nsFlags{
		enableYuniKorn: -1,
		generateAppID:  1,
	}

	assert.Equal(t, -1, cache.enableYuniKorn(""), "not in cache")
	assert.Equal(t, -1, cache.generateAppID(""), "not in cache")
	assert.Equal(t, -1, cache.enableYuniKorn("notset"), "exist not set")
	assert.Equal(t, -1, cache.generateAppID("notset"), "exist not set")
	assert.Equal(t, 0, cache.enableYuniKorn("exist-unset"), "exist enable not set")
	assert.Equal(t, 0, cache.generateAppID("exist-unset"), "exist generate not set")
	assert.Equal(t, 1, cache.enableYuniKorn("exist-set"), "exist enable set")
	assert.Equal(t, 1, cache.generateAppID("exist-set"), "exist generate set")
	assert.Equal(t, -1, cache.enableYuniKorn("generate-set"), "only generate set")
	assert.Equal(t, 1, cache.generateAppID("generate-set"), "generate should be set")
}

func TestNamespaceHandlers(t *testing.T) {
	kubeClient := client.NewKubeClientMock(false)

	informers := NewInformers(kubeClient, "default")
	cache := NewNamespaceCache(informers.Namespace)
	informers.Start()
	defer informers.Stop()

	// nothing in the cache
	assert.Equal(t, -1, cache.enableYuniKorn(testNS), "cache should have been empty")

	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testNS,
			Annotations: map[string]string{"key": "value"},
		},
	}

	nsInterface := kubeClient.GetClientSet().CoreV1().Namespaces()

	// validate OnAdd
	_, err := nsInterface.Create(context.Background(), ns, metav1.CreateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.namespaceExists(testNS)
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err)

	assert.Equal(t, -1, cache.enableYuniKorn(testNS), "cache should have contained NS")

	// validate OnUpdate
	ns2 := ns.DeepCopy()
	ns2.Annotations = map[string]string{constants.AnnotationEnableYuniKorn: "true",
		constants.AnnotationGenerateAppID: "false"}

	_, err = nsInterface.Update(context.Background(), ns2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.enableYuniKorn(testNS) > 0
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err)
	assert.Equal(t, 0, cache.generateAppID(testNS), "generate should have been set to false")

	ns2 = ns.DeepCopy()
	ns2.Annotations = map[string]string{constants.AnnotationGenerateAppID: "true"}

	_, err = nsInterface.Update(context.Background(), ns2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.generateAppID(testNS) > 0
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err)
	assert.Equal(t, -1, cache.enableYuniKorn(testNS), "enable should have been cleared")

	// validate OnDelete
	err = nsInterface.Delete(context.Background(), ns.Name, metav1.DeleteOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return !cache.namespaceExists(testNS)
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err, "ns not removed from cache")
}

func TestGetAnnotations(t *testing.T) {
	tests := map[string]struct {
		ns *v1.Namespace
		f  nsFlags
	}{
		"nil ns": {
			ns: nil,
			f:  nsFlags{enableYuniKorn: -1, generateAppID: -1},
		},
		"empty annotations": {
			ns: &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNS,
				},
			},
			f: nsFlags{enableYuniKorn: -1, generateAppID: -1},
		},
		"invalid values": {
			ns: &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNS,
					Annotations: map[string]string{
						constants.AnnotationGenerateAppID:  "",
						constants.AnnotationEnableYuniKorn: "unknown",
					},
				},
			},
			f: nsFlags{enableYuniKorn: 0, generateAppID: 0},
		},
		"true values": {
			ns: &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNS,
					Annotations: map[string]string{
						constants.AnnotationEnableYuniKorn: "true",
						constants.AnnotationGenerateAppID:  "true",
					},
				},
			},
			f: nsFlags{enableYuniKorn: 1, generateAppID: 1},
		},
		"distinct values": {
			ns: &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNS,
					Annotations: map[string]string{
						constants.AnnotationGenerateAppID:  "true",
						constants.AnnotationEnableYuniKorn: "false",
					},
				},
			},
			f: nsFlags{enableYuniKorn: 0, generateAppID: 1},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			f := getAnnotationValues(test.ns)
			assert.Equal(t, f.enableYuniKorn, test.f.enableYuniKorn, "enable value incorrect")
			assert.Equal(t, f.generateAppID, test.f.generateAppID, "enable value incorrect")
		})
	}
}
