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

	"github.com/G-Research/yunikorn-k8shim/pkg/client"
	"github.com/G-Research/yunikorn-k8shim/pkg/common/constants"
	"github.com/G-Research/yunikorn-k8shim/pkg/common/utils"
)

const testNS = "test-ns"

func TestFlags(t *testing.T) {
	cache, nsErr := NewNamespaceCache(nil)
	assert.NilError(t, nsErr)
	cache.nameSpaces["notset"] = nsFlags{
		enableYuniKorn: UNSET,
		generateAppID:  UNSET,
	}
	cache.nameSpaces["exist-unset"] = nsFlags{
		enableYuniKorn: FALSE,
		generateAppID:  FALSE,
	}
	cache.nameSpaces["exist-set"] = nsFlags{
		enableYuniKorn: TRUE,
		generateAppID:  TRUE,
	}
	cache.nameSpaces["generate-set"] = nsFlags{
		enableYuniKorn: UNSET,
		generateAppID:  TRUE,
	}

	assert.Equal(t, UNSET, cache.enableYuniKorn(""), "not in cache")
	assert.Equal(t, UNSET, cache.generateAppID(""), "not in cache")
	assert.Equal(t, UNSET, cache.enableYuniKorn("notset"), "exist not set")
	assert.Equal(t, UNSET, cache.generateAppID("notset"), "exist not set")
	assert.Equal(t, FALSE, cache.enableYuniKorn("exist-unset"), "exist enable not set")
	assert.Equal(t, FALSE, cache.generateAppID("exist-unset"), "exist generate not set")
	assert.Equal(t, TRUE, cache.enableYuniKorn("exist-set"), "exist enable set")
	assert.Equal(t, TRUE, cache.generateAppID("exist-set"), "exist generate set")
	assert.Equal(t, UNSET, cache.enableYuniKorn("generate-set"), "only generate set")
	assert.Equal(t, TRUE, cache.generateAppID("generate-set"), "generate should be set")
}

func TestNamespaceHandlers(t *testing.T) {
	kubeClient := client.NewKubeClientMock(false)

	informers := NewInformers(kubeClient, "default")
	cache, nsErr := NewNamespaceCache(informers.Namespace)
	assert.NilError(t, nsErr)
	informers.Start()
	defer informers.Stop()

	// nothing in the cache
	assert.Equal(t, UNSET, cache.enableYuniKorn(testNS), "cache should have been empty")

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

	assert.Equal(t, UNSET, cache.enableYuniKorn(testNS), "cache should have contained NS")

	// validate OnUpdate
	ns2 := ns.DeepCopy()
	ns2.Annotations = map[string]string{constants.AnnotationEnableYuniKorn: "true",
		constants.AnnotationGenerateAppID: "false"}

	_, err = nsInterface.Update(context.Background(), ns2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.enableYuniKorn(testNS) == TRUE
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err)
	assert.Equal(t, FALSE, cache.generateAppID(testNS), "generate should have been set to false")

	ns2 = ns.DeepCopy()
	ns2.Annotations = map[string]string{constants.AnnotationGenerateAppID: "true"}

	_, err = nsInterface.Update(context.Background(), ns2, metav1.UpdateOptions{})
	assert.NilError(t, err)

	err = utils.WaitForCondition(func() bool {
		return cache.generateAppID(testNS) == TRUE
	}, 10*time.Millisecond, 5*time.Second)
	assert.NilError(t, err)
	assert.Equal(t, UNSET, cache.enableYuniKorn(testNS), "enable should have been cleared")

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
			f:  nsFlags{enableYuniKorn: UNSET, generateAppID: UNSET},
		},
		"empty annotations": {
			ns: &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: testNS,
				},
			},
			f: nsFlags{enableYuniKorn: UNSET, generateAppID: UNSET},
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
			f: nsFlags{enableYuniKorn: FALSE, generateAppID: FALSE},
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
			f: nsFlags{enableYuniKorn: TRUE, generateAppID: TRUE},
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
			f: nsFlags{enableYuniKorn: FALSE, generateAppID: TRUE},
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
