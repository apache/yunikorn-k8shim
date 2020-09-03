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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/conf"
)

func TestUpdateLabels(t *testing.T) {
	// verify when appId/queue are not given,
	// we patch it correctly
	var patch []patchOperation

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random": "random",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
	}

	// verify if applicationId is given in the labels,
	// we won't modify it
	patch = make([]patchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random":        "random",
				"applicationId": "app-0001",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["applicationId"], "app-0001")
	} else {
		t.Fatal("patch info content is not as expected")
	}

	// verify if queue is given in the labels,
	// we won't modify it
	patch = make([]patchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"random": "random",
				"queue":  "root.abc",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.abc")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
	}

	// namespace might be empty
	// labels might be empty
	patch = make([]patchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
	}

	// pod name might be empty, it can comes from generatedName
	patch = make([]patchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "some-pod-",
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
	}

	// pod name and generate name could be both empty
	patch = make([]patchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       v1.PodSpec{},
		Status:     v1.PodStatus{},
	}

	patch = updateLabels("default", pod, patch)

	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/metadata/labels")
	if updatedMap, ok := patch[0].Value.(map[string]string); ok {
		assert.Equal(t, len(updatedMap), 2)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, strings.HasPrefix(updatedMap["applicationId"], autoGenAppPrefix), true)
	} else {
		t.Fatal("patch info content is not as expected")
	}
}

func TestUpdateSchedulerName(t *testing.T) {
	var patch []patchOperation
	patch = updateSchedulerName(patch)
	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, "add")
	assert.Equal(t, patch[0].Path, "/spec/schedulerName")
	if name, ok := patch[0].Value.(string); ok {
		assert.Equal(t, name, constants.SchedulerName)
	} else {
		t.Fatal("patch info content is not as expected")
	}
}

func TestValidateConfigMap(t *testing.T) {
	configName := fmt.Sprintf("%s.yaml", conf.DefaultPolicyGroup)
	controller := &admissionController{
		configName: configName,
	}
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.DefaultConfigMapName,
		},
		Data: make(map[string]string),
	}
	// specified config "queues.yaml" not found
	err := controller.validateConfigMap(configmap)
	assert.Assert(t, err != nil, "expecting error when specified config is not found")
	assert.Equal(t, err.Error(), "required config 'queues.yaml' not found in this configmap")
	// skip further validations which depends on the webservice of yunikorn-core
}

const ConfigData = `
partitions:
  - name: default
    placementrules:
        - name: tag
          value: namespace
          create: true
    queues:
      - name: root
        submitacl: "*"
`

/**
Test for the case when the POST request is successful, and the config change is allowed.
*/
func TestValidateConfigMapValidConfig(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(true)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(strings.Replace(srv.URL, "http://", "", 1))
	err := controller.validateConfigMap(configmap)
	assert.NilError(t, err, "No error expected")
}

/**
Test for the case when the POST request is successful, but the config change is not allowed.
*/
func TestValidateConfigMapInValidConfig(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(false)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(strings.Replace(srv.URL, "http://", "", 1))
	err := controller.validateConfigMap(configmap)
	assert.Equal(t, "Invalid config", err.Error(),
		"Other error returned than the expected one")
}

/**
Test for the case when the POST request fails
*/
func TestValidateConfigMapWrongRequest(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(false)
	defer srv.Close()
	// the url is wrong, so the POST request will fail and an error will be returned
	controller := prepareController(srv.URL)
	err := controller.validateConfigMap(configmap)
	assert.Equal(t, true, strings.Contains(err.Error(), "no such host"),
		"Other error returned than the expected one")
}

func prepareConfigMap(data string) *v1.ConfigMap {
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.DefaultConfigMapName,
		},
		Data: map[string]string{"queues.yaml": data},
	}
	return configmap
}

func prepareController(url string) *admissionController {
	configName := fmt.Sprintf("%s.yaml", conf.DefaultPolicyGroup)
	controller := &admissionController{
		configName: configName,
	}
	controller.schedulerValidateConfURL = fmt.Sprintf(schedulerValidateConfURLPattern, url)
	return controller
}

func serverMock(valid bool) *httptest.Server {
	handler := http.NewServeMux()
	if valid {
		handler.HandleFunc("/ws/v1/validate-conf", successResponseMock)
	} else {
		handler.HandleFunc("/ws/v1/validate-conf", failedResponseMock)
	}
	srv := httptest.NewServer(handler)
	return srv
}

func successResponseMock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	resp := `{
		"allowed": true,
		"reason": ""
		}`
	w.Write([]byte(resp)) //nolint:errcheck
}

func failedResponseMock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	resp := `{
		"allowed": false,
		"reason": "Invalid config"
		}`
	w.Write([]byte(resp)) //nolint:errcheck
}

func TestGenerateAppID(t *testing.T) {
	appID := generateAppID("")
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-default", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 24)

	appID = generateAppID("this-is-a-namespace")
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-this-is-a-namespace", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 36)

	appID = generateAppID("short")
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-short", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 22)

	appID = generateAppID(strings.Repeat("long", 100))
	assert.Equal(t, strings.HasPrefix(appID, fmt.Sprintf("%s-long", autoGenAppPrefix)), true)
	assert.Equal(t, len(appID), 63)
}

func TestIsConfigMapUpdateAllowed(t *testing.T) {
	testCases := []struct {
		name             string
		allowed          bool
		userInfo         string
		enableHotRefresh bool
	}{
		{"Hot refresh enabled, yunikorn user", true, "default:yunikorn-admin", true},
		{"Hot refresh enabled, non yunikorn user", true, "default:some-user", true},
		{"Hot refresh disabled, non yunikorn user", false, "default:some-user", false},
		{"Hot refresh disabled, yunikorn user", true, "default:yunikorn-admin", false},
	}
	for _, tc := range testCases {
		os.Setenv(enableConfigHotRefreshEnvVar, strconv.FormatBool(tc.enableHotRefresh))
		t.Run(tc.name, func(t *testing.T) {
			allowed := isConfigMapUpdateAllowed(tc.userInfo)
			assert.Equal(t, tc.allowed, allowed, "")
		})
	}
}
