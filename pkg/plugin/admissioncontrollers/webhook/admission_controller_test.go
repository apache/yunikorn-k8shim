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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/assert"
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/conf"
)

type responseMode int

const (
	Success = responseMode(0)
	Failure = responseMode(1)
	Error   = responseMode(2)
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
		assert.Equal(t, len(updatedMap), 4)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["disableStateAware"], "true")
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
		assert.Equal(t, len(updatedMap), 4)
		assert.Equal(t, updatedMap["random"], "random")
		assert.Equal(t, updatedMap["queue"], "root.abc")
		assert.Equal(t, updatedMap["disableStateAware"], "true")
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
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["disableStateAware"], "true")
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
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["disableStateAware"], "true")
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
		assert.Equal(t, len(updatedMap), 3)
		assert.Equal(t, updatedMap["queue"], "root.default")
		assert.Equal(t, updatedMap["disableStateAware"], "true")
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
	controller, err := initAdmissionController(configName, "", "", "", "", "", false, true, "", "", "")
	if err != nil {
		t.Fatal("failed to init admission controller", err)
	}
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.DefaultConfigMapName,
		},
		Data: make(map[string]string),
	}
	// specified config "queues.yaml" not found
	err = controller.validateConfigMap(configmap)
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
	srv := serverMock(Success)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(t, strings.Replace(srv.URL, "http://", "", 1), "", "", "", "")
	err := controller.validateConfigMap(configmap)
	assert.NilError(t, err, "No error expected")
}

/**
Test for the case when the POST request is successful, but the config change is not allowed.
*/
func TestValidateConfigMapInValidConfig(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Failure)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(t, strings.Replace(srv.URL, "http://", "", 1), "", "", "", "")
	err := controller.validateConfigMap(configmap)
	assert.Equal(t, "Invalid config", err.Error(),
		"Other error returned than the expected one")
}

/**
Test for the case when the POST request fails
*/
func TestValidateConfigMapWrongRequest(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Failure)
	defer srv.Close()
	// the url is wrong, so the POST request will fail, and success will be assumed
	controller := prepareController(t, srv.URL, "", "", "", "")
	err := controller.validateConfigMap(configmap)
	assert.NilError(t, err, "No error expected")
}

/**
Test for the case of server error
*/
func TestValidateConfigMapServerError(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Error)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(t, strings.Replace(srv.URL, "http://", "", 1), "", "", "", "")
	err := controller.validateConfigMap(configmap)
	assert.NilError(t, err, "No error expected")
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

func prepareController(t *testing.T, url string, processNs string, bypassNs string, labelNs string, noLabelNs string) *admissionController {
	configName := fmt.Sprintf("%s.yaml", conf.DefaultPolicyGroup)
	if bypassNs == "" {
		bypassNs = "^kube-system$"
	}
	controller, err := initAdmissionController(
		configName,
		fmt.Sprintf(schedulerValidateConfURLPattern, url),
		processNs, bypassNs, labelNs, noLabelNs,
		false, true,
		"system:serviceaccount:kube-system:job-controller", "testExtUser", "testExtGroup")
	if err != nil {
		t.Fatal("failed to prepare controller", err)
	}
	return controller
}

func serverMock(mode responseMode) *httptest.Server {
	handler := http.NewServeMux()
	switch mode {
	case Success:
		handler.HandleFunc("/ws/v1/validate-conf", successResponseMock)
	case Failure:
		handler.HandleFunc("/ws/v1/validate-conf", failedResponseMock)
	case Error:
		handler.HandleFunc("/ws/v1/validate-conf", errorResponseMock)
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

func errorResponseMock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(500)
	resp := `{}`
	w.Write([]byte(resp)) //nolint:errcheck
}

func TestGenerateAppID(t *testing.T) {
	appID := generateAppID("this-is-a-namespace")
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

func TestMutate(t *testing.T) {
	var ac *admissionController
	var pod v1.Pod
	var req *admissionv1.AdmissionRequest
	var resp *admissionv1.AdmissionResponse
	var podJSON []byte
	var err error

	ac = prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$")

	// nil request
	resp = ac.mutate(nil)
	assert.Check(t, !resp.Allowed, "response allowed with nil request")

	// yunikorn pod
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Labels:    map[string]string{"app": "yunikorn"},
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for yunikorn pod")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for yunikorn pod")

	// pod without applicationID
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for pod")
	assert.Equal(t, labels(t, resp.Patch)["applicationId"], "yunikorn-test-ns-autogen", "wrong applicationId label")
	assert.Equal(t, labels(t, resp.Patch)["disableStateAware"], "true", "missing disableStateAware label")

	// pod with applicationId
	pod.ObjectMeta.Labels = map[string]string{"applicationId": "test-app"}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for pod")
	assert.Equal(t, labels(t, resp.Patch)["applicationId"], "test-app", "wrong applicationId label")

	// pod in bypassed namespace
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "bypass",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "bypass",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for bypassed pod")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for bypassed pod")

	// pod in no-label namespace
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "nolabel",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "nolabel",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for nolabel pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for nolabel pod")
	assert.Equal(t, len(labels(t, resp.Patch)), 0, "non-empty labels for nolabel pod")

	// unknown object type
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Unknown"}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for unknown object type")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for unknown object type")
}

func TestExternalAuthentication(t *testing.T) {
	ac := prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$")

	// validation fails, submitter user is not whitelisted
	pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Annotations: map[string]string{
			userInfoAnnotation: "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}",
		},
	}}
	podJSON, err := json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req := &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		UserInfo: authv1.UserInfo{
			Username: "test",
			Groups:   []string{"dev"},
		},
	}
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Pod"}
	resp := ac.mutate(req)
	assert.Check(t, !resp.Allowed, "response was allowed")
	assert.Check(t, strings.Contains(resp.Result.Message, "not allowed to set user annotation"))

	// should pass as "testExtUser" is allowed to add the userInfo annotation
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		UserInfo: authv1.UserInfo{
			Username: "testExtUser",
			Groups:   []string{"dev"},
		},
	}
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Pod"}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed")

	// invalid annotation
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Annotations: map[string]string{
			userInfoAnnotation: "xyzxyz",
		},
	}}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Pod"}
	resp = ac.mutate(req)
	assert.Check(t, !resp.Allowed, "response was allowed")
	assert.Check(t, strings.Contains(resp.Result.Message, "invalid character 'x'"))
}

func parsePatch(t *testing.T, patch []byte) []patchOperation {
	res := make([]patchOperation, 0)
	if len(patch) == 0 {
		return res
	}
	err := json.Unmarshal(patch, &res)
	assert.NilError(t, err, "error unmarshalling patch")
	return res
}

func schedulerName(t *testing.T, patch []byte) string {
	ops := parsePatch(t, patch)
	for _, op := range ops {
		if op.Path == "/spec/schedulerName" {
			return op.Value.(string)
		}
	}
	return ""
}

func labels(t *testing.T, patch []byte) map[string]interface{} {
	ops := parsePatch(t, patch)
	for _, op := range ops {
		if op.Path == "/metadata/labels" {
			return op.Value.(map[string]interface{})
		}
	}
	return make(map[string]interface{})
}

func TestShouldProcessNamespace(t *testing.T) {
	ac := prepareController(t, "", "", "^kube-system$,^pre-,-post$", "", "")
	assert.Check(t, ac.shouldProcessNamespace("test"), "test namespace not allowed")
	assert.Check(t, !ac.shouldProcessNamespace("kube-system"), "kube-system namespace allowed")
	assert.Check(t, ac.shouldProcessNamespace("x-kube-system"), "x-kube-system namespace not allowed")
	assert.Check(t, ac.shouldProcessNamespace("kube-system-x"), "kube-system-x namespace not allowed")
	assert.Check(t, !ac.shouldProcessNamespace("pre-ns"), "pre-ns namespace allowed")
	assert.Check(t, !ac.shouldProcessNamespace("ns-post"), "ns-post namespace allowed")

	ac = prepareController(t, "", "^allow-", "^allow-except-", "", "")
	assert.Check(t, !ac.shouldProcessNamespace("test"), "test namespace allowed when not on process list")
	assert.Check(t, ac.shouldProcessNamespace("allow-this"), "allow-this namespace not allowed when on process list")
	assert.Check(t, !ac.shouldProcessNamespace("allow-except-this"), "allow-except-this namespace allowed when on bypass list")
}

func TestShouldLabelNamespace(t *testing.T) {
	ac := prepareController(t, "", "", "", "", "^skip$")
	assert.Check(t, ac.shouldLabelNamespace("test"), "test namespace not allowed")
	assert.Check(t, !ac.shouldLabelNamespace("skip"), "skip namespace allowed when on no-label list")

	ac = prepareController(t, "", "", "", "^allow-", "^allow-except-")
	assert.Check(t, !ac.shouldLabelNamespace("test"), "test namespace allowed when not on label list")
	assert.Check(t, ac.shouldLabelNamespace("allow-this"), "allow-this namespace not allowed when on label list")
	assert.Check(t, !ac.shouldLabelNamespace("allow-except-this"), "allow-except-this namespace allowed when on no-label list")
}

func TestParseRegexes(t *testing.T) {
	var regexes []*regexp.Regexp
	var err error

	regexes, err = parseRegexes("")
	assert.NilError(t, err, "failed to parse empty pattern")
	assert.Equal(t, len(regexes), 0, "got results for empty pattern")

	regexes, err = parseRegexes("^test$")
	assert.NilError(t, err, "failed to parse simple pattern")
	assert.Equal(t, len(regexes), 1, "wrong count for simple pattern")
	assert.Check(t, regexes[0].MatchString("test"), "didn't match test")
	assert.Check(t, !regexes[0].MatchString("testx"), "matched testx")
	assert.Check(t, !regexes[0].MatchString("xtest"), "matched xtest")

	regexes, err = parseRegexes(" ^this$, ^that$ ")
	assert.NilError(t, err, "failed to parse compound pattern")
	assert.Equal(t, len(regexes), 2, "wrong count for compound pattern")
	assert.Check(t, regexes[0].MatchString("this"), "didn't match this")
	assert.Check(t, regexes[1].MatchString("that"), "didn't match that")

	regexes, err = parseRegexes("^a\\s+b$")
	assert.NilError(t, err, "failed to parse escaped pattern")
	assert.Equal(t, len(regexes), 1, "wrong count for escaped pattern")
	assert.Check(t, regexes[0].MatchString("a \t b"), "didn't match 'a \t b'")
	assert.Check(t, !regexes[0].MatchString("ab"), "matched 'ab'")

	_, err = parseRegexes("^($")
	assert.ErrorContains(t, err, "error parsing regexp", "bad pattern doesn't fail")
}

func TestInitAdmissionControllerRegexErrorHandling(t *testing.T) {
	var err error

	_, err = initAdmissionController("", "", "", "", "", "", false, true, "", "", "")
	assert.NilError(t, err, "error returned from simple init")

	_, err = initAdmissionController("", "", "(", "", "", "", false, true, "", "", "")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad processNamespaces list")

	_, err = initAdmissionController("", "", "", "(", "", "", false, true, "", "", "")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad bypassNamespaces list")

	_, err = initAdmissionController("", "", "", "", "(", "", false, true, "", "", "")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad labelNamespaces list")

	_, err = initAdmissionController("", "", "", "", "", "(", false, true, "", "", "")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad noLabelNamespaces list")

	_, err = initAdmissionController("", "", "", "", "", "", false, true, "(", "", "")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad systemUsers list")

	_, err = initAdmissionController("", "", "", "", "", "", false, true, "", "(", "")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad externalUsers list")

	_, err = initAdmissionController("", "", "", "", "", "", false, true, "", "", "(")
	assert.ErrorContains(t, err, "error parsing regexp", "didn't fail on bad externalGroups list")
}

func TestEnvSettings(t *testing.T) {
	originalEnv := getEnvSettings()
	defer func() {
		os.Setenv(admissionControllerNamespace, originalEnv.namespace)
		os.Setenv(admissionControllerService, originalEnv.serviceName)
		os.Setenv(admissionControllerProcessNamespaces, originalEnv.processNamespaces)
		os.Setenv(admissionControllerBypassNamespaces, originalEnv.bypassNamespaces)
		os.Setenv(admissionControllerLabelNamespaces, originalEnv.labelNamespaces)
		os.Setenv(admissionControllerNoLabelNamespaces, originalEnv.noLabelNamespaces)
		os.Setenv(admissionControllerBypassAuth, strconv.FormatBool(originalEnv.bypassAuth))
		os.Setenv(admissionControllerSystemUsers, originalEnv.systemUsers)
		os.Setenv(admissionControllerExternalUsers, originalEnv.externalUsers)
		os.Setenv(admissionControllerExternalGroups, originalEnv.externalGroups)
		os.Setenv(admissionControllerBypassControllers, strconv.FormatBool(originalEnv.bypassControllers))
	}()

	// test valid settings
	os.Setenv(admissionControllerNamespace, "testNamespace")
	os.Setenv(admissionControllerService, "testYunikornService")
	os.Setenv(admissionControllerProcessNamespaces, "testProcessNamespaces")
	os.Setenv(admissionControllerBypassNamespaces, "testBypassNamespaces")
	os.Setenv(admissionControllerLabelNamespaces, "testLabelNamespaces")
	os.Setenv(admissionControllerNoLabelNamespaces, "testNolabelNamespaces")
	os.Setenv(admissionControllerBypassAuth, "false")
	os.Setenv(admissionControllerSystemUsers, "systemuser")
	os.Setenv(admissionControllerExternalUsers, "yunikorn")
	os.Setenv(admissionControllerExternalGroups, "devs")
	os.Setenv(admissionControllerBypassControllers, "true")
	env := getEnvSettings()
	assert.Equal(t, env.namespace, "testNamespace")
	assert.Equal(t, env.serviceName, "testYunikornService")
	assert.Equal(t, env.processNamespaces, "testProcessNamespaces")
	assert.Equal(t, env.bypassNamespaces, "testBypassNamespaces")
	assert.Equal(t, env.labelNamespaces, "testLabelNamespaces")
	assert.Equal(t, env.noLabelNamespaces, "testNolabelNamespaces")
	assert.Equal(t, env.bypassAuth, false)
	assert.Equal(t, env.systemUsers, "systemuser")
	assert.Equal(t, env.externalUsers, "yunikorn")
	assert.Equal(t, env.externalGroups, "devs")
	assert.Equal(t, env.bypassControllers, true)

	// test missing settings
	os.Unsetenv(admissionControllerProcessNamespaces)
	os.Unsetenv(admissionControllerBypassNamespaces)
	os.Unsetenv(admissionControllerLabelNamespaces)
	os.Unsetenv(admissionControllerNoLabelNamespaces)
	os.Unsetenv(admissionControllerBypassAuth)
	os.Unsetenv(admissionControllerSystemUsers)
	os.Unsetenv(admissionControllerExternalUsers)
	os.Unsetenv(admissionControllerExternalGroups)
	os.Unsetenv(admissionControllerBypassControllers)
	env = getEnvSettings()
	assert.Equal(t, env.processNamespaces, "")
	assert.Equal(t, env.bypassNamespaces, defaultBypassNamespaces)
	assert.Equal(t, env.labelNamespaces, "")
	assert.Equal(t, env.noLabelNamespaces, "")
	assert.Equal(t, env.bypassAuth, defaultBypassAuth)
	assert.Equal(t, env.systemUsers, defaultSystemUsers)
	assert.Equal(t, env.externalUsers, "")
	assert.Equal(t, env.externalGroups, "")
	assert.Equal(t, env.bypassControllers, defaultBypassControllers)

	// test faulty settings for boolean values
	os.Setenv(admissionControllerBypassAuth, "xyz")
	os.Setenv(admissionControllerBypassControllers, "xyz")
	env = getEnvSettings()
	assert.Equal(t, env.bypassAuth, defaultBypassAuth)
	assert.Equal(t, env.bypassControllers, defaultBypassControllers)
}
