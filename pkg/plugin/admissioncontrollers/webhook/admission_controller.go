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
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/admissioncontrollers/webhook/annotation"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

const (
	autoGenAppPrefix             = "yunikorn"
	autoGenAppSuffix             = "autogen"
	enableConfigHotRefreshEnvVar = "ENABLE_CONFIG_HOT_REFRESH"
	yunikornPod                  = "yunikorn"
	defaultQueue                 = "root.default"
	configHotFreshResponse       = "ConfigHotRefresh is disabled. Please use the REST API to update the configuration, or enable configHotRefresh. "
	admissionReviewAPIVersion    = "admission.k8s.io/v1"
	admissionReviewKind          = "AdmissionReview"
	userInfoAnnotation           = siCommon.DomainYuniKorn + "user.info"
)

var (
	runtimeScheme = runtime.NewScheme()
	codecs        = serializer.NewCodecFactory(runtimeScheme)
	deserializer  = codecs.UniversalDeserializer()
)

type admissionController struct {
	configName               string
	schedulerValidateConfURL string
	bypassAuth               bool
	processNamespaces        []*regexp.Regexp
	bypassNamespaces         []*regexp.Regexp
	labelNamespaces          []*regexp.Regexp
	noLabelNamespaces        []*regexp.Regexp
	annotationHandler        *annotation.UserGroupAnnotationHandler
}

type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

type ValidateConfResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

func initAdmissionController(configName string,
	schedulerValidateConfURL string,
	processNamespaces string,
	bypassNamespaces string,
	labelNamespaces string,
	noLabelNamespaces string,
	bypassAuth bool,
	trustControllers bool,
	systemUsers string,
	externalUsers string,
	externalGroups string) (*admissionController, error) {
	processRegexes, err := parseRegexes(processNamespaces)
	if err != nil {
		return nil, err
	}
	bypassRegexes, err := parseRegexes(bypassNamespaces)
	if err != nil {
		return nil, err
	}
	labelRegexes, err := parseRegexes(labelNamespaces)
	if err != nil {
		return nil, err
	}
	noLabelRegexes, err := parseRegexes(noLabelNamespaces)
	if err != nil {
		return nil, err
	}

	var systemUsersRegexes []*regexp.Regexp
	if systemUsers != "" {
		regexps, err := parseRegexes(systemUsers)
		if err != nil {
			return nil, err
		}
		systemUsersRegexes = regexps
	}

	var externalUsersRegexes []*regexp.Regexp
	if externalUsers != "" {
		regexps, err := parseRegexes(externalUsers)
		if err != nil {
			return nil, err
		}
		externalUsersRegexes = regexps
	}

	var externalGroupsRegexes []*regexp.Regexp
	if externalGroups != "" {
		regexps, err := parseRegexes(externalGroups)
		if err != nil {
			return nil, err
		}
		externalGroupsRegexes = regexps
	}

	annotationHandler := &annotation.UserGroupAnnotationHandler{
		TrustControllers: trustControllers,
		SystemUsers:      systemUsersRegexes,
		ExternalGroups:   externalGroupsRegexes,
		ExternalUsers:    externalUsersRegexes,
	}

	hook := admissionController{
		configName:               configName,
		schedulerValidateConfURL: schedulerValidateConfURL,
		bypassAuth:               bypassAuth,
		processNamespaces:        processRegexes,
		bypassNamespaces:         bypassRegexes,
		labelNamespaces:          labelRegexes,
		noLabelNamespaces:        noLabelRegexes,
		annotationHandler:        annotationHandler,
	}

	log.Logger().Info("Initialized YuniKorn Admission Controller",
		zap.String("processNs", processNamespaces),
		zap.String("bypassNs", bypassNamespaces),
		zap.String("labelNs", labelNamespaces),
		zap.String("noLabelNs", noLabelNamespaces),
		zap.Bool("bypassAuth", bypassAuth),
		zap.Bool("trustControllers", trustControllers),
		zap.String("systemUsers", systemUsers),
		zap.String("externalUsers", externalUsers),
		zap.String("externalGroups", externalGroups),
	)

	return &hook, nil
}

func parseRegexes(patterns string) ([]*regexp.Regexp, error) {
	result := make([]*regexp.Regexp, 0)
	for _, pattern := range strings.Split(patterns, ",") {
		pattern = strings.TrimSpace(pattern)
		if len(pattern) == 0 {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Logger().Error("Unable to compile regular expression", zap.String("pattern", pattern), zap.Error(err))
			return nil, err
		}
		result = append(result, re)
	}
	return result, nil
}

func admissionResponseBuilder(uid string, allowed bool, resultMessage string, patch []byte) *admissionv1.AdmissionResponse {
	res := &admissionv1.AdmissionResponse{}
	res.Allowed = allowed
	res.UID = types.UID(uid)

	if len(resultMessage) != 0 {
		res.Result = &metav1.Status{
			Message: resultMessage,
		}
	}
	if len(patch) != 0 {
		res.Patch = patch
		pt := admissionv1.PatchTypeJSONPatch
		res.PatchType = &pt
	}
	return res
}

func (c *admissionController) mutate(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req == nil {
		log.Logger().Warn("empty request received")
		return admissionResponseBuilder("", false, "", nil)
	}

	if req.Kind.Kind == "Pod" {
		return c.processPod(req)
	}

	return c.processWorkload(req)
}

func (c *admissionController) processPod(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	var patch []patchOperation
	var uid = string(req.UID)
	namespace := req.Namespace
	if namespace == "" {
		namespace = "default"
	}

	log.Logger().Info("AdmissionReview",
		zap.String("Namespace", namespace),
		zap.String("UID", uid),
		zap.String("Operation", string(req.Operation)),
		zap.Any("UserInfo", req.UserInfo))

	var pod v1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		log.Logger().Error("unmarshal failed", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	if failureResponse := c.checkUserInfoAnnotation(func() (string, bool) {
		a, ok := pod.Annotations[userInfoAnnotation]
		return a, ok
	}, req.UserInfo.Username, req.UserInfo.Groups, uid); failureResponse != nil {
		return failureResponse
	}

	if labelAppValue, ok := pod.Labels[constants.LabelApp]; ok {
		if labelAppValue == yunikornPod {
			log.Logger().Info("ignore yunikorn pod")
			return admissionResponseBuilder(uid, true, "", nil)
		}
	}

	if !c.shouldProcessNamespace(namespace) {
		log.Logger().Info("bypassing namespace", zap.String("namespace", namespace))
		return admissionResponseBuilder(uid, true, "", nil)
	}
	patch = updateSchedulerName(patch)

	if c.shouldLabelNamespace(namespace) {
		patch = updateLabels(namespace, &pod, patch)
	} else {
		log.Logger().Info("skipping update of pod labels since namespace is set to no-label",
			zap.String("podName", pod.Name),
			zap.String("generateName", pod.GenerateName),
			zap.String("namespace", namespace))
	}
	log.Logger().Info("generated patch",
		zap.String("podName", pod.Name),
		zap.String("generateName", pod.GenerateName),
		zap.Any("patch", patch))

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Logger().Error("failed to marshal patch", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	return admissionResponseBuilder(uid, true, "", patchBytes)
}

func (c *admissionController) processWorkload(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	var uid = string(req.UID)
	requestKind := req.Kind.Kind

	annotations, supported, err := c.annotationHandler.GetAnnotationsFromRequestKind(requestKind, req)
	if !supported {
		// Unknown request kind - pass
		return admissionResponseBuilder(uid, true, "", nil)
	}
	if err != nil {
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	if failureResponse := c.checkUserInfoAnnotation(func() (string, bool) {
		a, ok := annotations[userInfoAnnotation]
		return a, ok
	}, req.UserInfo.Username, req.UserInfo.Groups, uid); failureResponse != nil {
		return failureResponse
	}

	return admissionResponseBuilder(uid, true, "", nil)
}

func (c *admissionController) checkUserInfoAnnotation(getAnnotation func() (string, bool), userName string, groups []string, uid string) *admissionv1.AdmissionResponse {
	if annotation, ok := getAnnotation(); ok && !c.bypassAuth {
		if allowed := c.annotationHandler.IsAnnotationAllowed(userName, groups); !allowed {
			errMsg := fmt.Sprintf("user %s with groups [%s] is not allowed to set user annotation", userName,
				strings.Join(groups, ","))
			log.Logger().Error("user info validation failed - submitter is not allowed to set user annotation",
				zap.String("user", userName),
				zap.Strings("groups", groups))
			return admissionResponseBuilder(uid, false, errMsg, nil)
		}

		if err := c.annotationHandler.IsAnnotationValid(annotation); err != nil {
			log.Logger().Error("invalid user info annotation", zap.Error(err))
			return admissionResponseBuilder(uid, false, err.Error(), nil)
		}
	}

	return nil
}

func updateSchedulerName(patch []patchOperation) []patchOperation {
	log.Logger().Info("updating scheduler name")
	return append(patch, patchOperation{
		Op:    "add",
		Path:  "/spec/schedulerName",
		Value: constants.SchedulerName,
	})
}

// generate appID based on the namespace value,
// and the max length of the ID is 63 chars.
func generateAppID(namespace string) string {
	generatedID := fmt.Sprintf("%s-%s-%s", autoGenAppPrefix, namespace, autoGenAppSuffix)
	appID := fmt.Sprintf("%.63s", generatedID)
	return appID
}

func updateLabels(namespace string, pod *v1.Pod, patch []patchOperation) []patchOperation {
	log.Logger().Info("updating pod labels",
		zap.String("podName", pod.Name),
		zap.String("generateName", pod.GenerateName),
		zap.String("namespace", namespace),
		zap.Any("labels", pod.Labels))

	existingLabels := pod.Labels
	result := make(map[string]string)
	for k, v := range existingLabels {
		result[k] = v
	}

	if _, ok := existingLabels[constants.SparkLabelAppID]; !ok {
		if _, ok := existingLabels[constants.LabelApplicationID]; !ok {
			// if app id not exist, generate one
			// for each namespace, we group unnamed pods to one single app
			// application ID convention: ${AUTO_GEN_PREFIX}-${NAMESPACE}-${AUTO_GEN_SUFFIX}
			generatedID := generateAppID(namespace)
			result[constants.LabelApplicationID] = generatedID

			// if we generate an app ID, disable state-aware scheduling for this app
			if _, ok := existingLabels[constants.LabelDisableStateAware]; !ok {
				result[constants.LabelDisableStateAware] = "true"
			}
		}
	}

	if _, ok := existingLabels[constants.LabelQueueName]; !ok {
		result[constants.LabelQueueName] = defaultQueue
	}

	patch = append(patch, patchOperation{
		Op:    "add",
		Path:  "/metadata/labels",
		Value: result,
	})

	return patch
}

func isConfigMapUpdateAllowed(userInfo string) bool {
	hotRefreshEnabled := os.Getenv(enableConfigHotRefreshEnvVar)
	allowed, err := strconv.ParseBool(hotRefreshEnabled)
	if err != nil {
		log.Logger().Error("Failed to parse ENABLE_CONFIG_HOT_REFRESH value",
			zap.String("ENABLE_CONFIG_HOT_REFRESH", hotRefreshEnabled))
		return false
	}
	if allowed || strings.Contains(userInfo, "yunikorn-admin") {
		return true
	}
	return false
}

func (c *admissionController) validateConf(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req == nil {
		log.Logger().Warn("empty request received")
		return admissionResponseBuilder("", false, "", nil)
	}

	uid := string(req.UID)
	if !isConfigMapUpdateAllowed(req.UserInfo.Username) {
		return admissionResponseBuilder(uid, false, configHotFreshResponse, nil)
	}

	var requestKind = req.Kind.Kind
	if requestKind != "ConfigMap" {
		log.Logger().Warn("request kind is not configmap", zap.String("requestKind", requestKind))
		return admissionResponseBuilder(uid, true, "", nil)
	}

	var configmap v1.ConfigMap
	if err := json.Unmarshal(req.Object.Raw, &configmap); err != nil {
		log.Logger().Error("failed to unmarshal configmap", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	// validate new/updated config map
	if err := c.validateConfigMap(&configmap); err != nil {
		log.Logger().Error("failed to validate yunikorn configs", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	return admissionResponseBuilder(uid, true, "", nil)
}

func (c *admissionController) namespaceMatchesProcessList(namespace string) bool {
	if len(c.processNamespaces) == 0 {
		return true
	}
	for _, re := range c.processNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) namespaceMatchesBypassList(namespace string) bool {
	for _, re := range c.bypassNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) namespaceMatchesLabelList(namespace string) bool {
	if len(c.labelNamespaces) == 0 {
		return true
	}
	for _, re := range c.labelNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) namespaceMatchesNoLabelList(namespace string) bool {
	for _, re := range c.noLabelNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) shouldProcessNamespace(namespace string) bool {
	return c.namespaceMatchesProcessList(namespace) && !c.namespaceMatchesBypassList(namespace)
}

func (c *admissionController) shouldLabelNamespace(namespace string) bool {
	return c.namespaceMatchesLabelList(namespace) && !c.namespaceMatchesNoLabelList(namespace)
}

func (c *admissionController) validateConfigMap(cm *v1.ConfigMap) error {
	if cm.Name == constants.ConfigMapName {
		if content, ok := cm.Data[c.configName]; ok {
			checksum := fmt.Sprintf("%X", sha256.Sum256([]byte(content)))
			log.Logger().Info("Validating YuniKorn configuration", zap.String("checksum", checksum))
			log.Logger().Debug("Configmap data", zap.ByteString("content", []byte(content)))
			response, err := http.Post(c.schedulerValidateConfURL, "application/json", bytes.NewBuffer([]byte(content)))
			if err != nil {
				log.Logger().Error("YuniKorn scheduler is unreachable, assuming configmap is valid", zap.Error(err))
				return nil
			}
			defer response.Body.Close()
			if response.StatusCode < 200 || response.StatusCode > 299 {
				log.Logger().Error("YuniKorn scheduler responded with unexpected status, assuming configmap is valid",
					zap.Int("status", response.StatusCode))
				return nil
			}
			responseBytes, err := io.ReadAll(response.Body)
			if err != nil {
				log.Logger().Error("Unable to read response from YuniKorn scheduler, assuming configmap is valid", zap.Error(err))
				return nil
			}
			var responseData ValidateConfResponse
			if err := json.Unmarshal(responseBytes, &responseData); err != nil {
				log.Logger().Error("Unable to parse response from YuniKorn scheduler, assuming configmap is valid", zap.Error(err))
				return nil
			}
			if !responseData.Allowed {
				err = fmt.Errorf(responseData.Reason)
				log.Logger().Error("Configmap validation failed, aborting", zap.Error(err))
				return err
			}
		} else {
			err := fmt.Errorf("required config '%s' not found in this configmap", c.configName)
			log.Logger().Error("Unable to parse YuniKorn configmap", zap.Error(err))
			return err
		}
		log.Logger().Info("Successfully validated YuniKorn configuration")
	} else {
		log.Logger().Debug("Configmap does not belong to YuniKorn", zap.String("Name", cm.Name))
	}
	return nil
}

func (c *admissionController) health(w http.ResponseWriter, r *http.Request) {
	// for now, always healthy
	w.Header().Set("Content-type", "text/plain")
	w.WriteHeader(200)
	_, err := w.Write([]byte("OK\r\n"))
	if err != nil {
		log.Logger().Error("Unable to write health check result", zap.Error(err))
		return
	}
}

func (c *admissionController) serve(w http.ResponseWriter, r *http.Request) {
	log.Logger().Debug("request", zap.Any("httpRequest", r))
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil || len(body) == 0 {
			log.Logger().Debug("illegal request received: body invalid", zap.Error(err))
			http.Error(w, "empty or invalid body", http.StatusBadRequest)
			return
		}
	}

	// verify the content type is accurate
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		log.Logger().Debug("illegal request received: invalid content type", zap.String("requested content type", contentType))
		http.Error(w, "invalid Content-Type, expect `application/json`", http.StatusUnsupportedMediaType)
		return
	}

	urlPath := r.URL.Path
	if urlPath != mutateURL && urlPath != validateConfURL {
		log.Logger().Debug("unsupported request received", zap.String("urlPath", urlPath))
		http.Error(w, "request is neither mutation nor validation", http.StatusNotFound)
		return
	}
	ar := admissionv1.AdmissionReview{TypeMeta: metav1.TypeMeta{
		APIVersion: admissionReviewAPIVersion,
		Kind:       admissionReviewKind,
	}}

	var admissionResponse *admissionv1.AdmissionResponse
	_, _, err := deserializer.Decode(body, nil, &ar)
	if err != nil || ar.Request == nil {
		log.Logger().Error("request body decode failed or request empty", zap.Error(err))
		admissionResponse = admissionResponseBuilder("yunikorn-invalid-body", false, "body decode failed", nil)
	} else {
		req := ar.Request
		switch urlPath {
		case mutateURL:
			admissionResponse = c.mutate(req)
		case validateConfURL:
			admissionResponse = c.validateConf(req)
		}
	}
	admissionReview := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: admissionReviewAPIVersion,
			Kind:       admissionReviewKind,
		},
		Response: admissionResponse,
	}

	var resp []byte
	resp, err = json.Marshal(admissionReview)
	if err != nil {
		errMessage := fmt.Sprintf("could not encode response: %v", err)
		log.Logger().Error(errMessage)
		http.Error(w, errMessage, http.StatusInternalServerError)
	}

	if _, err = w.Write(resp); err != nil {
		errMessage := fmt.Sprintf("could not write response: %v", err)
		log.Logger().Error(errMessage)
		http.Error(w, errMessage, http.StatusInternalServerError)
	}
}
