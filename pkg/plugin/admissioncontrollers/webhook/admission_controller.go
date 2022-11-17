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
	ctx "context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	schedulerconf "github.com/apache/yunikorn-k8shim/pkg/conf"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/admissioncontrollers/webhook/annotation"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/admissioncontrollers/webhook/common"
	"github.com/apache/yunikorn-k8shim/pkg/plugin/admissioncontrollers/webhook/conf"
)

const (
	autoGenAppPrefix                = "yunikorn"
	autoGenAppSuffix                = "autogen"
	yunikornPod                     = "yunikorn"
	defaultQueue                    = "root.default"
	admissionReviewAPIVersion       = "admission.k8s.io/v1"
	admissionReviewKind             = "AdmissionReview"
	schedulerValidateConfURLPattern = "http://%s/ws/v1/validate-conf"
	mutateURL                       = "/mutate"
	validateConfURL                 = "/validate-conf"
	pod                             = "Pod"
)

var (
	runtimeScheme = runtime.NewScheme()
	codecs        = serializer.NewCodecFactory(runtimeScheme)
	deserializer  = codecs.UniversalDeserializer()
)

type admissionController struct {
	conf              *conf.AdmissionControllerConf
	annotationHandler *annotation.UserGroupAnnotationHandler
	clientSet         kubernetes.Interface
}

type ValidateConfResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

func initAdmissionController(conf *conf.AdmissionControllerConf, clientSet kubernetes.Interface) *admissionController {
	hook := &admissionController{
		conf:              conf,
		annotationHandler: annotation.NewUserGroupAnnotationHandler(conf),
		clientSet:         clientSet,
	}

	log.Logger().Info("Initialized YuniKorn Admission Controller")
	return hook
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
	namespace := req.Namespace
	if namespace == "" {
		namespace = "default"
	}
	log.Logger().Info("AdmissionReview",
		zap.String("Namespace", namespace),
		zap.String("UID", string(req.UID)),
		zap.String("Operation", string(req.Operation)),
		zap.String("Kind", req.Kind.Kind),
		zap.Any("UserInfo", req.UserInfo))

	if req.Operation == admissionv1.Update {
		if req.Kind.Kind == pod {
			return c.processPodUpdate(req, namespace)
		}

		// resource types other than pods are ignored for UPDATE operations
		return admissionResponseBuilder(string(req.UID), true, "", nil)
	}

	if req.Kind.Kind == pod {
		return c.processPod(req, namespace)
	}

	return c.processWorkload(req, namespace)
}

func (c *admissionController) processPod(req *admissionv1.AdmissionRequest, namespace string) *admissionv1.AdmissionResponse {
	var patch []common.PatchOperation
	var uid = string(req.UID)

	var pod v1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		log.Logger().Error("unmarshal failed", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	userName := req.UserInfo.Username
	groups := req.UserInfo.Groups
	failureResponse, userInfoSet := c.checkUserInfoAnnotation(func() (string, bool) {
		a, ok := pod.Annotations[common.UserInfoAnnotation]
		return a, ok
	}, userName, groups, uid)
	if failureResponse != nil {
		return failureResponse
	}

	if !userInfoSet && !c.conf.GetBypassAuth() {
		log.Logger().Info("setting user info annotation on pod")
		patchOp, err := c.annotationHandler.GetPatchForPod(pod.Annotations, userName, groups)
		if err != nil {
			return admissionResponseBuilder(uid, false, err.Error(), nil)
		}
		patch = append(patch, *patchOp)
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

func (c *admissionController) processWorkload(req *admissionv1.AdmissionRequest, namespace string) *admissionv1.AdmissionResponse {
	var uid = string(req.UID)

	annotations, supported, err := c.annotationHandler.GetAnnotationsFromRequestKind(req)
	if !supported {
		// Unknown request kind - pass
		return admissionResponseBuilder(uid, true, "", nil)
	}
	if err != nil {
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	userName := req.UserInfo.Username
	groups := req.UserInfo.Groups
	failureResponse, userInfoSet := c.checkUserInfoAnnotation(func() (string, bool) {
		a, ok := annotations[common.UserInfoAnnotation]
		return a, ok
	}, userName, groups, uid)
	if failureResponse != nil {
		return failureResponse
	}

	if !userInfoSet && !c.conf.GetBypassAuth() {
		patch, err := c.annotationHandler.GetPatchForWorkload(req, userName, groups)
		if err != nil {
			log.Logger().Error("could not generate patch for workload", zap.Error(err))
			return admissionResponseBuilder(uid, false, err.Error(), nil)
		}

		patchBytes, patchErr := json.Marshal(patch)
		if patchErr != nil {
			log.Logger().Error("failed to marshal patch", zap.Error(patchErr))
			return admissionResponseBuilder(uid, false, patchErr.Error(), nil)
		}
		log.Logger().Info("updating annotations on workload", zap.String("type", req.Kind.Kind),
			zap.Any("generated patch", patch))
		return admissionResponseBuilder(uid, true, "", patchBytes)
	}

	return admissionResponseBuilder(uid, true, "", nil)
}

func (c *admissionController) processPodUpdate(req *admissionv1.AdmissionRequest, namespace string) *admissionv1.AdmissionResponse {
	uid := string(req.UID)

	var newPod v1.Pod
	if err := json.Unmarshal(req.Object.Raw, &newPod); err != nil {
		log.Logger().Error("unmarshal failed", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	if labelAppValue, ok := newPod.Labels[constants.LabelApp]; ok {
		if labelAppValue == yunikornPod {
			log.Logger().Info("pod update - ignore yunikorn pod")
			return admissionResponseBuilder(uid, true, "", nil)
		}
	}

	if !c.shouldProcessNamespace(namespace) {
		log.Logger().Info("pod update - bypassing namespace", zap.String("namespace", namespace))
		return admissionResponseBuilder(uid, true, "", nil)
	}

	pod, err := c.clientSet.CoreV1().Pods(newPod.Namespace).Get(ctx.Background(), newPod.Name, metav1.GetOptions{})
	if err != nil {
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	originalUserInfo := pod.Annotations[common.UserInfoAnnotation]
	newUserInfo := newPod.Annotations[common.UserInfoAnnotation]

	log.Logger().Debug("checking original and new pod annotation", zap.String("original", originalUserInfo),
		zap.String("new", newUserInfo))

	if originalUserInfo != newUserInfo {
		return admissionResponseBuilder(uid, false, "user info annotation change is not allowed", nil)
	}

	return admissionResponseBuilder(uid, true, "", nil)
}

func (c *admissionController) checkUserInfoAnnotation(getAnnotation func() (string, bool), userName string, groups []string, uid string) (*admissionv1.AdmissionResponse, bool) {
	annotation, userInfoSet := getAnnotation()
	if userInfoSet && !c.conf.GetBypassAuth() {
		if allowed := c.annotationHandler.IsAnnotationAllowed(userName, groups); !allowed {
			errMsg := fmt.Sprintf("user %s with groups [%s] is not allowed to set user annotation", userName,
				strings.Join(groups, ","))
			log.Logger().Error("user info validation failed - submitter is not allowed to set user annotation",
				zap.String("user", userName),
				zap.Strings("groups", groups))
			return admissionResponseBuilder(uid, false, errMsg, nil), userInfoSet
		}

		if err := c.annotationHandler.IsAnnotationValid(annotation); err != nil {
			log.Logger().Error("invalid user info annotation", zap.Error(err))
			return admissionResponseBuilder(uid, false, err.Error(), nil), userInfoSet
		}
	}

	return nil, userInfoSet
}

func updateSchedulerName(patch []common.PatchOperation) []common.PatchOperation {
	log.Logger().Info("updating scheduler name")
	return append(patch, common.PatchOperation{
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

func updateLabels(namespace string, pod *v1.Pod, patch []common.PatchOperation) []common.PatchOperation {
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

	patch = append(patch, common.PatchOperation{
		Op:    "add",
		Path:  "/metadata/labels",
		Value: result,
	})

	return patch
}

func (c *admissionController) validateConf(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req == nil {
		log.Logger().Warn("empty request received")
		return admissionResponseBuilder("", false, "", nil)
	}

	uid := string(req.UID)

	var requestKind = req.Kind.Kind
	if requestKind != "ConfigMap" {
		log.Logger().Warn("request kind is not configmap", zap.String("requestKind", requestKind))
		return admissionResponseBuilder(uid, true, "", nil)
	}

	namespace := req.Namespace
	if namespace == "" {
		namespace = "default"
	}

	var configmap v1.ConfigMap
	if err := json.Unmarshal(req.Object.Raw, &configmap); err != nil {
		log.Logger().Error("failed to unmarshal configmap", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	// validate new/updated config map
	if err := c.validateConfigMap(namespace, &configmap); err != nil {
		log.Logger().Error("failed to validate yunikorn configs", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	return admissionResponseBuilder(uid, true, "", nil)
}

func (c *admissionController) namespaceMatchesProcessList(namespace string) bool {
	processNamespaces := c.conf.GetProcessNamespaces()
	if len(processNamespaces) == 0 {
		return true
	}
	for _, re := range processNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) namespaceMatchesBypassList(namespace string) bool {
	bypassNamespaces := c.conf.GetBypassNamespaces()
	for _, re := range bypassNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) namespaceMatchesLabelList(namespace string) bool {
	labelNamespaces := c.conf.GetLabelNamespaces()
	if len(labelNamespaces) == 0 {
		return true
	}
	for _, re := range labelNamespaces {
		if re.MatchString(namespace) {
			return true
		}
	}
	return false
}

func (c *admissionController) namespaceMatchesNoLabelList(namespace string) bool {
	noLabelNamespaces := c.conf.GetNoLabelNamespaces()
	for _, re := range noLabelNamespaces {
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

func (c *admissionController) validateConfigMap(namespace string, cm *v1.ConfigMap) error {
	if namespace != c.conf.GetNamespace() {
		log.Logger().Debug("Configmap does not belong to YuniKorn", zap.String("namespace", namespace), zap.String("Name", cm.Name))
		return nil
	}

	configMaps := c.conf.GetConfigMaps()
	switch cm.Name {
	case constants.DefaultConfigMapName:
		configMaps[0] = cm
	case constants.ConfigMapName:
		configMaps[1] = cm
	default:
		log.Logger().Debug("Configmap does not belong to YuniKorn", zap.String("namespace", namespace), zap.String("Name", cm.Name))
		return nil
	}

	configs := schedulerconf.FlattenConfigMaps(configMaps)
	policyGroup := conf.GetPendingPolicyGroup(configs)
	confKey := fmt.Sprintf("%s.yaml", policyGroup)

	content, ok := configs[confKey]
	if !ok {
		log.Logger().Info("Configmap missing policygroup config, using default", zap.String("entry", confKey))
		content = ""
	}

	checksum := fmt.Sprintf("%X", sha256.Sum256([]byte(content)))
	log.Logger().Info("Validating YuniKorn configuration", zap.String("checksum", checksum))
	log.Logger().Debug("Configmap data", zap.ByteString("content", []byte(content)))
	response, err := http.Post(fmt.Sprintf(schedulerValidateConfURLPattern, c.conf.GetSchedulerServiceAddress()), "application/json", bytes.NewBuffer([]byte(content)))
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
	if err = json.Unmarshal(responseBytes, &responseData); err != nil {
		log.Logger().Error("Unable to parse response from YuniKorn scheduler, assuming configmap is valid", zap.Error(err))
		return nil
	}
	if !responseData.Allowed {
		err = fmt.Errorf(responseData.Reason)
		log.Logger().Error("Configmap validation failed, aborting", zap.Error(err))
		return err
	}

	log.Logger().Info("Successfully validated YuniKorn configuration")
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
