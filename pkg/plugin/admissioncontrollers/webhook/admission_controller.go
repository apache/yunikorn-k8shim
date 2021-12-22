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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"k8s.io/api/admission/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

const (
	autoGenAppPrefix             = "yunikorn"
	autoGenAppSuffix             = "autogen"
	enableConfigHotRefreshEnvVar = "ENABLE_CONFIG_HOT_REFRESH"
	yunikornPod                  = "yunikorn"
	defaultQueue                 = "root.default"
	configHotFreshResponse       = "ConfigHotRefresh is disabled. Please use the REST API to update the configuration, or enable configHotRefresh. "
)

var (
	runtimeScheme = runtime.NewScheme()
	codecs        = serializer.NewCodecFactory(runtimeScheme)
	deserializer  = codecs.UniversalDeserializer()
)

type admissionController struct {
	configName               string
	schedulerValidateConfURL string
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

func admissionResponseBuilder(uid string, allowed bool, resultMessage string, patch []byte) *v1beta1.AdmissionResponse {
	res := &v1beta1.AdmissionResponse{}
	res.Allowed = allowed
	res.UID = types.UID(uid)

	if len(resultMessage) != 0 {
		res.Result = &metav1.Status{
			Message: resultMessage,
		}
	}
	if len(patch) != 0 {
		res.Patch = patch
		pt := v1beta1.PatchTypeJSONPatch
		res.PatchType = &pt
	}
	return res
}
func (c *admissionController) mutate(req *v1beta1.AdmissionRequest) *v1beta1.AdmissionResponse {
	namespace := req.Namespace
	var patch []patchOperation

	var requestKind = req.Kind.Kind
	var uid = string(req.UID)

	if requestKind != "Pod" {
		log.Logger().Warn("request kind is not pod", zap.String("uid", uid),
			zap.String("requestKind", requestKind))

		return admissionResponseBuilder(uid, true, "", nil)
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

	if labelAppValue, ok := pod.Labels[constants.LabelApp]; ok {
		if labelAppValue == yunikornPod {
			log.Logger().Info("ignore yunikorn pod")
			admissionResponseBuilder(uid, true, "", nil)
		}
	}

	patch = updateSchedulerName(patch)
	patch = updateLabels(namespace, &pod, patch)
	log.Logger().Info("generated patch", zap.String("podName", pod.Name),
		zap.Any("patch", patch))

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Logger().Error("failed to marshal patch", zap.Error(err))
		return admissionResponseBuilder(uid, false, err.Error(), nil)
	}

	return admissionResponseBuilder(uid, true, "", patchBytes)
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
	ns := "default"
	if namespace != "" {
		ns = namespace
	}
	generatedID := fmt.Sprintf("%s-%s-%s", autoGenAppPrefix, ns, autoGenAppSuffix)
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

func (c *admissionController) validateConf(req *v1beta1.AdmissionRequest) *v1beta1.AdmissionResponse {
	uid := string(req.UID)
	if !isConfigMapUpdateAllowed(req.UserInfo.Username) {
		return admissionResponseBuilder(uid, false, configHotFreshResponse, nil)
	}

	var requestKind = req.Kind.Kind
	if requestKind != "ConfigMap" {
		log.Logger().Warn("request kind is not configmap", zap.String("requestKind", requestKind))
		admissionResponseBuilder(uid, true, "", nil)
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

func (c *admissionController) validateConfigMap(cm *v1.ConfigMap) error {
	if cm.Name == constants.DefaultConfigMapName {
		log.Logger().Info("validating yunikorn configs")
		if content, ok := cm.Data[c.configName]; ok {
			response, err := http.Post(c.schedulerValidateConfURL, "application/json", bytes.NewBuffer([]byte(content)))
			if err != nil {
				return err
			}
			defer response.Body.Close()
			responseBytes, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return err
			}
			var responseData ValidateConfResponse
			if err := json.Unmarshal(responseBytes, &responseData); err != nil {
				return err
			}
			if !responseData.Allowed {
				return fmt.Errorf(responseData.Reason)
			}
		} else {
			return fmt.Errorf("required config '%s' not found in this configmap", c.configName)
		}
	} else {
		log.Logger().Debug("Configmap does not belong to Yunikorn", zap.String("Name", cm.Name))
	}
	return nil
}

func (c *admissionController) serve(w http.ResponseWriter, r *http.Request) {
	log.Logger().Debug("request", zap.Any("httpRequest", r))
	var body []byte
	if r.Body != nil {
		var err error
		body, err = ioutil.ReadAll(r.Body)
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
	ar := v1beta1.AdmissionReview{}
	req := ar.Request
	var admissionResponse *v1beta1.AdmissionResponse
	_, _, err := deserializer.Decode(body, nil, &ar)
	if err != nil || ar.Request == nil {
		log.Logger().Error("request body decode failed or request empty", zap.Error(err))
		admissionResponse = admissionResponseBuilder("yunikorn-invalid-body", false, "body decode failed", nil)
	} else {
		switch urlPath {
		case mutateURL:
			admissionResponse = c.mutate(req)
		case validateConfURL:
			admissionResponse = c.validateConf(req)
		}
	}
	admissionReview := v1beta1.AdmissionReview{Response: admissionResponse}
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
