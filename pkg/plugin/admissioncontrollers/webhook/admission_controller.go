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

	"github.com/apache/incubator-yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/incubator-yunikorn-k8shim/pkg/log"
)

const (
	autoGenAppPrefix             = "yunikorn"
	autoGenAppSuffix             = "autogen"
	enableConfigHotRefreshEnvVar = "ENABLE_CONFIG_HOT_REFRESH"
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

func (c *admissionController) mutate(ar *v1beta1.AdmissionReview) *v1beta1.AdmissionResponse {
	req := ar.Request
	namespace := ar.Request.Namespace
	log.Logger().Info("AdmissionReview",
		zap.Any("Kind", req.Kind),
		zap.String("Namespace", namespace),
		zap.String("UID", string(req.UID)),
		zap.String("Operation", string(req.Operation)),
		zap.Any("UserInfo", req.UserInfo))

	var patch []patchOperation

	if req.Kind.Kind == "Pod" {
		var pod v1.Pod
		if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
			return &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: err.Error(),
				},
			}
		}

		if labelAppValue, ok := pod.Labels[constants.LabelApp]; ok {
			if labelAppValue == "yunikorn" {
				log.Logger().Info("ignore yunikorn pod")
				return &v1beta1.AdmissionResponse{
					Allowed: true,
				}
			}
		}

		patch = updateSchedulerName(patch)
		patch = updateLabels(namespace, &pod, patch)
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return &v1beta1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: err.Error(),
			},
		}
	}

	return &v1beta1.AdmissionResponse{
		Allowed: true,
		Patch:   patchBytes,
		PatchType: func() *v1beta1.PatchType {
			pt := v1beta1.PatchTypeJSONPatch
			return &pt
		}(),
	}
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
			log.Logger().Debug("adding application ID",
				zap.String("generatedID", generatedID))
			result[constants.LabelApplicationID] = generatedID
		}
	}

	if _, ok := existingLabels[constants.LabelQueueName]; !ok {
		log.Logger().Debug("adding queue name",
			zap.String("defaultQueue", "root.default"))
		result[constants.LabelQueueName] = "root.default"
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

func (c *admissionController) validateConf(ar *v1beta1.AdmissionReview) *v1beta1.AdmissionResponse {
	req := ar.Request
	if !isConfigMapUpdateAllowed(req.UserInfo.Username) {
		return &v1beta1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: fmt.Sprintf("ConfigHotRefresh is disabled. " +
					"Please use the REST API to update the configuration, or enable configHotRefresh"),
			},
		}
	}
	log.Logger().Info("AdmissionReview",
		zap.Any("Kind", req.Kind),
		zap.String("Namespace", req.Namespace),
		zap.String("UID", string(req.UID)),
		zap.String("Operation", string(req.Operation)),
		zap.Any("UserInfo", req.UserInfo))

	if req.Kind.Kind == "ConfigMap" {
		var configmap v1.ConfigMap
		if err := json.Unmarshal(req.Object.Raw, &configmap); err != nil {
			return &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: err.Error(),
				},
			}
		}
		// validate new/updated config map
		if err := c.validateConfigMap(&configmap); err != nil {
			log.Logger().Error("failed to validate yunikorn configs", zap.Error(err))
			return &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: err.Error(),
				},
			}
		}
	}

	return &v1beta1.AdmissionResponse{
		Allowed: true,
	}
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
	}
	return nil
}

func (c *admissionController) serve(w http.ResponseWriter, r *http.Request) {
	log.Logger().Debug("request", zap.Any("httpRequest", r))
	var body []byte
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	// verify the content type is accurate
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		http.Error(w, "invalid Content-Type, expect `application/json`", http.StatusUnsupportedMediaType)
		return
	}

	var admissionResponse *v1beta1.AdmissionResponse
	ar := v1beta1.AdmissionReview{}
	if _, _, err := deserializer.Decode(body, nil, &ar); err != nil {
		log.Logger().Error("Can't decode the body", zap.Error(err))
		admissionResponse = &v1beta1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Message: err.Error(),
			},
		}
	} else if r.URL.Path == mutateURL {
		admissionResponse = c.mutate(&ar)
	} else if r.URL.Path == validateConfURL {
		admissionResponse = c.validateConf(&ar)
	}

	admissionReview := v1beta1.AdmissionReview{}
	if admissionResponse != nil {
		admissionReview.Response = admissionResponse
		if ar.Request != nil {
			admissionReview.Response.UID = ar.Request.UID
		}
	}

	resp, err := json.Marshal(admissionReview)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
	}

	log.Logger().Info("writing response...")
	if _, err = w.Write(resp); err != nil {
		http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
	}
}
