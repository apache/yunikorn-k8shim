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

package cache

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

func FindAppTaskGroup(appTaskGroups []*TaskGroup, groupName string) (*TaskGroup, error) {
	if groupName == "" {
		// task has no group defined
		return nil, nil
	}

	// app has no taskGroups associated
	if len(appTaskGroups) == 0 {
		return nil, nil
	}

	// task group defined in app, return the corresponding taskGroup
	for _, tg := range appTaskGroups {
		if tg.Name == groupName {
			return tg, nil
		}
	}

	// task group name specified, but could not find a mapping value in app taskGroups
	return nil, fmt.Errorf("taskGroup %s is not defined in the application", groupName)
}

// GeneratePlaceholderName creates the placeholder name for a pod, pod name can not be longer than 63 chars,
// appID and taskGroupName will be truncated if they go over 28/20 chars respectively,
// and each name will be suffixed with a randomly generated 10-character nonce.
func GeneratePlaceholderName(taskGroupName, appID string) string {
	// appID no longer than 28 chars
	// taskGroup name no longer than 20 chars
	// nonce equal to 10 chars
	// total length no longer than 28 + 20 + 5 + 10 = 63
	return fmt.Sprintf("tg-%.28s-%.20s-%s", appID, taskGroupName, generateNonce(10))
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz0123456789"

// generateNonce generates an alphanumeric string (i.e. base-36) of the given length
func generateNonce(length int) string {
	buf := make([]byte, length)
	for i := range buf {
		// this uses the built-in rand package as it does not need to be hardened
		buf[i] = letterBytes[rand.Intn(len(letterBytes))] //nolint:gosec
	}
	return string(buf)
}

// GetPlaceholderResourceRequests converts the map of resources requested into a list of resources for the request
// specification that can be added to a pod. No checks on what is requested.
func GetPlaceholderResourceRequests(resources map[string]resource.Quantity) v1.ResourceList {
	resourceReq := v1.ResourceList{}
	for k, v := range resources {
		if k == "" {
			continue
		}
		resourceReq[v1.ResourceName(k)] = v
	}
	return resourceReq
}

func GetSchedulingPolicyParam(pod *v1.Pod) *SchedulingPolicyParameters {
	timeout := int64(0)
	style := constants.SchedulingPolicyStyleParamDefault
	schedulingPolicyParams := NewSchedulingPolicyParameters(timeout, style)
	param := utils.GetPodAnnotationValue(pod, constants.AnnotationSchedulingPolicyParam)
	if param == "" {
		return schedulingPolicyParams
	}
	params := strings.Split(param, constants.SchedulingPolicyParamDelimiter)
	var err error
	for _, p := range params {
		param := strings.Split(p, "=")
		if len(param) != 2 {
			log.Log(log.ShimUtils).Warn("Skipping malformed scheduling policy parameter: ", zap.String("namespace", pod.Namespace), zap.String("name", pod.Name), zap.String("Scheduling Policy parameters passed in annotation: ", p))
			continue
		}
		if param[0] == constants.SchedulingPolicyTimeoutParam {
			timeout, err = strconv.ParseInt(param[1], 10, 64)
			if err != nil {
				log.Log(log.ShimUtils).Warn("Failed to parse timeout value from annotation", zap.String("namespace", pod.Namespace), zap.String("name", pod.Name), zap.Int64("Using Placeholder timeout: ", timeout), zap.String("Placeholder timeout passed in annotation: ", p))
			}
		} else if param[0] == constants.SchedulingPolicyStyleParam {
			style = constants.SchedulingPolicyStyleParamValues[param[1]]
			if style == "" {
				style = constants.SchedulingPolicyStyleParamDefault
				log.Log(log.ShimUtils).Warn("Unknown gang scheduling style, using "+constants.SchedulingPolicyStyleParamDefault+" style as default",
					zap.String("namespace", pod.Namespace), zap.String("name", pod.Name), zap.String("Gang scheduling style passed in annotation: ", p))
			}
		}
	}
	schedulingPolicyParams = NewSchedulingPolicyParameters(timeout, style)
	return schedulingPolicyParams
}
