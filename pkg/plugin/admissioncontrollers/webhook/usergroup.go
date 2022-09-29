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
	"strings"

	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"

	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func (c *admissionController) validateExternalUserInfo(req *admissionv1.AdmissionRequest, userInfo string) error {
	userName := req.UserInfo.Username
	groups := req.UserInfo.Groups

	if c.bypassControllers {
		for _, sysUser := range c.systemUsers {
			if sysUser.MatchString(userName) {
				log.Logger().Debug("Request submitted from a system user, bypassing",
					zap.String("userName", userName))
				return nil
			}
		}
	}

	for _, allowedUser := range c.externalUsers {
		if allowedUser.MatchString(userName) {
			log.Logger().Debug("Request submitted from an allowed external user",
				zap.String("userName", userName))
			return nil
		}
	}

	for _, allowedGroup := range c.externalGroups {
		for _, group := range groups {
			if allowedGroup.MatchString(group) {
				log.Logger().Debug("Request submitted from an allowed external group",
					zap.String("userName", userName),
					zap.String("group", group))
				return nil
			}
		}
	}

	return fmt.Errorf("user %s with groups [%s] is not allowed to set user annotation", userName,
		strings.Join(groups, ","))
}

func (c *admissionController) validateAnnotation(userInfoAnnotation string) error {
	var userGroups si.UserGroupInformation
	err := json.Unmarshal([]byte(userInfoAnnotation), &userGroups)
	if err != nil {
		return err
	}

	log.Logger().Debug("Successfully validated user info annotation", zap.String("externally provided user", userGroups.User),
		zap.String("externally provided groups", strings.Join(userGroups.Groups, ",")))

	return nil
}
